import asyncio, json, os, uuid, time, sys
from fastapi import FastAPI, HTTPException, Request, Query
from starlette.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from mcp import StdioServerParameters, stdio_client, ClientSession
from common.settings import settings as S
from common.structured_logging import get_logger, SERVICE_NAME
from common.settings import settings
from langchain.prompts import ChatPromptTemplate
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from . import db_models
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from common.kafka_utils import publish_event
from contextlib import asynccontextmanager
from datetime import timedelta, datetime
from langchain_core.messages import AIMessage
from langchain.callbacks.base import AsyncCallbackHandler
import inspect  # for token usage
from pathlib import Path
# import service layer
from .service import (
    BookRecommendation,
    RecommendResponse,
    generate_recommendations,
    generate_reader_recommendations,
    generate_agent_recommendations,
)
from common.metrics import REQUEST_COUNTER, REQUEST_LATENCY
from pydantic import BaseModel, Field, validator
from typing import List, Dict, Any, Optional
from common.events import FeedbackEvent, FEEDBACK_EVENTS_TOPIC

async def _validate_configuration():
    """Validate critical configuration on startup."""
    logger.info("Validating configuration")
    errors = []
    
    # Check required environment variables
    if not S.openai_api_key:
        errors.append("OPENAI_API_KEY not configured")
    
    if not S.db_url:
        errors.append("Database URL not configured")
    
    # Validate database connection
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("Database connection validated")
    except Exception as e:
        errors.append(f"Database connection failed: {e}")
    
    # Validate Redis connection (non-critical)
    try:
        from common.redis_utils import get_redis_client
        redis_client = get_redis_client()
        if redis_client:
            await redis_client.ping()
            logger.info("Redis connection validated")
        else:
            logger.warning("Redis not available, using fallback")
    except Exception as e:
        logger.warning(f"Redis connection failed: {e}")
    
    # Validate vector store directory
    try:
        S.vector_store_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Vector store directory validated")
    except Exception as e:
        errors.append(f"Vector store directory creation failed: {e}")
    
    # Validate model configuration
    if S.model_max_tokens <= 0:
        errors.append("MODEL_MAX_TOKENS must be positive")
    
    if S.similarity_threshold < 0 or S.similarity_threshold > 1:
        errors.append("SIMILARITY_THRESHOLD must be between 0 and 1")
    
    if errors:
        logger.error("Configuration validation failed", extra={"errors": errors})
        raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")
    
    logger.info("Configuration validation completed successfully")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown tasks using FastAPI lifespan events."""
    logger.info("Starting up recommendation API")
    try:
        await _validate_configuration()
        await _ensure_tables()
    except Exception:
        logger.error("Failed to start up recommendation API", exc_info=True)
        raise

    # Application runs during this yield
    yield

    logger.info("Shutting down recommendation API")
    # Producer cleanup handled automatically per event loop
    logger.debug("Kafka producer cleanup handled automatically")

app = FastAPI(
    title="Book Recommendation Engine API",
    description="AI-powered book recommendation system for educational institutions",
    version="1.0.0",
    contact={
        "name": "Dan Guilliams",
        "email": "dan@example.com"
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT"
    },
    lifespan=lifespan
)

# Rate limiting configuration
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

logger = get_logger(__name__)

# --- auto-DDL setup ----------------------------------------------------
logger.info("Initializing database connection")
# Convert postgresql:// to postgresql+asyncpg:// for async operations
db_url_str = str(S.db_url)
if db_url_str.startswith("postgresql://"):
    async_db_url = db_url_str.replace("postgresql://", "postgresql+asyncpg://")
elif db_url_str.startswith("postgresql+asyncpg://"):
    async_db_url = db_url_str
else:
    async_db_url = db_url_str
engine = create_async_engine(async_db_url, echo=False)

async def _ensure_tables():
    logger.info("Ensuring database tables exist")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(db_models.Base.metadata.create_all)
            # Additional idempotent DDL for recommendation_history with composite PK
            await conn.execute(text(
                """
                CREATE TABLE IF NOT EXISTS recommendation_history (
                    student_id TEXT NOT NULL,
                    book_id TEXT NOT NULL,
                    recommended_at TIMESTAMPTZ DEFAULT NOW(),
                    justification TEXT,
                    PRIMARY KEY (student_id, book_id)
                );
                """
            ))
        logger.info("Database tables verified/created successfully")
    except Exception as e:
        logger.error("Failed to create database tables", exc_info=True)
        raise

# --- metrics -----------------------------------------------------------
logger.info("Initializing Kafka producer for metrics")
TOPIC = "api_metrics"

async def push_metric(event: str, extra: dict | None = None):
    """Send metric to Kafka and Redis without blocking the caller."""
    # Compose payload once
    payload = {"event": event, "timestamp": time.time(), "request_id": uuid.uuid4().hex}
    if extra:
        payload.update(extra)

    async def _send():
        try:
            # Send to Kafka
            await publish_event(TOPIC, payload)
            logger.debug("Metric pushed to Kafka", extra={"event": event, "payload": payload})
            
            # Also store in Redis for Streamlit access
            try:
                from common.redis_utils import get_redis_client
                redis_client = get_redis_client()
                redis_key = "metrics:api:recent"
                
                import json
                await redis_client.lpush(redis_key, json.dumps(payload))
                await redis_client.ltrim(redis_key, 0, 19)  # Keep only last 20
                
                logger.debug("Metric stored in Redis", extra={"event": event, "key": redis_key})
            except Exception as redis_error:
                logger.warning(f"Failed to store metric in Redis: {redis_error}")
                
        except Exception:
            logger.error("Failed to push metric", exc_info=True, extra={"event": event})

    # Fire-and-forget so the caller returns immediately
    asyncio.create_task(_send())

# --- FastMCP lifecycle -------------------------------------------------
logger.info("Setting up FastMCP server parameters")

# On Windows we *must* use ProactorEventLoopPolicy because Fast-MCP launches a
# subprocess and `asyncio.create_subprocess_exec` only works with the Proactor
# loop.  This line is mostly relevant for code paths that import this module
# directly (e.g. Streamlit inside Docker).  When you start uvicorn from the
# command line you still have to ensure the process is started with the
# Proactor policy (see run_api_windows.py helper script).
if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# build absolute path to the MCP server script so subprocess can locate it when cwd changes
_mcp_path = Path(__file__).parent / "mcp_book_server.py"

server_params = StdioServerParameters(
    command="python",
    args=[str(_mcp_path)],
    env=dict(os.environ),
)

# --- LLM model for ReAct agent ---------------------------------------
chat_model = ChatOpenAI(
    model=S.model_name,
    api_key=S.openai_api_key,
    temperature=0.3,
    max_tokens=S.model_max_tokens,
)
logger.info("ChatOpenAI model initialised for ReAct agent")

async def ask_agent(agent, query: str, callbacks=None):
    logger.info("Executing agent query:")
    logger.debug(f"Query: {query}")
    # LangChain runnables expect callbacks via the `config` dict
    cfg = {"callbacks": callbacks} if callbacks else None
    response = await agent.ainvoke({"messages": query}, config=cfg)

    # Get the final AI message (the actual analysis)
    final_message = next(
        msg for msg in reversed(response["messages"]) if isinstance(msg, AIMessage) and msg.content
    )
    logger.debug(f"Final message: {final_message}")
    # Return both the final message and the response json with metadata
    return final_message, response

# --- endpoints ---------------------------------------------------------
@app.get("/health")
async def health():
    """Comprehensive health check that tests all critical system components."""
    logger.debug("Health check requested")
    health_status = {
        "status": "ok",
        "timestamp": time.time(),
        "components": {}
    }
    
    # Test database connection
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        health_status["components"]["database"] = {"status": "healthy", "response_time_ms": 0}
    except Exception as e:
        health_status["components"]["database"] = {"status": "unhealthy", "error": str(e)}
        health_status["status"] = "degraded"
    
    # Test Redis connection
    try:
        from common.redis_utils import get_redis_client
        redis_client = get_redis_client()
        if redis_client:
            await redis_client.ping()
            health_status["components"]["redis"] = {"status": "healthy"}
        else:
            health_status["components"]["redis"] = {"status": "unavailable", "note": "Using fallback"}
    except Exception as e:
        health_status["components"]["redis"] = {"status": "unhealthy", "error": str(e)}
        health_status["status"] = "degraded"
    
    # Test OpenAI API availability (lightweight check)
    try:
        # Just check if we have API key configured
        if S.openai_api_key:
            health_status["components"]["openai"] = {"status": "configured"}
        else:
            health_status["components"]["openai"] = {"status": "not_configured"}
            health_status["status"] = "degraded"
    except Exception as e:
        health_status["components"]["openai"] = {"status": "error", "error": str(e)}
        health_status["status"] = "degraded"
    
    # Test vector store availability
    try:
        vector_store_path = S.vector_store_dir
        if vector_store_path.exists():
            health_status["components"]["vector_store"] = {"status": "available"}
        else:
            health_status["components"]["vector_store"] = {"status": "not_initialized"}
    except Exception as e:
        health_status["components"]["vector_store"] = {"status": "error", "error": str(e)}
        health_status["status"] = "degraded"
    
    # Return appropriate HTTP status code
    status_code = 200 if health_status["status"] == "ok" else 503
    return JSONResponse(content=health_status, status_code=status_code)

@app.get("/metrics")
async def metrics():
    logger.debug("Metrics endpoint requested")
    return JSONResponse({"status": "ok"})

# --- Pydantic models for Swagger --------------------------------------------
class BookRow(BaseModel, extra="allow"):
    """Flexible model that accepts any catalog columns."""
    book_id: str = Field(..., examples=["B001"])

class BooksResponse(BaseModel):
    rows: List[BookRow]

class MetricItem(BaseModel):
    label: str = Field(..., examples=["recommendation_served"])
    value: int = Field(..., examples=[42])

# --- Reader Mode Models ---
class FeedbackRequest(BaseModel):
    """Request model for reader feedback submission."""
    user_hash_id: str = Field(..., description="Hashed user identifier", min_length=1, max_length=100)
    book_id: str = Field(..., description="Book ID that received feedback", min_length=1, max_length=50)
    score: int = Field(..., description="Feedback score: 1 for thumbs up, -1 for thumbs down")
    feedback_text: Optional[str] = Field(None, description="Optional feedback text", max_length=1000)
    
    @validator('score')
    def validate_score(cls, v):
        if v not in [-1, 1]:
            raise ValueError('Score must be either 1 (thumbs up) or -1 (thumbs down)')
        return v
    
    @validator('user_hash_id')
    def validate_user_hash_id(cls, v):
        if not v.strip():
            raise ValueError('User hash ID cannot be empty')
        return v.strip()
    
    @validator('book_id')
    def validate_book_id(cls, v):
        if not v.strip():
            raise ValueError('Book ID cannot be empty')
        return v.strip()

class FeedbackResponse(BaseModel):
    """Response model for feedback submission."""
    message: str
    request_id: str

class ReaderRecommendResponse(BaseModel):
    """Response model for reader recommendations."""
    request_id: str
    user_hash_id: str
    recommendations: List[BookRecommendation]
    duration_sec: float
    based_on_books: List[str]  # Titles of books used for similarity

class UserBooksResponse(BaseModel):
    """Response model for user's uploaded books."""
    user_hash_id: str
    books: List[Dict[str, Any]]
    total_count: int

# ---------------------------------------------------------------------------
# New endpoints for React UI -------------------------------------------------
# ---------------------------------------------------------------------------

@app.get(
    "/books",
    tags=["catalog"],
    summary="Return a slice of the book catalog",
    response_model=BooksResponse,
    responses={
        200: {"description": "Catalog slice returned"},
        400: {"description": "Invalid limit parameter"},
        500: {"description": "Database query failed"},
    },
)
async def get_books(limit: int = Query(100, ge=1, le=500, description="Max rows to return")):
    """Simple endpoint so the React UI can populate its data-explorer table."""
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text("SELECT * FROM catalog ORDER BY book_id LIMIT :limit"),
                {"limit": limit},
            )
            rows = [dict(row) for row in result]
        return {"rows": rows}
    except HTTPException:
        raise  # re-throw untouched
    except Exception as exc:
        logger.error("Failed to fetch books", exc_info=True)
        raise HTTPException(500, "Failed to fetch books") from exc

@app.get(
    "/metrics/summary",
    tags=["metrics"],
    summary="Aggregate recent metric events (last 20) for quick charts",
    response_model=List[MetricItem],
    responses={
        200: {"description": "Counts returned (may be empty)"},
    },
)
async def metrics_summary():
    """Aggregate last ~20 metric events stored in Redis.

    If Redis is unavailable, returns a single placeholder so the UI does not
    break. This keeps the endpoint always 200-OK to avoid noisy errors in the
    SPA while still logging the real issue.
    """
    try:
        from common.redis_utils import get_redis_client

        redis_client = get_redis_client()
        raw_items = await redis_client.lrange("metrics:api:recent", 0, -1)
        counts: Dict[str, int] = {}
        for b in raw_items:
            try:
                payload: Dict[str, Any] = json.loads(b)
                event = payload.get("event", "unknown")
                counts[event] = counts.get(event, 0) + 1
            except Exception:
                continue
        return [{"label": k, "value": v} for k, v in counts.items()]
    except Exception as exc:
        logger.warning("Failed to fetch metrics summary", exc_info=True)
        # Graceful degradation – return placeholder instead of 500 so UI chart renders
        return [{"label": "no_data", "value": 0}]

@app.post(
    "/recommend", 
    response_model=RecommendResponse, 
    response_model_exclude_none=True,
    summary="Generate book recommendations for a student",
    description="""
    Generate personalized book recommendations for a student based on their reading history,
    preferences, and optional search query. Uses AI-powered analysis to match books to
    student reading level and interests.
    
    **Privacy Controls:**
    - Set `SUPER_USER=1` environment variable to include student context in response
    - Default responses exclude sensitive student information
    
    **Rate Limits:**
    - Recommendations are cached and logged to prevent abuse
    - Complex LLM processing may take 5-15 seconds
    """,
    responses={
        200: {
            "description": "Successful recommendation generation",
            "content": {
                "application/json": {
                    "example": {
                        "request_id": "abc123",
                        "duration_sec": 8.45,
                        "recommendations": [
                            {
                                "book_id": "B001",
                                "title": "Charlotte's Web",
                                "author": "E.B. White",
                                "reading_level": 5.2,
                                "librarian_blurb": "A heartwarming tale of friendship between a pig and spider.",
                                "justification": "Matches student's interest in animal stories and appropriate reading level."
                            }
                        ]
                    }
                }
            }
        },
        422: {
            "description": "Invalid request parameters",
            "content": {
                "application/json": {
                    "example": {
                        "detail": [
                            {
                                "loc": ["query", "student_id"],
                                "msg": "field required",
                                "type": "value_error.missing"
                            }
                        ]
                    }
                }
            }
        },
        500: {
            "description": "Internal server error during recommendation generation",
            "content": {
                "application/json": {
                    "example": {
                        "detail": "Failed to generate recommendation"
                    }
                }
            }
        }
    },
    tags=["recommendations"]
)
@limiter.limit("10/minute")
async def recommend(
    request: Request,
    student_id: str = Query(..., description="Student identifier (e.g., 'S001')", examples=["S001"]),
    n: int = Query(3, ge=1, le=10, description="Number of recommendations to return", examples=[3]),
    query: str = Query("", description="Optional search query to filter recommendations", examples=["space adventure"])
):
    request_id = uuid.uuid4().hex

    logger.info("Recommendation request received", extra={
        "request_id": request_id,
        "student_id": student_id,
        "n": n,
        "query": query,
    })

    started = time.perf_counter()

    try:
        # Build student context for unified agent function
        from .service import _get_student_context_cached
        from .candidate_builder import build_candidates
        from .scoring import score_candidates
        
        # Get student context
        avg_level, recent_titles, top_genres, band_hist = await _get_student_context_cached(student_id)
        
        # Build and score candidates
        candidates = await build_candidates(student_id, n)
        scored_candidates = await score_candidates(candidates, student_id, query)
        
        # Build context for unified agent
        context = {
            "student_id": student_id,
            "candidates": scored_candidates,
            "avg_level": avg_level,
            "recent_titles": recent_titles,
            "top_genres": top_genres,
            "band_hist": band_hist,
        }
        
        # Use unified agent function
        recs, meta = await generate_agent_recommendations("student", context, query, n, request_id)

        total_duration = time.perf_counter() - started

        asyncio.create_task(
            push_metric(
                "recommendation_served",
                {
                    "request_id": request_id,
                    "student_id": student_id,
                    "n": n,
                    "duration_sec": total_duration,
                    **meta,
                },
            )
        )

        return RecommendResponse(
            request_id=request_id,
            duration_sec=round(total_duration, 3),
            recommendations=recs,
            student_avg_level=meta.get('student_avg_level'),
            recent_books=meta.get('recent_books'),
        )

    except Exception as exc:
        logger.error("Recommendation request failed", exc_info=True, extra={"request_id": request_id})
        raise HTTPException(500, "Failed to generate recommendation") from exc

# ---------------------------------------------------------------------------
# Reader Mode Endpoints
# ---------------------------------------------------------------------------

@app.post(
    "/feedback",
    response_model=FeedbackResponse,
    summary="Submit reader feedback on book recommendations",
    description="""
    Submit thumbs up/down feedback on book recommendations in Reader Mode.
    Feedback is processed asynchronously and used to improve future recommendations.
    """,
    responses={
        200: {"description": "Feedback submitted successfully"},
        422: {"description": "Invalid feedback data"},
        500: {"description": "Failed to process feedback"}
    },
    tags=["reader-mode"]
)
@limiter.limit("30/minute")
async def submit_feedback(request: Request, feedback: FeedbackRequest):
    # Check if Reader Mode is enabled
    if not settings.enable_reader_mode:
        logger.warning("Reader Mode feedback endpoint called but feature is disabled")
        raise HTTPException(
            status_code=404,
            detail="Reader Mode is currently disabled. Set ENABLE_READER_MODE=true to enable this feature."
        )
    """Submit feedback for a book recommendation."""
    request_id = uuid.uuid4().hex
    
    logger.info("Feedback submission received", extra={
        "request_id": request_id,
        "user_hash_id": feedback.user_hash_id,
        "book_id": feedback.book_id,
        "score": feedback.score
    })
    
    try:
        # Create feedback event
        event = FeedbackEvent(
            user_hash_id=feedback.user_hash_id,
            book_id=feedback.book_id,
            score=feedback.score
        )
        
        # Publish to Kafka for async processing
        await publish_event(FEEDBACK_EVENTS_TOPIC, event.model_dump())
        
        logger.info("Feedback event published", extra={
            "request_id": request_id,
            "user_hash_id": feedback.user_hash_id,
            "book_id": feedback.book_id
        })
        
        return FeedbackResponse(
            message="Feedback submitted successfully",
            request_id=request_id
        )
        
    except Exception as exc:
        logger.error("Failed to submit feedback", exc_info=True, extra={"request_id": request_id})
        raise HTTPException(500, "Failed to process feedback") from exc

@app.get(
    "/recommendations/{user_hash_id}",
    response_model=ReaderRecommendResponse,
    summary="Get personalized recommendations for a reader",
    description="""
    Generate personalized book recommendations for a Reader Mode user based on their
    uploaded book list and feedback history. Uses book similarity and collaborative
    filtering to suggest relevant books.
    """,
    responses={
        200: {"description": "Recommendations generated successfully"},
        404: {"description": "User not found"},
        500: {"description": "Failed to generate recommendations"}
    },
    tags=["reader-mode"]
)
@limiter.limit("20/minute")
async def get_reader_recommendations(
    request: Request,
    user_hash_id: str,
    n: int = Query(5, ge=1, le=20, description="Number of recommendations to return"),
    query: str = Query("", description="Optional search query to filter recommendations")
):
    # Check if Reader Mode is enabled
    if not settings.enable_reader_mode:
        logger.warning("Reader Mode recommendations endpoint called but feature is disabled")
        raise HTTPException(
            status_code=404,
            detail="Reader Mode is currently disabled. Set ENABLE_READER_MODE=true to enable this feature."
        )
    """Get personalized recommendations for a reader."""
    request_id = uuid.uuid4().hex
    
    logger.info("Reader recommendation request received", extra={
        "request_id": request_id,
        "user_hash_id": user_hash_id,
        "n": n,
        "query": query
    })
    
    started = time.perf_counter()
    
    try:
        # Check if user exists
        async with engine.begin() as conn:
            user_exists = await conn.execute(
                text("SELECT 1 FROM public_users WHERE hash_id = :user_hash_id LIMIT 1"),
                {"user_hash_id": user_hash_id}
            )
            if not user_exists.fetchone():
                raise HTTPException(404, f"User {user_hash_id} not found")
        
        # Build reader context for unified agent function
        from .service import _fetch_user_uploaded_books_cached, _fetch_user_feedback_scores_cached
        
        # Get user's uploaded books and feedback
        uploaded_books = await _fetch_user_uploaded_books_cached(user_hash_id)
        feedback_scores = await _fetch_user_feedback_scores_cached(user_hash_id)
        
        if not uploaded_books:
            # Fall back to rule-based recommendations if no uploaded books
            recs, meta = await generate_reader_recommendations(user_hash_id, query, n, request_id)
        else:
            # Build candidates for agent-based recommendations
            from .service import _fetch_similar_books
            async with engine.begin() as conn:
                candidates = await _fetch_similar_books(uploaded_books, user_hash_id, conn)
            
            # Build context for unified agent
            context = {
                "user_hash_id": user_hash_id,
                "uploaded_books": uploaded_books,
                "feedback_scores": feedback_scores,
                "candidates": candidates,
            }
            
            # Use unified agent function
            recs, meta = await generate_agent_recommendations("reader", context, query, n, request_id)
        
        total_duration = time.perf_counter() - started
        
        # Log metrics
        asyncio.create_task(
            push_metric(
                "reader_recommendation_served",
                {
                    "request_id": request_id,
                    "user_hash_id": user_hash_id,
                    "n": n,
                    "duration_sec": total_duration,
                    **meta,
                },
            )
        )
        
        return ReaderRecommendResponse(
            request_id=request_id,
            user_hash_id=user_hash_id,
            recommendations=recs,
            duration_sec=round(total_duration, 3),
            based_on_books=meta.get('based_on_books', [])
        )
        
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Reader recommendation request failed", exc_info=True, extra={"request_id": request_id})
        raise HTTPException(500, "Failed to generate recommendations") from exc

@app.get(
    "/user/{user_hash_id}/books",
    response_model=UserBooksResponse,
    summary="Get user's uploaded books",
    description="""
    Retrieve the list of books uploaded by a specific Reader Mode user.
    Includes book metadata and upload timestamps.
    """,
    responses={
        200: {"description": "User books retrieved successfully"},
        404: {"description": "User not found"},
        500: {"description": "Failed to retrieve user books"}
    },
    tags=["reader-mode"]
)
async def get_user_books(
    user_hash_id: str,
    limit: int = Query(100, ge=1, le=500, description="Maximum number of books to return"),
    offset: int = Query(0, ge=0, description="Number of books to skip")
):
    # Check if Reader Mode is enabled
    if not settings.enable_reader_mode:
        logger.warning("Reader Mode user books endpoint called but feature is disabled")
        raise HTTPException(
            status_code=404,
            detail="Reader Mode is currently disabled. Set ENABLE_READER_MODE=true to enable this feature."
        )
    
    """Get user's uploaded books."""
    logger.info("User books request received", extra={
        "user_hash_id": user_hash_id,
        "limit": limit,
        "offset": offset
    })
    
    try:
        async with engine.begin() as conn:
            # Check if user exists
            user_exists = await conn.execute(
                text("SELECT 1 FROM public_users WHERE hash_id = :user_hash_id LIMIT 1"),
                {"user_hash_id": user_hash_id}
            )
            if not user_exists.fetchone():
                raise HTTPException(404, f"User {user_hash_id} not found")
            
            # Get user's books
            books_result = await conn.execute(
                text("""
                    SELECT ub.id, ub.title, ub.author, ub.rating, ub.notes, ub.raw_payload, ub.created_at
                    FROM uploaded_books ub
                    JOIN public_users pu ON ub.user_id = pu.id
                    WHERE pu.hash_id = :user_hash_id 
                    ORDER BY ub.created_at DESC 
                    LIMIT :limit OFFSET :offset
                """),
                {"user_hash_id": user_hash_id, "limit": limit, "offset": offset}
            )
            
            books = []
            for row in books_result.fetchall():
                raw_payload = row.raw_payload or {}
                books.append({
                    "id": str(row.id),
                    "title": row.title,
                    "author": row.author,
                    "rating": row.rating,
                    "notes": row.notes,
                    "genre": raw_payload.get("genre"),
                    "isbn": raw_payload.get("isbn"),
                    "reading_level": raw_payload.get("reading_level"),
                    "uploaded_at": row.created_at.isoformat() if row.created_at else None
                })
            
            # Get total count
            count_result = await conn.execute(
                text("""
                    SELECT COUNT(*) 
                    FROM uploaded_books ub
                    JOIN public_users pu ON ub.user_id = pu.id
                    WHERE pu.hash_id = :user_hash_id
                """),
                {"user_hash_id": user_hash_id}
            )
            total_count = count_result.fetchone()[0]
            
            return UserBooksResponse(
                user_hash_id=user_hash_id,
                books=books,
                total_count=total_count
            )
            
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to retrieve user books", exc_info=True, extra={"user_hash_id": user_hash_id})
        raise HTTPException(500, "Failed to retrieve user books") from exc

class MetricCallbackHandler(AsyncCallbackHandler):
    """Collect per-run observability data."""

    def __init__(self):
        self.tools_used: list[str] = []
        self.error_count: int = 0

    async def on_tool_start(self, serialized, input_str, **kwargs):  # type: ignore[override]
        name = (
            serialized.get("name", "unknown") if isinstance(serialized, dict) else "unknown"
        )
        self.tools_used.append(name)

    async def on_tool_error(self, error, **kwargs):  # type: ignore[override]
        self.error_count += 1 

# ---------------------------------------------------------------------------
# Prometheus middleware (lightweight – no external deps beyond prometheus_client)
# ---------------------------------------------------------------------------

@app.middleware("http")
async def _prometheus_middleware(request: Request, call_next):  # noqa: D401
    """Track per-request latency and count using Prometheus helpers."""

    start_time = time.perf_counter()
    response = await call_next(request)

    duration = time.perf_counter() - start_time
    endpoint = request.url.path

    # Record metrics (labels are no-ops if Prometheus disabled)
    REQUEST_LATENCY.labels(service=SERVICE_NAME, endpoint=endpoint).observe(duration)
    REQUEST_COUNTER.labels(
        service=SERVICE_NAME,
        method=request.method,
        endpoint=endpoint,
        status_code=response.status_code,
    ).inc()

    return response

# ---------------------------------------------------------------------------
# Script entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    # Ensure correct loop policy when executed directly (e.g. python -m ...)
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    uvicorn.run(
        "src.recommendation_api.main:app",
        host="127.0.0.1",
        port=S.recommendation_api_port,
        reload=False,
    ) 
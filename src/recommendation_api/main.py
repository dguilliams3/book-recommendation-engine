import asyncio, json, os, uuid, time, sys
from fastapi import FastAPI, HTTPException, Request, Query
from starlette.responses import JSONResponse
from mcp import StdioServerParameters, stdio_client, ClientSession
from common import SettingsInstance as S
from common.structured_logging import get_logger, SERVICE_NAME
from langchain.prompts import ChatPromptTemplate
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from . import db_models
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from common.kafka_utils import publish_event
from contextlib import asynccontextmanager
from datetime import timedelta
from langchain_core.messages import AIMessage
from langchain.callbacks.base import AsyncCallbackHandler
import inspect  # for token usage
from pathlib import Path
# import service layer
from .service import (
    BookRecommendation,
    RecommendResponse,
    generate_recommendations,
)
from common.metrics import REQUEST_COUNTER, REQUEST_LATENCY
from pydantic import BaseModel, Field
from typing import List, Dict, Any

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage startup and shutdown tasks using FastAPI lifespan events."""
    logger.info("Starting up recommendation API")
    try:
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
def health():
    logger.debug("Health check requested")
    return {"status": "ok"}

@app.get("/metrics")
async def metrics():
    logger.debug("Metrics endpoint requested")
    return JSONResponse({"status": "ok"})

# --- Pydantic models for Swagger --------------------------------------------
class BookRow(BaseModel, extra="allow"):
    """Flexible model that accepts any catalog columns."""
    book_id: str = Field(..., example="B001")

class BooksResponse(BaseModel):
    rows: List[BookRow]

class MetricItem(BaseModel):
    label: str = Field(..., example="recommendation_served")
    value: int = Field(..., example=42)

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
async def recommend(
    student_id: str = Query(..., description="Student identifier (e.g., 'S001')", example="S001"),
    n: int = Query(3, ge=1, le=10, description="Number of recommendations to return", example=3),
    query: str = Query("", description="Optional search query to filter recommendations", example="space adventure")
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
        recs, meta = await generate_recommendations(student_id, query, n, request_id)

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
        port=S.api_port,
        reload=False,
    ) 
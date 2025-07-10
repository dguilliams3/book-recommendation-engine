"""Core recommendation logic independent of FastAPI layer.

This module handles:
1.   Launching the Fast-MCP subprocess via `mcp.stdio_client`.
2.   Loading LangChain-wrapped MCP tools and running the ReAct agent.
3.   Returning a structured list of book recommendations plus useful
     metrics (duration, tools used, etc.).

It deliberately contains no FastAPI imports so it can be reused from a
worker, a CLI, or tests without bringing in the web stack.

Performance Optimizations:
- Intelligent caching for student context and book metadata
- Connection pooling for database operations
- Async batch processing for bulk operations
- Performance monitoring and profiling
"""

from __future__ import annotations

import asyncio, json, os, time
from datetime import timedelta
from pathlib import Path
from typing import Any, List, Tuple
from collections import Counter

from mcp import StdioServerParameters, stdio_client, ClientSession
from langchain_openai import ChatOpenAI
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import AIMessage
from langchain.callbacks.base import AsyncCallbackHandler
from pydantic import BaseModel

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

from common.settings import settings as S
from common.structured_logging import get_logger
from common.performance import (
    cached, performance_context, get_cache, get_connection_pool,
    get_performance_monitor, BatchProcessor
)
from .prompts import build_student_prompt, build_reader_prompt, parser
from .scoring import score_candidates
from .candidate_builder import build_candidates
from common.redis_utils import mark_recommended
from common.reading_level_utils import get_student_reading_level_from_db
from .config import reader_config

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class BookRecommendation(BaseModel):
    """Schema for a single recommended book."""

    book_id: str
    title: str
    author: str
    reading_level: float
    librarian_blurb: str
    justification: str

class RecommendResponse(BaseModel):
    """API response model returned by FastAPI layer."""

    request_id: str
    duration_sec: float
    recommendations: List[BookRecommendation]
    student_avg_level: float | None = None
    recent_books: List[str] | None = None

# ---------------------------------------------------------------------------
# Internal helper classes
# ---------------------------------------------------------------------------

class MetricCallbackHandler(AsyncCallbackHandler):
    """Collect tool-usage stats during a LangChain agent run."""

    def __init__(self) -> None:
        self.tools_used: list[str] = []
        self.error_count: int = 0

    async def on_tool_start(self, serialized, _input_str, **_kw):  # type: ignore[override]
        name = serialized.get("name", "unknown") if isinstance(serialized, dict) else "unknown"
        self.tools_used.append(name)

    async def on_tool_error(self, _err, **_kw):  # type: ignore[override]
        self.error_count += 1

# ---------------------------------------------------------------------------
# One-time globals (vector store, model, etc.)
# ---------------------------------------------------------------------------

logger.info("Initialising ChatOpenAI model for recommendation agent")
chat_model = ChatOpenAI(
    model=S.model_name,
    api_key=S.openai_api_key,
    temperature=0.3,
    max_tokens=S.model_max_tokens,
)

logger.info("Preparing server parameters for MCP subprocess")
_mcp_path = Path(__file__).parent / "mcp_book_server.py"
server_params = StdioServerParameters(
    command="python",
    args=[str(_mcp_path)],
    env=dict(os.environ),
)

# Create database engine for recommendation history logging
logger.info("Initialising database engine for recommendation history")
async_db_url = str(S.db_url).replace("postgresql://", "postgresql+asyncpg://")
engine = create_async_engine(async_db_url, echo=False)

# Performance optimization: Initialize connection pool
connection_pool = None
batch_processor = BatchProcessor(batch_size=20, max_concurrent=5)

async def get_db_connection():
    """Get database connection from pool."""
    global connection_pool
    if connection_pool is None:
        connection_pool = await get_connection_pool()
    return await connection_pool.get_db_pool(async_db_url)

# ---------------------------------------------------------------------------
# Cached helper functions
# ---------------------------------------------------------------------------

@cached(ttl=300, cache_key_prefix="student_context")
async def _get_student_context_cached(student_id: str) -> Tuple[float, List[str], List[str], dict]:
    """Cached version of student context retrieval."""
    async with performance_context(f"student_context:{student_id}") as monitor:
        conn = None
        try:
            conn = await engine.begin()
            rows = await conn.execute(
                text("""
                     SELECT c.title, c.reading_level, c.genre
                       FROM checkout co
                       JOIN catalog c USING(book_id)
                      WHERE co.student_id = :sid
                   ORDER BY co.checkout_date DESC LIMIT 30"""),
                {"sid": student_id},
            )

            titles: list[str] = []
            levels: list[float] = []
            genres: list[str] = []

            def _level_to_band(g: float | None):
                if g is None:
                    return "Unknown"
                if g < 3.0:
                    return "Elementary"
                elif g < 6.0:
                    return "Middle"
                elif g < 9.0:
                    return "High School"
                else:
                    return "College"

            for row in rows:
                titles.append(row.title)
                if row.reading_level is not None:
                    levels.append(row.reading_level)
                if row.genre:
                    genres.append(row.genre)

            avg_level = sum(levels) / len(levels) if levels else None
            recent_titles = titles[:10]
            top_genres = [genre for genre, _ in Counter(genres).most_common(5)]

            band_hist = dict(Counter(_level_to_band(lv) for lv in levels))

            return avg_level, recent_titles, top_genres, band_hist
        except Exception as e:
            logger.error(f"Database error in student context fetch: {e}", exc_info=True, extra={"student_id": student_id})
            # Return sensible defaults on error
            return None, [], [], {}
        finally:
            if conn:
                await conn.close()

@cached(ttl=600, cache_key_prefix="user_uploaded_books")
async def _fetch_user_uploaded_books_cached(user_hash_id: str) -> List[dict]:
    """Cached version of user uploaded books retrieval."""
    async with performance_context(f"user_books:{user_hash_id}") as monitor:
        conn = None
        try:
            conn = await engine.begin()
            result = await conn.execute(
                text("""
                    SELECT book_id, title, author, isbn, genre, reading_level, 
                           user_rating, read_date, notes, created_at
                    FROM uploaded_books 
                    WHERE user_hash_id = :user_hash_id 
                    ORDER BY created_at DESC
                """),
                {"user_hash_id": user_hash_id}
            )
            
            books = []
            for row in result:
                books.append({
                    "book_id": row.book_id,
                    "title": row.title,
                    "author": row.author,
                    "isbn": row.isbn,
                    "genre": row.genre,
                    "reading_level": row.reading_level,
                    "user_rating": row.user_rating,
                    "read_date": row.read_date,
                    "notes": row.notes,
                    "created_at": row.created_at
                })
            
            return books
        except Exception as e:
            logger.error(f"Database error in user books fetch: {e}", exc_info=True, extra={"user_hash_id": user_hash_id})
            # Return empty list on error
            return []
        finally:
            if conn:
                await conn.close()

@cached(ttl=300, cache_key_prefix="user_feedback_scores")
async def _fetch_user_feedback_scores_cached(user_hash_id: str) -> dict:
    """Cached version of user feedback scores retrieval."""
    async with performance_context(f"user_feedback:{user_hash_id}") as monitor:
        conn = None
        try:
            conn = await engine.begin()
            result = await conn.execute(
                text("""
                    SELECT book_id, feedback_type, COUNT(*) as count
                    FROM feedback 
                    WHERE user_hash_id = :user_hash_id 
                    GROUP BY book_id, feedback_type
                """),
                {"user_hash_id": user_hash_id}
            )
            
            scores = {}
            for row in result:
                if row.book_id not in scores:
                    scores[row.book_id] = {"thumbs_up": 0, "thumbs_down": 0}
                scores[row.book_id][row.feedback_type] = row.count
            
            return scores
        except Exception as e:
            logger.error(f"Database error in feedback scores fetch: {e}", exc_info=True, extra={"user_hash_id": user_hash_id})
            # Return empty dict on error
            return {}
        finally:
            if conn:
                await conn.close()

# ---------------------------------------------------------------------------
# Public helper
# ---------------------------------------------------------------------------

async def generate_recommendations(
    student_id: str,
    query: str,
    n: int,
    request_id: str,
) -> Tuple[List[BookRecommendation], dict[str, Any]]:
    """Main orchestration entry-point for the API layer.

    The responsibilities are deliberately *narrow*:

    1. Validate student exists in the database
    2. Fetch & score candidate books (cheap, deterministic).
    3. Build a structured prompt and call the LLM agent (expensive, stochastic).
    4. Parse / validate the agent's answer and persist side-effects.

    All heavy I/O (DB queries, Redis, FAISS) lives in helper modules so we can
    unit-test this function with minimal monkey-patching (see tests).
    
    Performance optimizations:
    - Cached student context retrieval
    - Async database operations
    - Performance monitoring
    """

    async with performance_context(f"generate_recommendations:{student_id}") as monitor:
        logger.debug("Starting recommendation generation", extra={"request_id": request_id, "student_id": student_id})
        
        # Validate student exists before doing expensive operations
        try:
            async with engine.begin() as conn:
                student_exists = await conn.execute(
                    text("SELECT 1 FROM students WHERE student_id = :student_id LIMIT 1"),
                    {"student_id": student_id}
                )
                if not student_exists.fetchone():
                    logger.warning("Student not found in database", extra={"student_id": student_id, "request_id": request_id})
                    # Return error recommendation instead of failing
                    return [
                        BookRecommendation(
                            book_id="ERROR",
                            title="Student Not Found",
                            author="",
                            reading_level=0.0,
                            librarian_blurb=f"Student ID '{student_id}' was not found in the system. Please verify the student ID and try again. Available students in the database have IDs like 'S001', 'S002', etc.",
                            justification=""
                        )
                    ], {
                        "agent_duration": 0.0,
                        "tool_count": 0,
                        "tools": [],
                        "error_count": 1,
                        "error": "student_not_found"
                    }
        except Exception as e:
            logger.error("Database error during student validation", exc_info=True, extra={"student_id": student_id, "request_id": request_id})
            # Continue with recommendation generation even if validation fails
            
        logger.debug("Starting MCP session", extra={"request_id": request_id})
        start_ts = time.perf_counter()

        async with stdio_client(server_params) as (read, write):
            logger.debug("MCP stdio tunnel established", extra={"request_id": request_id})

            async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=300)) as session:
                await session.initialize()
                logger.debug("MCP session initialised", extra={"request_id": request_id})

                lc_tools = await load_mcp_tools(session)
                agent = create_react_agent(chat_model, lc_tools, name="BookRecommenderAgent")

                # --- Fetch student context (cached) -------------------------------------------------
                avg_level, recent_titles, top_genres, band_hist = await _get_student_context_cached(student_id)

                # --- Build candidates & prompt ---------------------------------------------------
                candidates = await build_candidates(student_id, n)
                scored_candidates = await score_candidates(candidates, student_id, query)

                prompt_messages = build_prompt(
                    student_id=student_id,
                    query=query,
                    candidates=scored_candidates,
                    avg_level=avg_level,
                    recent_titles=recent_titles,
                    top_genres=top_genres,
                    band_hist=band_hist,
                    n=n,
                )

                # --- Ask agent -------------------------------------------------------------------
                logger.debug("Calling LLM agent", extra={"request_id": request_id})
                callbacks = [MetricCallbackHandler()]
                agent_response = await _ask_agent(agent, prompt_messages, callbacks)

                # --- Parse & validate response ---------------------------------------------------
                try:
                    parsed_recs = parser.parse(agent_response)
                    valid_recs = [r for r in parsed_recs if r.book_id != "ERROR"]
                    
                    if not valid_recs:
                        logger.warning("No valid recommendations parsed", extra={"request_id": request_id})
                        # Return fallback recommendations
                        valid_recs = await _get_fallback_recommendations(n, engine)
                        
                except Exception as e:
                    logger.error("Failed to parse agent response", exc_info=True, extra={"request_id": request_id})
                    valid_recs = await _get_fallback_recommendations(n, engine)

                # --- Mark as recommended (fire-and-forget) ---------------------------------------
                try:
                    book_ids = [r.book_id for r in valid_recs]
                    await mark_recommended(student_id, book_ids, ttl_hours=24)
                except Exception as e:
                    logger.warning("Failed to mark books as recommended", exc_info=True, extra={"request_id": request_id})

                # --- Collect metrics -------------------------------------------------------------
                total_duration = time.perf_counter() - start_ts
                cb = callbacks[0] if callbacks else MetricCallbackHandler()
                
                metrics = {
                    "agent_duration": total_duration,
                    "tool_count": len(cb.tools_used),
                    "tools": cb.tools_used,
                    "error_count": cb.error_count,
                    "cache_enabled": True,
                    "performance_optimized": True
                }

                logger.info("Recommendation generation completed", extra={
                    "request_id": request_id,
                    "student_id": student_id,
                    "duration_sec": total_duration,
                    "recommendations_count": len(valid_recs),
                    "tools_used": cb.tools_used,
                    "error_count": cb.error_count
                })

                return valid_recs, metrics

# ---------------------------------------------------------------------------
# Small utility
# ---------------------------------------------------------------------------

async def _fetch_user_uploaded_books(user_hash_id: str, conn) -> List[dict]:
    """Fetch user's uploaded books from database."""
    uploaded_books_result = await conn.execute(
        text("""
            SELECT ub.title, ub.author, ub.rating, ub.notes, ub.raw_payload
            FROM uploaded_books ub
            JOIN public_users pu ON ub.user_id = pu.id
            WHERE pu.hash_id = :user_hash_id 
            ORDER BY ub.created_at DESC 
            LIMIT :limit
        """),
        {"user_hash_id": user_hash_id, "limit": reader_config.MAX_UPLOADED_BOOKS}
    )
    
    uploaded_books = []
    for row in uploaded_books_result.fetchall():
        # Extract genre and reading_level from raw_payload if available
        raw_payload = row.raw_payload or {}
        uploaded_books.append({
            "title": row.title,
            "author": row.author,
            "genre": raw_payload.get("genre"),
            "reading_level": raw_payload.get("reading_level"),
            "rating": row.rating,
            "notes": row.notes
        })
    
    return uploaded_books


async def _fetch_user_feedback_scores(user_hash_id: str, conn) -> dict:
    """Fetch user's feedback scores from database."""
    feedback_result = await conn.execute(
        text("""
            SELECT f.book_id, f.score 
            FROM feedback f
            JOIN public_users pu ON f.user_id = pu.id
            WHERE pu.hash_id = :user_hash_id
        """),
        {"user_hash_id": user_hash_id}
    )
    
    feedback_scores = {}
    for row in feedback_result.fetchall():
        book_id = row.book_id
        score = row.score
        feedback_scores[book_id] = feedback_scores.get(book_id, 0) + score
    
    return feedback_scores


async def _fetch_similar_books(uploaded_books: List[dict], user_hash_id: str, conn) -> List[dict]:
    """Fetch similar books from catalog based on user's uploaded books."""
    user_genres = [book["genre"] for book in uploaded_books if book.get("genre")]
    user_authors = [book["author"] for book in uploaded_books if book.get("author")]
    
    # Build dynamic query based on available data
    where_conditions = []
    query_params = {"user_hash_id": user_hash_id}
    
    if user_genres:
        where_conditions.append("c.genre = ANY(:user_genres)")
        query_params["user_genres"] = user_genres
    
    if user_authors:
        where_conditions.append("c.author = ANY(:user_authors)")
        query_params["user_authors"] = user_authors
    
    # If no specific criteria, get popular books
    if not where_conditions:
        where_conditions.append(f"c.average_rating > {reader_config.MIN_RATING_THRESHOLD}")
    
    similar_books_result = await conn.execute(
        text(f"""
            SELECT DISTINCT c.book_id, c.title, c.author, c.genre, c.reading_level, 
                   c.description as librarian_blurb, c.isbn, c.average_rating
            FROM catalog c
            WHERE {' OR '.join(where_conditions)}
            ORDER BY c.average_rating DESC NULLS LAST, c.book_id
            LIMIT :limit
        """),
        {**query_params, "limit": reader_config.MAX_SIMILAR_BOOKS}
    )
    
    similar_books = []
    for row in similar_books_result.fetchall():
        similar_books.append({
            "book_id": row.book_id,
            "title": row.title,
            "author": row.author,
            "genre": row.genre,
            "reading_level": row.reading_level or 5.0,
            "librarian_blurb": row.librarian_blurb,
            "isbn": row.isbn,
            "average_rating": row.average_rating
        })
    
    return similar_books


def _calculate_similarity_score(book: dict, uploaded_books: List[dict], feedback_scores: dict, query: str = "") -> float:
    """Calculate similarity score between a catalog book and user's uploaded books."""
    similarity_score = 0.0
    
    # Genre matching (configurable weight)
    book_genre = book["genre"]
    for uploaded_book in uploaded_books:
        if uploaded_book.get("genre") == book_genre:
            similarity_score += reader_config.GENRE_MATCH_WEIGHT
    
    # Author matching (configurable weight)
    book_author = book["author"]
    for uploaded_book in uploaded_books:
        if uploaded_book.get("author") == book_author:
            similarity_score += reader_config.AUTHOR_MATCH_WEIGHT
    
    # Reading level proximity (configurable weights based on closeness)
    book_level = book["reading_level"]
    user_avg_level = sum(
        (book.get("reading_level") or 5.0) for book in uploaded_books
    ) / len(uploaded_books)
    
    level_diff = abs(book_level - user_avg_level)
    if level_diff <= 1.0:
        similarity_score += reader_config.READING_LEVEL_WEIGHT_HIGH
    elif level_diff <= 2.0:
        similarity_score += reader_config.READING_LEVEL_WEIGHT_MEDIUM
    elif level_diff <= 3.0:
        similarity_score += reader_config.READING_LEVEL_WEIGHT_LOW
    
    # Apply feedback score adjustment (configurable weight)
    book_id = book["book_id"]
    if reader_config.ENABLE_FEEDBACK_SCORING and book_id in feedback_scores:
        similarity_score += feedback_scores[book_id] * reader_config.FEEDBACK_WEIGHT
    
    # Boost highly rated uploaded books' similar titles (configurable weight)
    for uploaded_book in uploaded_books:
        if uploaded_book.get("rating", 0) >= 4:
            if (uploaded_book.get("genre") == book_genre or 
                uploaded_book.get("author") == book_author):
                similarity_score += reader_config.RATING_BOOST_WEIGHT
    
    # Query matching if provided (configurable weight)
    if reader_config.ENABLE_QUERY_MATCHING and query:
        query_lower = query.lower()
        if (query_lower in (book["title"] or "").lower() or 
            query_lower in (book["author"] or "").lower() or
            query_lower in (book["genre"] or "").lower()):
            similarity_score += reader_config.QUERY_MATCH_WEIGHT
    
    return similarity_score


def _create_justification(book: dict, uploaded_books: List[dict], feedback_scores: dict) -> str:
    """Create justification text for why a book was recommended."""
    justification_parts = []
    
    # Check what made this book similar
    for uploaded_book in uploaded_books:
        if uploaded_book.get("genre") == book["genre"]:
            justification_parts.append(f"Similar genre ({book['genre']})")
            break
    
    for uploaded_book in uploaded_books:
        if uploaded_book.get("author") == book["author"]:
            justification_parts.append(f"Same author ({book['author']})")
            break
    
    book_id = book["book_id"]
    if book_id in feedback_scores and feedback_scores[book_id] > 0:
        justification_parts.append("Based on your positive feedback")
    
    if not justification_parts:
        justification_parts.append("Matches your reading preferences")
    
    return f"Recommended because: {', '.join(justification_parts)}"


async def _get_fallback_recommendations(n: int, conn) -> List[dict]:
    """Get popular books as fallback when not enough similar books found."""
    fallback_result = await conn.execute(
        text("""
            SELECT c.book_id, c.title, c.author, c.genre, c.reading_level, 
                   c.description as librarian_blurb
            FROM catalog c
            WHERE c.average_rating > :threshold
            ORDER BY c.average_rating DESC NULLS LAST
            LIMIT :n
        """),
        {"n": n, "threshold": reader_config.FALLBACK_RATING_THRESHOLD}
    )
    
    fallback_books = []
    for row in fallback_result.fetchall():
        fallback_books.append({
            "book_id": row.book_id,
            "title": row.title,
            "author": row.author,
            "genre": row.genre,
            "reading_level": row.reading_level or 5.0,
            "librarian_blurb": row.librarian_blurb
        })
    
    return fallback_books


async def generate_reader_recommendations(
    user_hash_id: str,
    query: str,
    n: int,
    request_id: str,
) -> Tuple[List[BookRecommendation], dict[str, Any]]:
    """Generate personalized book recommendations for a reader based on their uploaded books."""
    
    logger.info("Starting reader recommendation generation", extra={
        "request_id": request_id,
        "user_hash_id": user_hash_id,
        "n": n,
        "query": query
    })
    
    start_ts = time.perf_counter()
    
    try:
        async with engine.begin() as conn:
            # Fetch user's uploaded books
            uploaded_books = await _fetch_user_uploaded_books(user_hash_id, conn)
            
            if not uploaded_books:
                logger.warning("No uploaded books found for user", extra={"user_hash_id": user_hash_id})
                return [], {
                    "agent_duration": 0.0,
                    "tool_count": 0,
                    "tools": [],
                    "error_count": 1,
                    "error": "no_uploaded_books",
                    "based_on_books": []
                }
            
            # Fetch user's feedback history
            feedback_scores = await _fetch_user_feedback_scores(user_hash_id, conn)
            
            # Find similar books in catalog
            similar_books = await _fetch_similar_books(uploaded_books, user_hash_id, conn)
            
            # Calculate similarity scores for all candidates
            candidates = []
            for book in similar_books:
                similarity_score = _calculate_similarity_score(book, uploaded_books, feedback_scores, query)
                book["similarity_score"] = similarity_score
                candidates.append(book)
            
            # Sort by similarity score and take top N
            candidates.sort(key=lambda x: x["similarity_score"], reverse=True)
            top_candidates = candidates[:n]
            
            # If we don't have enough recommendations, add popular books
            if len(top_candidates) < n:
                needed = n - len(top_candidates)
                fallback_books = await _get_fallback_recommendations(needed, conn)
                
                # Add fallback books with default similarity score
                for book in fallback_books:
                    book["similarity_score"] = reader_config.FALLBACK_SCORE
                    top_candidates.append(book)
            
            # Build final recommendations
            recommendations = []
            for candidate in top_candidates:
                justification = _create_justification(candidate, uploaded_books, feedback_scores)
                
                recommendation = BookRecommendation(
                    book_id=candidate["book_id"],
                    title=candidate["title"] or "Unknown Title",
                    author=candidate["author"] or "Unknown Author",
                    reading_level=candidate["reading_level"],
                    librarian_blurb=candidate["librarian_blurb"] or "No description available",
                    justification=justification
                )
                recommendations.append(recommendation)
            
            # Calculate metrics
            duration = time.perf_counter() - start_ts
            
            # Based on books for metadata
            based_on_books = [f"{book['title']} by {book['author']}" for book in uploaded_books[:3]]
            
            logger.info("Reader recommendation generation completed", extra={
                "request_id": request_id,
                "user_hash_id": user_hash_id,
                "recommendations_count": len(recommendations),
                "duration_sec": duration,
                "based_on_books_count": len(uploaded_books)
            })
            
            return recommendations, {
                "agent_duration": duration,
                "tool_count": 1,
                "tools": ["similarity_matching"],
                "error_count": 0,
                "based_on_books": based_on_books
            }
            
    except Exception as exc:
        duration = time.perf_counter() - start_ts
        logger.error("Reader recommendation generation failed", extra={
            "request_id": request_id,
            "user_hash_id": user_hash_id,
            "duration_sec": duration,
            "error": str(exc)
        }, exc_info=True)
        
        return [], {
            "agent_duration": duration,
            "tool_count": 0,
            "tools": [],
            "error_count": 1,
            "error": str(exc),
            "based_on_books": []
        }

async def _ask_agent(agent, prompt_messages, callbacks=None):
    """Run the LangChain agent and return final AIMessage.

    ``prompt_messages`` can be a string or a list of LangChain ``BaseMessage``
    instances – whatever ``agent.ainvoke`` accepts via the ``messages`` field.
    """
    cfg = {"callbacks": callbacks} if callbacks else None
    response = await agent.ainvoke({"messages": prompt_messages}, config=cfg)
    final_msg = next(m for m in reversed(response["messages"]) if isinstance(m, AIMessage))
    return final_msg, response 

async def generate_agent_recommendations(
    mode: str,  # "student" or "reader"
    context: dict,
    query: str,
    n: int,
    request_id: str,
) -> Tuple[List[BookRecommendation], dict[str, Any]]:
    """Unified agent-based recommendation function for both Student and Reader modes."""
    logger.info("Starting agent-based recommendation", extra={
        "request_id": request_id,
        "mode": mode,
        "n": n,
        "query": query
    })
    start_ts = time.perf_counter()
    
    try:
        # Setup MCP session and tools (same as existing student function)
        async with stdio_client(server_params) as (read, write):
            logger.debug("MCP stdio tunnel established", extra={"request_id": request_id})

            async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=300)) as session:
                await session.initialize()
                logger.debug("MCP session initialised", extra={"request_id": request_id})

                lc_tools = await load_mcp_tools(session)
                agent = create_react_agent(chat_model, lc_tools, name="BookRecommenderAgent")

                # Build prompt based on mode
                if mode == "student":
                    prompt_messages = build_student_prompt(
                        student_id=context["student_id"],
                        query=query,
                        candidates=context["candidates"],
                        avg_level=context["avg_level"],
                        recent_titles=context["recent_titles"],
                        top_genres=context["top_genres"],
                        band_hist=context["band_hist"],
                        n=n,
                    )
                elif mode == "reader":
                    prompt_messages = build_reader_prompt(
                        user_hash_id=context["user_hash_id"],
                        query=query,
                        uploaded_books=context["uploaded_books"],
                        feedback_scores=context["feedback_scores"],
                        candidates=context["candidates"],
                        n=n,
                    )
                else:
                    raise ValueError(f"Unknown mode: {mode}")

                # Ask agent
                logger.debug("Calling LLM agent", extra={"request_id": request_id})
                callbacks = [MetricCallbackHandler()]
                agent_response = await _ask_agent(agent, prompt_messages, callbacks)

                # Parse agent response
                try:
                    parsed_recs = parser.parse(agent_response)
                    valid_recs = [r for r in parsed_recs if r.book_id != "ERROR"]
                    
                    if not valid_recs:
                        logger.warning("No valid recommendations parsed", extra={"request_id": request_id})
                        # Return fallback recommendations
                        async with engine.begin() as conn:
                            valid_recs = await _get_fallback_recommendations(n, conn)
                        
                except Exception as e:
                    logger.error("Failed to parse agent response", exc_info=True, extra={"request_id": request_id})
                    # Return fallback recommendations
                    async with engine.begin() as conn:
                        valid_recs = await _get_fallback_recommendations(n, conn)

                # Mark as recommended (fire-and-forget) for student mode
                if mode == "student":
                    try:
                        book_ids = [r.book_id for r in valid_recs]
                        await mark_recommended(context["student_id"], book_ids, ttl_hours=24)
                    except Exception as e:
                        logger.warning("Failed to mark books as recommended", exc_info=True, extra={"request_id": request_id})

                # Collect metrics
                duration = time.perf_counter() - start_ts
                cb = callbacks[0] if callbacks else MetricCallbackHandler()
                
                metrics = {
                    "agent_duration": duration,
                    "tool_count": len(cb.tools_used),
                    "tools": cb.tools_used,
                    "error_count": cb.error_count,
                    "mode": mode,
                    "cache_enabled": True,
                    "performance_optimized": True
                }

                logger.info("Agent-based recommendation completed", extra={
                    "request_id": request_id,
                    "mode": mode,
                    "duration_sec": duration,
                    "recommendations_count": len(valid_recs),
                    "tools_used": cb.tools_used,
                    "error_count": cb.error_count
                })

                return valid_recs, metrics
                
    except Exception as exc:
        duration = time.perf_counter() - start_ts
        logger.error("Agent-based recommendation failed", extra={
            "request_id": request_id,
            "mode": mode,
            "duration_sec": duration,
            "error": str(exc)
        }, exc_info=True)
        
        # Return fallback recommendations on error
        try:
            async with engine.begin() as conn:
                fallback_recs = await _get_fallback_recommendations(n, conn)
                return fallback_recs, {
                    "agent_duration": duration,
                    "tool_count": 0,
                    "tools": [],
                    "error_count": 1,
                    "error": str(exc),
                    "mode": mode
                }
        except Exception as fallback_error:
            logger.error("Failed to get fallback recommendations", exc_info=True, extra={"request_id": request_id})
            return [], {
                "agent_duration": duration,
                "tool_count": 0,
                "tools": [],
                "error_count": 1,
                "error": str(exc),
                "mode": mode
            } 
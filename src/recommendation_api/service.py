"""Core recommendation logic independent of FastAPI layer.

This module handles:
1.   Launching the Fast-MCP subprocess via `mcp.stdio_client`.
2.   Loading LangChain-wrapped MCP tools and running the ReAct agent.
3.   Returning a structured list of book recommendations plus useful
     metrics (duration, tools used, etc.).

It deliberately contains no FastAPI imports so it can be reused from a
worker, a CLI, or tests without bringing in the web stack.
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

from common import SettingsInstance as S
from common.structured_logging import get_logger
from .prompts import build_prompt, parser
from .scoring import score_candidates
from .candidate_builder import build_candidates
from common.redis_utils import mark_recommended
from common.reading_level_utils import get_student_reading_level_from_db

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
    """

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

            # --- Fetch student context -------------------------------------------------
            async def _student_context():
                """Return rich student context for prompt & meta.

                Returns:
                    tuple(avg_level, recent_titles, top_genres, band_hist)
                """
                async with engine.begin() as conn:
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
                            return None
                        if g <= 2.0:
                            return "beginner"
                        if g <= 4.0:
                            return "early_elementary"
                        if g <= 6.0:
                            return "late_elementary"
                        if g <= 8.0:
                            return "middle_school"
                        return "advanced"

                    bands: list[str] = []

                    for r in rows:
                        title, rl, genre = r[0], r[1], r[2]
                        if title:
                            titles.append(title)
                        if rl is not None:
                            levels.append(float(rl))
                            band = _level_to_band(float(rl))
                            if band:
                                bands.append(band)
                        if genre:
                            genres.append(genre)

                    avg_level = round(sum(levels) / len(levels), 1) if levels else None
                    top_genres = [g for g, _ in Counter(genres).most_common(3)] if genres else []
                    band_hist = dict(Counter(bands)) if bands else {}
                    recent_titles = titles[:5]

                    return avg_level, recent_titles, top_genres, band_hist

            avg_level, recent_titles, top_genres, band_hist = await _student_context()

            # --- EDGE CASE FIX: Get reading level for new students ---
            # This enables the α-term (reading_match) in scoring which is critical for quality
            if avg_level is None:
                try:
                    async with engine.begin() as conn:
                        # Convert engine to asyncpg pool-like interface
                        from asyncpg import create_pool
                        pool_url = str(S.db_url).replace("postgresql://", "postgresql://").replace("postgresql+asyncpg://", "postgresql://")
                        pool = await create_pool(pool_url, min_size=1, max_size=1)
                        try:
                            rl_info = await get_student_reading_level_from_db(student_id, pool)
                            avg_level = rl_info.get("avg_reading_level")
                            logger.debug("Fetched reading level for new student", extra={
                                "student_id": student_id, 
                                "reading_level": avg_level,
                                "method": rl_info.get("method")
                            })
                        finally:
                            await pool.close()
                except Exception:
                    logger.warning("Failed to compute reading level for new student", exc_info=True)
                    avg_level = 4.0  # Safe fallback to 4th grade level

            # Build human-readable context string for the LLM
            ctx_parts: list[str] = []
            ctx_parts.append("Student average RL: " + (f"{avg_level}" if avg_level else "unknown"))
            if recent_titles:
                ctx_parts.append("Recent books: " + ", ".join(recent_titles))
            if top_genres:
                ctx_parts.append("Top genres: " + ", ".join(top_genres))
            if band_hist:
                band_str = ", ".join(f"{b}:{cnt}" for b, cnt in band_hist.items())
                ctx_parts.append("Difficulty bands: " + band_str)
            # Include search keywords if provided
            if query and query.strip():
                ctx_parts.append("Search keywords: " + query.strip())

            context_line = "; ".join(ctx_parts) + "\n"

            # --- DEBUG: log constructed context string ----------------------
            logger.debug(
                "LLM context_line created",
                extra={
                    "student_id": student_id,
                    "context_line": context_line,
                },
            )

            # ------------------------------------------------------------------
            # Build candidates → score → prompt → LLM
            # ------------------------------------------------------------------

            try:
                candidates = await build_candidates(student_id, k=n * 4, query=query)
            except Exception:
                logger.warning("Candidate builder failed, falling back to empty list", exc_info=True)
                candidates = []
            
            # --- EDGE CASE FIX: Populate student_level for scoring ---
            # This enables the α-term (reading_match_weight) which is crucial for quality
            for cand in candidates:
                cand["student_level"] = avg_level
            
            ranked_scores = score_candidates(candidates)
            ranked = ranked_scores[: n * 2]

            # --- EDGE CASE FIX: Ensure we always have candidates ---
            if not ranked:
                logger.warning("No ranked candidates available, using popular fallback", extra={"student_id": student_id})
                try:
                    # Get popular books as emergency fallback
                    async with engine.begin() as conn:
                        fallback_rows = await conn.execute(
                            text("""
                                SELECT book_id, title, author, reading_level
                                FROM catalog
                                ORDER BY RANDOM()  -- In production, use actual popularity metric
                                LIMIT :limit
                            """),
                            {"limit": n * 2}
                        )
                        for row in fallback_rows:
                            fallback_cand = {
                                "book_id": row[0],
                                "title": row[1], 
                                "author": row[2],
                                "level": row[3],
                                "student_level": avg_level,
                                "neighbour_recent": 0,
                                "staff_pick": False,
                                "semantic_candidate": False
                            }
                            ranked.append((0.1, fallback_cand))  # Low score fallback
                except Exception:
                    logger.error("Even fallback candidate generation failed", exc_info=True)

            # --- Enrich metadata for ranked candidates ----------------------
            try:
                needed_ids = [c[1]["book_id"] for c in ranked]
                async with engine.begin() as conn:
                    if needed_ids:
                        rows_meta = await conn.execute(
                            text("SELECT book_id, title, author, reading_level FROM catalog WHERE book_id = ANY(:ids)"),
                            {"ids": needed_ids},
                        )
                        meta_map = {r[0]: r for r in rows_meta}

                missing_title_cnt = 0
                for _score, cand in ranked:
                    meta = meta_map.get(cand["book_id"]) if 'meta_map' in locals() else None
                    if meta:
                        if not cand.get("title"):
                            cand["title"] = meta[1]
                        if not cand.get("author"):
                            cand["author"] = meta[2]
                        if cand.get("level") is None:
                            cand["level"] = meta[3]
                    if not cand.get("title"):
                        missing_title_cnt += 1

                logger.debug("Ranked candidate metadata enriched", extra={
                    "student_id": student_id,
                    "ranked_count": len(ranked),
                    "missing_title_count": missing_title_cnt,
                })
            except Exception:
                logger.debug("Metadata enrichment for ranked candidates failed", exc_info=True)

            logger.info(
                "Candidates ranked",
                extra={
                    "student_id": student_id,
                    "candidates": len(candidates),
                    "ranked_used": len(ranked),
                },
            )

            # Convert to brief text for LLM context (title+book_id placeholders for now)
            cand_lines = [f"{c['book_id']}: {c.get('title', 'Unknown')} by {c.get('author', 'Unknown')} (RL {c.get('level', 'N/A')})" for _, c in ranked]
            raw_user_prompt = (
                context_line +
                "Recommend {n} books for student {sid}. Choices:\n".format(n=n, sid=student_id)
                + "\n".join(cand_lines)
            )

            # --- DEBUG: log final user prompt sent to build_prompt -----------
            logger.debug(
                "Raw user_prompt built",
                extra={
                    "student_id": student_id,
                    "raw_user_prompt": raw_user_prompt,
                },
            )

            prompt_messages = build_prompt(raw_user_prompt)

            callback = MetricCallbackHandler()
            
            # --- EDGE CASE FIX: Guard agent calls ---
            try:
                ai_msg, _unused = await _ask_agent(agent, prompt_messages, callbacks=[callback])
            except Exception as e:
                logger.error("LLM agent call failed, using fallback recommendations", exc_info=True, extra={
                    "student_id": student_id,
                    "request_id": request_id,
                    "error": str(e)
                })
                # Return top-ranked candidates as fallback
                fallback_recommendations = []
                for _score, cand in ranked[:n]:
                    fallback_recommendations.append(BookRecommendation(
                        book_id=cand["book_id"],
                        title=cand.get("title", "Unknown Title"),
                        author=cand.get("author", "Unknown Author"), 
                        reading_level=cand.get("level", avg_level or 4.0),
                        librarian_blurb="Recommended based on your reading preferences. LLM service temporarily unavailable.",
                        justification=f"Selected from top candidates (score: {_score:.2f})"
                    ))
                
                return fallback_recommendations, {
                    "agent_duration": time.perf_counter() - start_ts,
                    "tool_count": 0,
                    "tools": [],
                    "error_count": 1,
                    "error": "llm_failure",
                    "fallback_used": True
                }

    duration = time.perf_counter() - start_ts

    # Parse payload using the strict Pydantic parser first, then fall back
    recommendations: List[BookRecommendation]
    try:
        parsed = parser.parse(ai_msg.content)
        
        # Fetch book metadata for all recommended books
        book_ids = [rec.book_id for rec in parsed.recommendations]
        book_metadata = {}
        
        try:
            async with engine.begin() as conn:
                if book_ids:
                    metadata_rows = await conn.execute(
                        text("SELECT book_id, title, author, reading_level FROM catalog WHERE book_id = ANY(:book_ids)"),
                        {"book_ids": book_ids}
                    )
                    for row in metadata_rows:
                        book_metadata[row[0]] = {
                            "book_id": row[0],
                            "title": row[1], 
                            "author": row[2],
                            "reading_level": row[3]
                        }
                    
                    logger.info("Book metadata fetched", extra={
                        "book_ids": book_ids,
                        "metadata_found": len(book_metadata),
                        "metadata_keys": list(book_metadata.keys())
                    })
        except Exception as e:
            logger.warning("Failed to fetch book metadata", exc_info=True, extra={"book_ids": book_ids})
        
        recommendations = []
        for rec in parsed.recommendations:
            metadata = book_metadata.get(rec.book_id, {})
            recommendation = BookRecommendation(
                book_id=rec.book_id,
                title=metadata.get("title", rec.title),
                author=metadata.get("author", rec.author),
                reading_level=metadata.get("reading_level", rec.reading_level),
                librarian_blurb=rec.librarian_blurb,
                justification=rec.justification,
            )
            recommendations.append(recommendation)
            
            logger.info("Recommendation constructed", extra={
                "book_id": rec.book_id,
                "title": recommendation.title,
                "author": recommendation.author,
                "reading_level": recommendation.reading_level,
                "has_metadata": bool(metadata)
            })
    except Exception:
        # Fallback to legacy best-effort parsing so API never fails
        logger.warning(
            "Structured parse failed – falling back to json.loads", extra={"request_id": request_id}
        )
        try:
            data = json.loads(ai_msg.content)
            if isinstance(data, dict):
                data = data.get("recommendations", [])
            
            # Fetch book metadata for fallback parsing
            book_ids = [d.get("book_id") for d in data if d.get("book_id")]
            book_metadata = {}
            
            try:
                async with engine.begin() as conn:
                    if book_ids:
                        metadata_rows = await conn.execute(
                            text("SELECT book_id, title, author, reading_level FROM catalog WHERE book_id = ANY(:book_ids)"),
                            {"book_ids": book_ids}
                        )
                        for row in metadata_rows:
                            book_metadata[row[0]] = {
                                "book_id": row[0],
                                "title": row[1],
                                "author": row[2], 
                                "reading_level": row[3]
                            }
            except Exception as e:
                logger.warning("Failed to fetch book metadata for fallback", exc_info=True)
            
            recommendations = []
            for d in data:
                metadata = book_metadata.get(d.get("book_id"), {})
                recommendations.append(BookRecommendation(
                    book_id=d.get("book_id", "UNKNOWN"),
                    title=metadata.get("title", d.get("title", "Unknown")),
                    author=metadata.get("author", d.get("author", "Unknown")),
                    reading_level=metadata.get("reading_level", d.get("reading_level", 0.0)),
                    librarian_blurb=d.get("librarian_blurb", "No description available"),
                    justification=d.get("justification", ""),
                ))
        except Exception:
            logger.warning(
                "Agent returned unparseable output, wrapping raw content", extra={"request_id": request_id}
            )
            recommendations = [
                BookRecommendation(
                    book_id="UNKNOWN",
                    title="See message",
                    author="",
                    reading_level=0.0,
                    librarian_blurb=ai_msg.content,
                    justification="",
                )
            ]

    # --- EDGE CASE FIX: Ensure we return exactly n recommendations ---
    if len(recommendations) < n:
        logger.info("Top-up needed for recommendations", extra={
            "received": len(recommendations),
            "target": n,
            "student_id": student_id
        })
        
        # Use remaining ranked candidates to fill the gap
        used_book_ids = {rec.book_id for rec in recommendations}
        for _score, cand in ranked:
            if len(recommendations) >= n:
                break
            if cand["book_id"] not in used_book_ids:
                # Fetch metadata for top-up candidate
                try:
                    async with engine.begin() as conn:
                        meta_row = await conn.execute(
                            text("SELECT title, author, reading_level FROM catalog WHERE book_id = :book_id"),
                            {"book_id": cand["book_id"]}
                        )
                        meta = meta_row.fetchone() if meta_row else None
                        
                    recommendations.append(BookRecommendation(
                        book_id=cand["book_id"],
                        title=meta[0] if meta else cand.get("title", "Unknown Title"),
                        author=meta[1] if meta else cand.get("author", "Unknown Author"),
                        reading_level=meta[2] if meta else cand.get("level", avg_level or 4.0),
                        librarian_blurb="Additional recommendation based on your reading profile.",
                        justification=f"Top-up candidate (score: {_score:.2f})"
                    ))
                    used_book_ids.add(cand["book_id"])
                except Exception:
                    logger.debug("Failed to fetch metadata for top-up candidate", extra={"book_id": cand["book_id"]})
                    continue

    meta = {
        "agent_duration": duration,
        "tool_count": len(callback.tools_used),
        "tools": callback.tools_used[:10],
        "error_count": callback.error_count,
    }

    # Expose student context for privileged users only
    if os.getenv("SUPER_USER") == "1":
        meta["student_avg_level"] = avg_level
        meta["recent_books"] = recent_titles
        meta["top_genres"] = top_genres
        meta["band_histogram"] = band_hist

    # ------------------------------------------------------------------
    # Idempotent history logging
    # ------------------------------------------------------------------
    try:
        async with engine.begin() as conn:
            for rec in recommendations:
                await conn.execute(
                    text("INSERT INTO recommendation_history(student_id, book_id, justification, recommended_at) VALUES(:sid, :bid, :just, NOW()) ON CONFLICT DO NOTHING"),
                    {"sid": student_id, "bid": rec.book_id, "just": rec.justification},
                )
                # mark in Redis Bloom / set
                await mark_recommended(student_id, rec.book_id)
    except Exception:
        logger.warning("Failed to write recommendation_history", exc_info=True)

    return recommendations, meta

# ---------------------------------------------------------------------------
# Small utility
# ---------------------------------------------------------------------------

async def _ask_agent(agent, prompt_messages, callbacks=None):
    """Run the LangChain agent and return final AIMessage.

    ``prompt_messages`` can be a string or a list of LangChain ``BaseMessage``
    instances – whatever ``agent.ainvoke`` accepts via the ``messages`` field.
    """
    cfg = {"callbacks": callbacks} if callbacks else None
    response = await agent.ainvoke({"messages": prompt_messages}, config=cfg)
    final_msg = next(m for m in reversed(response["messages"]) if isinstance(m, AIMessage))
    return final_msg, response 
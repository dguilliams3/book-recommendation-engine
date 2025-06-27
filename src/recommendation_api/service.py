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
                async with engine.begin() as conn:
                    rows = await conn.execute(
                        text("""
                             SELECT c.title, c.reading_level
                               FROM checkout co
                               JOIN catalog c USING(book_id)
                              WHERE co.student_id = :sid
                           ORDER BY co.checkout_date DESC LIMIT 5"""),
                        {"sid": student_id},
                    )
                    titles = []
                    levels = []
                    for r in rows:
                        title, rl = r[0], r[1]
                        if title:
                            titles.append(title)
                        if rl is not None:
                            levels.append(float(rl))
                    avg_level = round(sum(levels) / len(levels), 1) if levels else None
                    return avg_level, titles
            avg_level, recent_titles = await _student_context()
            context_line = "Student average RL: " + (f"{avg_level}" if avg_level else "unknown")
            if recent_titles:
                context_line += "; recent books: " + ", ".join(recent_titles)
            context_line += "\n"

            # ------------------------------------------------------------------
            # Build candidates → score → prompt → LLM
            # ------------------------------------------------------------------

            try:
                candidates = await build_candidates(student_id, k=n * 4, query=query)
            except Exception:
                logger.warning("Candidate builder failed, falling back to empty list", exc_info=True)
                candidates = []
            ranked = score_candidates(candidates)[: n * 2]

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

            prompt_messages = build_prompt(raw_user_prompt)

            callback = MetricCallbackHandler()
            ai_msg, _unused = await _ask_agent(agent, prompt_messages, callbacks=[callback])

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
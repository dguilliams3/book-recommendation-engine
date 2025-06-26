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

from common import SettingsInstance as S
from common.structured_logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class BookRecommendation(BaseModel):
    """Schema for a single recommended book."""

    book_id: str
    title: str
    librarian_blurb: str

class RecommendResponse(BaseModel):
    """API response model returned by FastAPI layer."""

    request_id: str
    duration_sec: float
    recommendations: List[BookRecommendation]

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
chat_model = ChatOpenAI(model=S.model_name, api_key=S.openai_api_key, temperature=0.3)

logger.info("Preparing server parameters for MCP subprocess")
_mcp_path = Path(__file__).parent / "mcp_book_server.py"
server_params = StdioServerParameters(
    command="python",
    args=[str(_mcp_path)],
    env=dict(os.environ),
)

# ---------------------------------------------------------------------------
# Public helper
# ---------------------------------------------------------------------------

async def generate_recommendations(
    student_id: str,
    query: str,
    n: int,
    request_id: str,
) -> Tuple[List[BookRecommendation], dict[str, Any]]:
    """Return list of recommendations plus metrics.

    Parameters
    ----------
    student_id: str
        ID of the elementary-school student.
    query: str
        Interest keywords supplied by the caller.
    n: int
        Number of books to recommend.
    request_id: str
        Correlates logs/metrics with one API request.
    """

    logger.debug("Starting MCP session", extra={"request_id": request_id})
    start_ts = time.perf_counter()

    async with stdio_client(server_params) as (read, write):
        logger.debug("MCP stdio tunnel established", extra={"request_id": request_id})

        async with ClientSession(read, write, read_timeout_seconds=timedelta(seconds=300)) as session:
            await session.initialize()
            logger.debug("MCP session initialised", extra={"request_id": request_id})

            lc_tools = await load_mcp_tools(session)
            agent = create_react_agent(chat_model, lc_tools, name="BookRecommenderAgent")

            user_prompt = (
                f"Recommend {n} books suitable for a grade-4 student with id {student_id}. "
                f"Their interest keywords: '{query}'. Provide JSON list with book_id, title, librarian_blurb."
            )

            callback = MetricCallbackHandler()
            ai_msg, _unused = await _ask_agent(agent, user_prompt, callbacks=[callback])

    duration = time.perf_counter() - start_ts

    # Parse JSON payload from AI response
    recommendations: List[BookRecommendation]
    try:
        data = json.loads(ai_msg.content)
        if isinstance(data, dict):
            data = data.get("recommendations", [])
        recommendations = [BookRecommendation(**d) for d in data]
    except Exception:
        logger.warning("Agent returned non-JSON, wrapping raw content", extra={"request_id": request_id})
        recommendations = [
            BookRecommendation(
                book_id="UNKNOWN",
                title="See message",
                librarian_blurb=ai_msg.content,
            )
        ]

    meta = {
        "agent_duration": duration,
        "tool_count": len(callback.tools_used),
        "tools": callback.tools_used[:10],
        "error_count": callback.error_count,
    }
    return recommendations, meta

# ---------------------------------------------------------------------------
# Small utility
# ---------------------------------------------------------------------------

async def _ask_agent(agent, prompt: str, callbacks=None):
    """Run the LangChain agent with optional callbacks and return final AIMessage."""
    cfg = {"callbacks": callbacks} if callbacks else None
    response = await agent.ainvoke({"messages": prompt}, config=cfg)
    final_msg = next(m for m in reversed(response["messages"]) if isinstance(m, AIMessage))
    return final_msg, response 
import asyncio, json, os, uuid, time, sys
from fastapi import FastAPI, HTTPException, Request
from starlette.responses import JSONResponse
from mcp import StdioServerParameters, stdio_client, ClientSession
from common import SettingsInstance as S
from common.structured_logging import get_logger, SERVICE_NAME
from langchain.prompts import ChatPromptTemplate
from sqlalchemy.ext.asyncio import create_async_engine
from . import db_models
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from common.kafka_utils import event_producer
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
    try:
        await event_producer.close()
    except Exception:
        logger.error("Error stopping Kafka producer", exc_info=True)

app = FastAPI(title="Book-Recommendation-API", lifespan=lifespan)
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
        logger.info("Database tables verified/created successfully")
    except Exception as e:
        logger.error("Failed to create database tables", exc_info=True)
        raise

# --- metrics -----------------------------------------------------------
logger.info("Initializing Kafka producer for metrics")
TOPIC = "api_metrics"

async def push_metric(event: str, extra: dict | None = None):
    """Send metric to Kafka without blocking the caller."""
    # Compose payload once
    payload = {"event": event, "timestamp": time.time(), "request_id": uuid.uuid4().hex}
    if extra:
        payload.update(extra)

    async def _send():
        try:
            await event_producer.publish_event(TOPIC, payload)
            logger.debug("Metric pushed", extra={"event": event, "payload": payload})
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
chat_model = ChatOpenAI(model=S.model_name, api_key=S.openai_api_key, temperature=0.3)
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

@app.post("/recommend")
async def recommend(student_id: str, n: int = 3, query: str = "adventure"):
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
# Script entry-point (mirrors pattern from main_security_agent_server.py)
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
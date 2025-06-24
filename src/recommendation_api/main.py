import asyncio, json, os, uuid, time, sys
from fastapi import FastAPI, HTTPException
from starlette.responses import JSONResponse
from mcp import StdioServerParameters, stdio_client, ClientSession
from common import SettingsInstance as S
from common.structured_logging import get_logger
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

# @asynccontextmanager
# async def get_mcp_session():
#     """Yield an initialized FastMCP ClientSession and ensure it closes cleanly."""
#     logger.debug("Creating MCP session")
#     try:
#         async with stdio_client(server_params) as (reader, writer):
#             logger.debug("MCP subprocess stdio tunnel established")

#             session = ClientSession(reader, writer, read_timeout_seconds=timedelta(seconds=300))

#             logger.debug("Sending MCP initialize request")
#             try:
#                 await session.initialize()
#                 logger.debug("MCP initialize completed")
#             except Exception:
#                 logger.error("MCP initialize failed", exc_info=True)
#                 raise
#             try:
#                 yield session
#             finally:
#                 try:
#                     await session.shutdown()
#                     logger.debug("MCP session shutdown complete")
#                 except Exception:
#                     logger.warning("Error shutting down MCP session", exc_info=True)
#     except Exception:
#         logger.error("Failed to create MCP session", exc_info=True)
#         raise

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
        "query": query
    })
    
    start = time.perf_counter()
    
    try:
        # Get MCP session and build ReAct agent
        logger.debug("Getting MCP session", extra={"request_id": request_id})
        async with stdio_client(server_params) as (read, write):
            logger.info("Server connection established!", extra={"request_id": request_id})
            # Initialize client session for communication
            async with ClientSession(
                read, write, read_timeout_seconds=timedelta(seconds=300)
            ) as session:
                # initialize handshake
                await session.initialize()
                logger.info("Client session initialized", extra={"request_id": request_id})

                lc_tools = await load_mcp_tools(session)

                agent = create_react_agent(chat_model, lc_tools, name="BookRecommenderAgent")
                logger.info(f"Agent {agent.name} created and ready to process requests!", extra={"request_id": request_id})
                
                # TODO: Have this more structured ahead of time, RAG results included, a PrmpmtTemplate, and so on
                user_prompt = (
                    f"Recommend {n} books suitable for a grade-4 student with id {student_id}. "
                    f"Their interest keywords: '{query}'. Provide JSON list with book_id, title, librarian_blurb."
                )

                logger.debug(f"Running ReAct agent {agent.name}", extra={"request_id": request_id})

                cb = MetricCallbackHandler()
                agent_start = time.perf_counter()
                final_message, response = await ask_agent(agent, user_prompt, callbacks=[cb])
                agent_duration = time.perf_counter() - agent_start
                final_message_content = final_message.content

        duration = time.perf_counter() - start
        logger.info("Recommendation generated successfully", extra={
            "request_id": request_id,
            "duration_sec": round(duration, 3),
            "response_len": len(final_message_content) if isinstance(final_message_content, str) else None
        })
        
        # token usage if available (OpenAI returns in metadata)
        usage_meta = getattr(final_message, "usage_metadata", None) or {}

        metric_payload = {
            "request_id": request_id,
            "student_id": student_id,
            "duration_sec": agent_duration,
            "tool_count": len(cb.tools_used),
            "tools": cb.tools_used[:10],  # avoid huge payloads
            "error_count": cb.error_count,
        }
        metric_payload.update(
            {k: v for k, v in usage_meta.items() if k in {"input_tokens", "output_tokens", "total_tokens"}}
        )

        asyncio.create_task(push_metric("agent_run", metric_payload))
        
        asyncio.create_task(push_metric("recommendation_served", {
            "duration_sec": duration,
            "request_id": request_id,
            "student_id": student_id,
            "n": n,
        }))
        
        return final_message_content
        
    except Exception as e:
        duration = time.perf_counter() - start
        logger.error("Recommendation request failed", exc_info=True, extra={
            "request_id": request_id,
            "student_id": student_id,
            "duration_sec": round(duration, 3),
            "error": str(e)
        })
        raise
    finally:
        # Capture total token usage
        usage_metrics = {}
        if "final_message" in locals():
            usage_metadata = final_message.usage_metadata
            usage_metrics = {
                "input_tokens": usage_metadata.get("input_tokens"),
                "output_tokens": usage_metadata.get("output_tokens"),
                "total_tokens": usage_metadata.get("total_tokens"),
                "input_token_details": usage_metadata.get("input_token_details"),
                "output_token_details": usage_metadata.get("output_token_details"),
            }
        logger.debug(f"Usage metrics: {usage_metrics}")

        # Pull tool names out of your ToolMessage calls
        # TODO: If the MetricsCallbackHandler is successful, we can remove this.  If not, we'll fall back to this method
        # tools = []
        # if "response" in locals():
        #     for msg in response["messages"]:
        #         if hasattr(msg, "additional_kwargs") and msg.additional_kwargs.get("tool_calls"):
        #             for call in msg.additional_kwargs["tool_calls"]:
        #                 tools.append(call["function"]["name"])
        #     logger.debug(f"Tools called: {tools}") 

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
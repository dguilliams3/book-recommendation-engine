import asyncio, json, os, uuid, time
from fastapi import FastAPI, HTTPException
from starlette.responses import JSONResponse
from mcp import StdioServerParameters, stdio_client, ClientSession
from common import SettingsInstance as S
from common.structured_logging import get_logger
from langgraph.graph import Graph
from langchain_openai import OpenAI
from langchain.prompts import ChatPromptTemplate
from sqlalchemy.ext.asyncio import create_async_engine
from . import db_models
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
from common.kafka_utils import event_producer
from contextlib import asynccontextmanager
from datetime import timedelta

app = FastAPI(title="Book‑Recommendation‑API")
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

# Pass through current environment so the child process has the same
# DB_URL / OPENAI_API_KEY etc.  FastMCP will merge with its defaults.
server_params = StdioServerParameters(
    command="python",
    args=["-m", "recommendation_api.mcp_book_server"],
    env=dict(os.environ),
)

@asynccontextmanager
async def get_mcp_session():
    """Yield an initialized FastMCP ClientSession and ensure it closes cleanly."""
    logger.debug("Creating MCP session")
    try:
        async with stdio_client(server_params) as (reader, writer):
            session = ClientSession(reader, writer, read_timeout_seconds=timedelta(seconds=300))
            await session.initialize()
            logger.debug("MCP session initialized successfully")
            try:
                yield session
            finally:
                try:
                    await session.shutdown()
                except Exception:
                    logger.warning("Error shutting down MCP session", exc_info=True)
    except Exception:
        logger.error("Failed to create MCP session", exc_info=True)
        raise

# --- LangGraph flow ----------------------------------------------------
logger.info("Initializing LangGraph recommendation flow")
prompt = ChatPromptTemplate.from_template(
    "You are a friendly librarian. Given the following JSON from `search_catalog`, "
    "pick the best {n} books for elementary grade‑4 student {student_id}. "
    "Return JSON list of objects: book_id, title, librarian_blurb.\n\n{catalog_json}"
)
llm = OpenAI(model=S.model_name, api_key=S.openai_api_key, temperature=0.3)

graph = Graph()
graph.add_node("recommend", llm)
graph.set_entry_point("recommend")
langgraph_chain = graph.compile()
logger.info("LangGraph flow initialized successfully")

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
        # Get MCP session and tools
        logger.debug("Getting MCP session", extra={"request_id": request_id})
        async with get_mcp_session() as mcp:
            lc_tools = await load_mcp_tools(mcp)
            # convert list to dict keyed by tool name for convenience
            tools = {t.name: t for t in lc_tools}
            if "search_catalog" not in tools:
                logger.error("MCP tool 'search_catalog' not found", extra={"request_id": request_id})
                raise HTTPException(500, "MCP tool not found")
            
            logger.debug("Searching catalog", extra={"request_id": request_id, "query": query})
            catalog_json = await tools["search_catalog"](keyword=query, k=n*3)
            logger.debug("Catalog search completed", extra={
                "request_id": request_id,
                "results_count": len(catalog_json) if isinstance(catalog_json, list) else 0
            })
        
        # Generate recommendations
        logger.debug("Generating recommendations with LangGraph", extra={"request_id": request_id})
        resp = await langgraph_chain.ainvoke(
            {"n": n, "student_id": student_id, "catalog_json": json.dumps(catalog_json)}
        )
        
        duration = time.perf_counter() - start
        logger.info("Recommendation generated successfully", extra={
            "request_id": request_id,
            "duration_sec": round(duration, 3),
            "recommendations_count": len(resp) if isinstance(resp, list) else 0
        })
        
        # Push metrics
        await push_metric("recommendation_served", {
            "duration_sec": duration,
            "request_id": request_id,
            "student_id": student_id,
            "n": n
        })
        
        return resp
        
    except Exception as e:
        duration = time.perf_counter() - start
        logger.error("Recommendation request failed", exc_info=True, extra={
            "request_id": request_id,
            "student_id": student_id,
            "duration_sec": round(duration, 3),
            "error": str(e)
        })
        raise

@app.on_event("startup")
async def startup_event():
    logger.info("Starting up recommendation API")
    try:
        # Ensure database tables exist
        await _ensure_tables()
        
    except Exception as e:
        logger.error("Failed to start up recommendation API", exc_info=True)
        raise

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down recommendation API")
    try:
        await event_producer.close()
    except Exception:
        logger.error("Error stopping Kafka producer", exc_info=True) 
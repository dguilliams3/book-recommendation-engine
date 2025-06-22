import asyncio, json, os, uuid, time
from fastapi import FastAPI, HTTPException
from starlette.responses import JSONResponse
from aiokafka import AIOKafkaProducer
from fastmcp import StdioServerParameters, stdio_client, ClientSession
from common import SettingsInstance as S
from common.logging import get_logger
from langgraph.graph import Graph
from langchain_openai import OpenAI
from langchain.prompts import ChatPromptTemplate
from sqlalchemy.ext.asyncio import create_async_engine
from . import db_models
from langchain_mcp_adapters.tools import load_mcp_tools
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

app = FastAPI(title="Book‑Recommendation‑API")
logger = get_logger(__name__)

# --- auto-DDL setup ----------------------------------------------------
logger.info("Initializing database connection")
engine = create_async_engine(S.db_url, echo=False)

async def _ensure_tables():
    logger.info("Ensuring database tables exist")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(db_models.Base.metadata.create_all)
        logger.info("Database tables verified/created successfully")
    except Exception as e:
        logger.error("Failed to create database tables", exc_info=True)
        raise

asyncio.get_event_loop().run_until_complete(_ensure_tables())

# --- metrics -----------------------------------------------------------
logger.info("Initializing Kafka producer for metrics")
producer = AIOKafkaProducer(bootstrap_servers=S.kafka_bootstrap)
asyncio.get_event_loop().run_until_complete(producer.start())
TOPIC = "api_metrics"
logger.info("Kafka producer started successfully")

async def push_metric(event: str, extra: dict | None = None):
    try:
        payload = {"event": event, "timestamp": time.time(), "request_id": uuid.uuid4().hex}
        if extra:
            payload.update(extra)
        await producer.send_and_wait(TOPIC, json.dumps(payload).encode())
        logger.debug("Metric pushed", extra={"event": event, "payload": payload})
    except Exception as e:
        logger.error("Failed to push metric", exc_info=True, extra={"event": event})

# --- FastMCP lifecycle -------------------------------------------------
logger.info("Setting up FastMCP server parameters")
server_params = StdioServerParameters(
    command="python",
    args=["-m", "recommendation_api.mcp_book_server"],
)

async def get_mcp_session():
    logger.debug("Creating MCP session")
    try:
        reader, writer = await stdio_client(server_params)
        session = ClientSession(reader, writer)
        await session.initialize()
        logger.debug("MCP session initialized successfully")
        return session
    except Exception as e:
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
    buffer_size = producer._buffer_size
    logger.debug("Producer buffer size", extra={"buffer_size": buffer_size})
    return JSONResponse({"producer_buffer": buffer_size})

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
        async with await get_mcp_session() as mcp:
            tools = await mcp.load_tools()
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

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down recommendation API")
    try:
        await producer.stop()
        logger.info("Kafka producer stopped successfully")
    except Exception as e:
        logger.error("Error stopping Kafka producer", exc_info=True) 
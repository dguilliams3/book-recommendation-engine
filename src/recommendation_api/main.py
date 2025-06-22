import asyncio, json, os, uuid, time, logging
from fastapi import FastAPI, HTTPException
from starlette.responses import JSONResponse
from aiokafka import AIOKafkaProducer
from fastmcp import StdioServerParameters, stdio_client, ClientSession
from common import SettingsInstance as S
from langgraph.graph import Graph
from langchain_openai import OpenAI
from langchain.prompts import ChatPromptTemplate
from sqlalchemy.ext.asyncio import create_async_engine
from . import db_models

app = FastAPI(title="Book‑Recommendation‑API")
logger = logging.getLogger("recommendation_api")
logging.basicConfig(level=logging.INFO)

# --- auto-DDL setup ----------------------------------------------------
engine = create_async_engine(S.db_url, echo=False)

async def _ensure_tables():
    async with engine.begin() as conn:
        await conn.run_sync(db_models.Base.metadata.create_all)

asyncio.get_event_loop().run_until_complete(_ensure_tables())

# --- metrics -----------------------------------------------------------
producer = AIOKafkaProducer(bootstrap_servers=S.kafka_bootstrap)
asyncio.get_event_loop().run_until_complete(producer.start())
TOPIC = "api_metrics"

async def push_metric(event: str, extra: dict | None = None):
    payload = {"event": event, "timestamp": time.time(), "request_id": uuid.uuid4().hex}
    if extra:
        payload.update(extra)
    await producer.send_and_wait(TOPIC, json.dumps(payload).encode())

# --- FastMCP lifecycle -------------------------------------------------
server_params = StdioServerParameters(
    command="python",
    args=["-m", "recommendation_api.mcp_book_server"],
)

async def get_mcp_session():
    reader, writer = await stdio_client(server_params)
    session = ClientSession(reader, writer)
    await session.initialize()
    return session

# --- LangGraph flow ----------------------------------------------------
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

# --- endpoints ---------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/metrics")
async def metrics():
    # simple gauge example
    return JSONResponse({"producer_buffer": producer._buffer_size})

@app.post("/recommend")
async def recommend(student_id: str, n: int = 3, query: str = "adventure"):
    start = time.perf_counter()
    async with await get_mcp_session() as mcp:
        tools = await mcp.load_tools()
        if "search_catalog" not in tools:
            raise HTTPException(500, "MCP tool not found")
        catalog_json = await tools["search_catalog"](keyword=query, k=n*3)
    resp = await langgraph_chain.ainvoke(
        {"n": n, "student_id": student_id, "catalog_json": json.dumps(catalog_json)}
    )
    duration = time.perf_counter() - start
    await push_metric("recommendation_served", {"duration_sec": duration})
    return resp

@app.on_event("shutdown")
async def shutdown_event():
    await producer.stop() 
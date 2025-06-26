import asyncio, json, os
from pathlib import Path
from filelock import FileLock
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from aiokafka import AIOKafkaConsumer
from common.settings import settings as S
from common.kafka_utils import KafkaEventConsumer, event_producer
from common.events import BOOK_EVENTS_TOPIC, BookAddedEvent, BookUpdatedEvent
from common.structured_logging import get_logger

logger = get_logger(__name__)

VECTOR_DIR = Path("data/vector_store")
VECTOR_DIR.mkdir(parents=True, exist_ok=True)
LOCK_FILE = VECTOR_DIR / "index.lock"

embeddings = OpenAIEmbeddings(api_key=S.openai_api_key, model=S.embedding_model)

async def ensure_store():
    if (VECTOR_DIR / "index.faiss").exists():
        return FAISS.load_local(VECTOR_DIR, embeddings, allow_dangerous_deserialization=True)
    return FAISS.from_texts(["dummy"], embeddings, metadatas=[{"book_id": "dummy"}])

async def handle_book_event(evt: dict):
    etype = evt.get("event_type")
    if etype not in {"books_added", "book_updated"}:
        return
    logger.info("Processing book event", extra=evt)
    ids = evt.get("book_ids") or [evt.get("book_id")]
    if not ids:
        return
    # Fetch book data from DB lazily to avoid large event payloads
    import asyncpg
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)
    rows = await conn.fetch("SELECT book_id,title,description FROM catalog WHERE book_id = ANY($1::text[])", ids)
    await conn.close()
    if not rows:
        logger.warning("No matching books found", extra={"ids": ids})
        return
    texts = [f"{r['title']}. {r['description'] or ''}" for r in rows]
    metadatas = [{"book_id": r["book_id"]} for r in rows]
    # Embed and add to FAISS with lock
    with FileLock(str(LOCK_FILE)):
        store = await ensure_store()
        store.add_texts(texts, metadatas=metadatas)
        store.save_local(VECTOR_DIR)
    logger.info("Book vectors updated", extra={"added": len(rows), "index_size": store.index.ntotal})

async def main():
    consumer = KafkaEventConsumer(BOOK_EVENTS_TOPIC, "book_vector_worker")
    await consumer.start(handle_book_event)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass 
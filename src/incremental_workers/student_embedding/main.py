import asyncio, json

import asyncpg
from langchain_openai import OpenAIEmbeddings
from common.settings import settings as S
from common.kafka_utils import KafkaEventConsumer, event_producer
from common.events import (
    STUDENT_PROFILE_TOPIC,
    STUDENT_EMBEDDING_TOPIC,
    StudentEmbeddingChangedEvent,
)
from common.structured_logging import get_logger

logger = get_logger(__name__)

EMB = OpenAIEmbeddings(api_key=S.openai_api_key, model=S.embedding_model)

async def fetch_profile(student_id: str) -> str:
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)
    row = await conn.fetchrow(
        "SELECT histogram FROM student_profile_cache WHERE student_id=$1", student_id
    )
    await conn.close()
    if not row:
        return ""
    hist = row["histogram"] or {}
    # Convert histogram into pseudo document: token repeated count times
    parts = []
    for token, cnt in hist.items():
        parts.extend([token] * int(cnt))
    return " ".join(parts) or "no_history"

async def upsert_embedding(student_id: str, vec):
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)
    pg_vec = "[" + ",".join(f"{x:.6f}" for x in vec) + "]"
    await conn.execute(
        """INSERT INTO student_embeddings VALUES($1,$2,$3)
           ON CONFLICT(student_id) DO UPDATE SET vec=$2, last_event=$3""",
        student_id,
        pg_vec,
        None,
    )
    await conn.close()

async def handle_profile_event(evt: dict):
    student_id = evt.get("student_id")
    if not student_id:
        return
    logger.info("Embedding profile", extra={"student_id": student_id})
    doc = await fetch_profile(student_id)
    vec = EMB.embed_query(doc)
    await upsert_embedding(student_id, vec)
    emb_evt = StudentEmbeddingChangedEvent(student_id=student_id)
    await event_producer.publish_event(STUDENT_EMBEDDING_TOPIC, emb_evt.dict())
    logger.info("Student embedding updated", extra={"student_id": student_id})

async def main():
    consumer = KafkaEventConsumer(STUDENT_PROFILE_TOPIC, "student_embedding_worker")
    await consumer.start(handle_profile_event)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass 
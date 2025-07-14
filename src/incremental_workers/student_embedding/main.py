import asyncio, json
import os
import uuid

import asyncpg
from langchain_openai import OpenAIEmbeddings
from common.settings import settings as S
from common.kafka_utils import KafkaEventConsumer, publish_event
from common.events import (
    STUDENT_PROFILE_TOPIC,
    STUDENT_EMBEDDING_TOPIC,
    StudentEmbeddingChangedEvent,
)
from common.structured_logging import get_logger
import numpy as np

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
    hist_data = row["histogram"]

    # Parse JSON if it's a string, otherwise use as-is
    if isinstance(hist_data, str):
        try:
            hist = json.loads(hist_data) if hist_data else {}
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse histogram JSON for student {student_id}")
            hist = {}
    else:
        hist = hist_data or {}

    # Convert histogram into pseudo document: token repeated count times
    parts = []
    for token, cnt in hist.items():
        parts.extend([token] * int(cnt))
    return " ".join(parts) or "no_history"


async def compute_embedding(student_id: str) -> np.ndarray:
    """Compute embedding vector for a student based on their profile."""
    logger.debug("Computing embedding for student", extra={"student_id": student_id})
    doc = await fetch_profile(student_id)
    vec = EMB.embed_query(doc)
    return np.array(vec)


async def cache_embedding(student_id: str, vec: np.ndarray, event_id: str = None):
    """Store computed embedding in the `student_embeddings` table."""
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)

    # Generate event ID if not provided
    if event_id is None:
        event_id = str(uuid.uuid4())

    pg_vec = "[" + ",".join(f"{x:.6f}" for x in vec) + "]"
    await conn.execute(
        """INSERT INTO student_embeddings VALUES($1,$2,$3)
           ON CONFLICT(student_id) DO UPDATE SET vec=$2, last_event=$3""",
        student_id,
        pg_vec,
        event_id,
    )
    await conn.close()


async def handle_profile_change(evt: dict):
    """Kafka handler for *student_profile_changed* events."""
    student_id = evt.get("student_id")
    if not student_id:
        return

    # Generate unique event ID for traceability
    event_id = str(uuid.uuid4())

    logger.info(
        "Handling profile change event",
        extra={"student_id": student_id, "event_id": event_id},
    )
    emb = await compute_embedding(student_id)
    await cache_embedding(student_id, emb, event_id)
    logger.info(
        "Student embedding cached",
        extra={"student_id": student_id, "event_id": event_id},
    )


async def main():
    consumer = KafkaEventConsumer(STUDENT_PROFILE_TOPIC, "student_embedding_worker")
    await consumer.start(handle_profile_change)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

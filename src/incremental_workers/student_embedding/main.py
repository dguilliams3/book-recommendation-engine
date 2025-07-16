import asyncio, json
import os
import uuid
import hashlib
from typing import Tuple, Optional

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


def compute_profile_hash(profile_text: str) -> str:
    """Compute a hash of the student profile text to detect changes"""
    return hashlib.sha256(profile_text.encode()).hexdigest()


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


async def check_existing_embedding(student_id: str, profile_hash: str) -> Tuple[bool, Optional[str]]:
    """Check if student embedding exists and if profile has changed"""
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)
    
    try:
        result = await conn.fetchrow(
            "SELECT student_id, content_hash FROM student_embeddings WHERE student_id=$1",
            student_id
        )
        
        if not result:
            return False, None  # Embedding doesn't exist
        
        existing_hash = result["content_hash"]
        if existing_hash == profile_hash:
            return True, existing_hash  # Embedding exists and profile is identical
        
        return True, existing_hash  # Embedding exists but profile has changed
        
    finally:
        await conn.close()


async def compute_embedding(student_id: str) -> np.ndarray:
    """Compute embedding vector for a student based on their profile."""
    logger.debug("Computing embedding for student", extra={"student_id": student_id})
    doc = await fetch_profile(student_id)
    vec = EMB.embed_query(doc)
    return np.array(vec)


async def cache_embedding(student_id: str, vec: np.ndarray, profile_hash: str, event_id: str = None):
    """Store computed embedding in the `student_embeddings` table with content hash."""
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)

    # Generate event ID if not provided
    if event_id is None:
        event_id = str(uuid.uuid4())

    pg_vec = "[" + ",".join(f"{x:.6f}" for x in vec) + "]"
    await conn.execute(
        """INSERT INTO student_embeddings VALUES($1,$2,$3,$4)
           ON CONFLICT(student_id) DO UPDATE SET vec=$2, last_event=$3, content_hash=$4""",
        student_id,
        pg_vec,
        event_id,
        profile_hash,
    )
    await conn.close()


async def handle_profile_change(evt: dict):
    """Kafka handler for *student_profile_changed* events with idempotency checks."""
    student_id = evt.get("student_id")
    if not student_id:
        return

    # Generate unique event ID for traceability
    event_id = str(uuid.uuid4())

    logger.info(
        "Handling profile change event",
        extra={"student_id": student_id, "event_id": event_id},
    )
    
    # Fetch current profile
    profile_text = await fetch_profile(student_id)
    if not profile_text:
        logger.warning(f"No profile found for student {student_id}, skipping embedding generation")
        return
    
    # Compute profile hash for idempotency check
    profile_hash = compute_profile_hash(profile_text)
    
    # Check if embedding already exists and profile hasn't changed
    exists, existing_hash = await check_existing_embedding(student_id, profile_hash)
    
    if exists and existing_hash == profile_hash:
        # Embedding exists and profile is identical - skip processing
        logger.info(
            f"Student {student_id} profile unchanged, skipping embedding generation",
            extra={"student_id": student_id, "event_id": event_id}
        )
        return
    
    # Profile has changed or embedding doesn't exist - generate new embedding
    emb = await compute_embedding(student_id)
    await cache_embedding(student_id, emb, profile_hash, event_id)
    
    logger.info(
        "Student embedding cached",
        extra={
            "student_id": student_id, 
            "event_id": event_id,
            "profile_changed": exists and existing_hash != profile_hash,
            "new_embedding": not exists
        },
    )


async def main():
    consumer = KafkaEventConsumer(STUDENT_PROFILE_TOPIC, "student_embedding_worker")
    await consumer.start(handle_profile_change)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

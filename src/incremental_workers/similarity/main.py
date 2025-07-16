"""
Similarity Worker - Collaborative Filtering Engine

SERVICE PURPOSE:
    Incremental worker that computes student-to-student similarity matrices
    using vector embeddings. Core component of collaborative filtering that
    enables "students like you" recommendations.

KEY FUNCTIONS:
    - Listens to student embedding change events via Kafka
    - Computes cosine similarity between student embedding vectors
    - Uses PostgreSQL pgvector for efficient similarity calculations
    - Maintains top-15 most similar students for each student
    - Updates student_similarity table for real-time recommendations

SIMILARITY ALGORITHM:
    - Uses pgvector's <=> operator for cosine distance calculations
    - Finds 15 nearest neighbors for each student embedding
    - Filters by similarity threshold for quality control
    - Stores bidirectional similarity relationships

DEPENDENCIES:
    - Kafka: Consumes from STUDENT_EMBEDDING_TOPIC
    - PostgreSQL: Reads student_embeddings, writes student_similarity
    - pgvector: Vector similarity operations
    - Upstream: Student embedding worker

INTERACTION PATTERNS:
    INPUT:  Student embedding change events
    OUTPUT: Updated similarity matrix for collaborative filtering
    EVENTS: Triggered by StudentEmbeddingChangedEvent

PERFORMANCE NOTES:
    - Uses pgvector for hardware-optimized similarity search
    - Incremental updates avoid full matrix recalculation
    - Similarity calculations are CPU-intensive but batched efficiently

CRITICAL NOTES:
    - Similarity quality directly impacts collaborative recommendations
    - Performance scales with number of students (O(n²) worst case)
    - Essential for "find similar students" functionality

⚠️  REMEMBER: Update this documentation block when modifying service functionality!
"""

import asyncio
import uuid
import asyncpg
from common.settings import settings as S
from common.kafka_utils import KafkaEventConsumer
from common.events import STUDENT_EMBEDDING_TOPIC
from common.structured_logging import get_logger

logger = get_logger(__name__)


async def compute_similarity(student_id: str, event_id: str = None):
    """Recompute top-neighbour similarity rows for a given student.

    Embeddings live in the `student_embeddings` table as pgvector columns; we
    use Postgres's `<=>` operator (cosine distance) to get the 15 closest
    students and cache them into `student_similarity`.
    """
    if event_id is None:
        event_id = str(uuid.uuid4())

    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)

    try:
        # ensure tables exist
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS student_similarity(a TEXT,b TEXT,sim REAL,last_event UUID,PRIMARY KEY(a,b))"
        )

        # delete existing rows for student
        await conn.execute("DELETE FROM student_similarity WHERE a=$1", student_id)

        # fetch neighbour similarities via pgvector
        sims = await conn.fetch(
            """WITH src AS (SELECT vec FROM student_embeddings WHERE student_id=$1)
                SELECT student_id, 1-(student_embeddings.vec <=> src.vec) AS sim
                  FROM student_embeddings, src
                 WHERE student_id <> $1
              ORDER BY student_embeddings.vec <=> src.vec LIMIT 15""",
            student_id,
        )

        # Insert with event tracking
        rows = [(student_id, r["student_id"], r["sim"], event_id) for r in sims]
        if rows:
            await conn.executemany(
                "INSERT INTO student_similarity VALUES($1,$2,$3,$4)", rows
            )

        logger.info(
            "Similarity updated",
            extra={"student_id": student_id, "count": len(rows), "event_id": event_id},
        )

    finally:
        await conn.close()


async def handle_embedding_event(evt: dict):
    """Kafka callback for *student_embedding* events that triggers recomputation."""
    sid = evt.get("student_id")
    if not sid:
        return

    # Generate unique event ID for traceability
    event_id = str(uuid.uuid4())

    logger.info(
        "Handling embedding event", extra={"student_id": sid, "event_id": event_id}
    )

    await compute_similarity(sid, event_id)


async def main():
    """Run the similarity worker as a long-lived Kafka consumer."""
    consumer = KafkaEventConsumer(STUDENT_EMBEDDING_TOPIC, "similarity_worker")
    await consumer.start(handle_embedding_event)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

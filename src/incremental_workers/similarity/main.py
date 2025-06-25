import asyncio
import asyncpg
from common.settings import settings as S
from common.kafka_utils import KafkaEventConsumer
from common.events import STUDENT_EMBEDDING_TOPIC
from common.structured_logging import get_logger

logger = get_logger(__name__)

async def compute_similarity(student_id: str):
    """Recompute top-neighbour similarity rows for a given student.

    Embeddings live in the `student_embeddings` table as pgvector columns; we
    use Postgres's `<=>` operator (cosine distance) to get the 15 closest
    students and cache them into `student_similarity`.
    """
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)
    # ensure tables exist
    await conn.execute("CREATE TABLE IF NOT EXISTS student_similarity(a TEXT,b TEXT,sim REAL,PRIMARY KEY(a,b))")
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
    rows = [(student_id, r["student_id"], r["sim"]) for r in sims]
    if rows:
        await conn.executemany("INSERT INTO student_similarity VALUES($1,$2,$3)", rows)
    await conn.close()
    logger.info("Similarity updated", extra={"student_id": student_id, "count": len(rows)})

async def handle_embedding_event(evt: dict):
    """Kafka callback for *student_embedding* events that triggers recomputation."""
    sid = evt.get("student_id")
    if not sid:
        return
    await compute_similarity(sid)

async def main():
    """Run the similarity worker as a long-lived Kafka consumer."""
    consumer = KafkaEventConsumer(STUDENT_EMBEDDING_TOPIC, "similarity_worker")
    await consumer.start(handle_embedding_event)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass 
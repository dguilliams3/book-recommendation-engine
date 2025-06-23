import asyncio, json
from collections import Counter

import asyncpg
from common.settings import settings as S
from common.kafka_utils import KafkaEventConsumer, event_producer
from common.events import (
    CHECKOUT_EVENTS_TOPIC,
    STUDENT_PROFILE_TOPIC,
    StudentProfileChangedEvent,
)
from common.structured_logging import get_logger

logger = get_logger(__name__)

async def build_profile(student_id: str) -> dict:
    """Aggregate checkout history into a histogram of difficulty bands."""
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)
    rows = await conn.fetch(
        """SELECT difficulty_band FROM checkout JOIN catalog USING(book_id)
           WHERE student_id=$1""",
        student_id,
    )
    await conn.close()
    bands = [r["difficulty_band"] for r in rows if r["difficulty_band"]]
    hist = Counter(bands)
    return dict(hist)

async def update_profile_cache(student_id: str, histogram: dict):
    pg_url = str(S.db_url).replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(pg_url)
    await conn.execute(
        """INSERT INTO student_profile_cache VALUES($1,$2,$3)
           ON CONFLICT(student_id) DO UPDATE SET histogram=$2, last_event=$3""",
        student_id,
        json.dumps(histogram),
        None,
    )
    await conn.close()

async def handle_checkout(evt: dict):
    student_id = evt.get("student_id")
    if not student_id:
        return
    logger.info("Handling checkout event", extra={"student_id": student_id})
    hist = await build_profile(student_id)
    await update_profile_cache(student_id, hist)
    # emit event
    prof_evt = StudentProfileChangedEvent(student_id=student_id)
    await event_producer.publish_event(STUDENT_PROFILE_TOPIC, prof_evt.dict())
    logger.info("Student profile updated", extra={"student_id": student_id})

async def main():
    consumer = KafkaEventConsumer(CHECKOUT_EVENTS_TOPIC, "student_profile_worker")
    await consumer.start(handle_checkout)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass 
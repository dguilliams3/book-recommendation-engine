"""
1. Ensure schema exists (executes 00_init_schema.sql once).
2. Validate & coerce CSV rows → Pydantic models.
3. Upsert rows into Postgres and build / update FAISS embeddings.
"""

import asyncio, sys, time
from pathlib import Path
from typing import Iterable

# Add src to Python path so we can find the common module when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

from common.settings import settings as S
from common import models
from common.structured_logging import get_logger
from common.kafka_utils import publish_event
from common.events import (BookAddedEvent, BOOK_EVENTS_TOPIC, StudentAddedEvent,
                            StudentUpdatedEvent, CheckoutAddedEvent, STUDENT_EVENTS_TOPIC,
                            CHECKOUT_EVENTS_TOPIC)
from .pipeline import run_ingestion
from common.metrics import JOB_RUNS_TOTAL, JOB_DURATION_SECONDS

logger = get_logger(__name__)
TOPIC = "ingestion_metrics"

async def _send(payload):
    """Send metrics payload to Kafka."""
    try:
        await publish_event(TOPIC, payload)
        logger.debug("Metric sent successfully", extra={"payload": payload})
    except Exception:
        logger.error("Metric send failed", exc_info=True, extra={"payload": payload})

def ingest():
    """Run the ingestion pipeline and emit Prometheus job metrics."""
    job_name = "ingestion_service"
    start = time.perf_counter()

    JOB_RUNS_TOTAL.labels(job=job_name, status="started").inc()
    logger.info("Starting ingestion service")

    try:
        asyncio.run(run_ingestion())
        JOB_RUNS_TOTAL.labels(job=job_name, status="success").inc()
        JOB_DURATION_SECONDS.labels(job=job_name, status="success").observe(
            time.perf_counter() - start
        )
    except Exception:
        JOB_RUNS_TOTAL.labels(job=job_name, status="failure").inc()
        JOB_DURATION_SECONDS.labels(job=job_name, status="failure").observe(
            time.perf_counter() - start
        )
        raise

if __name__ == "__main__":
    try:
        ingest()
        logger.info("Ingestion completed successfully")
    except Exception:
        logger.exception("Ingestion service failed") 
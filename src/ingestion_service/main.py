"""
1. Ensure schema exists (executes 00_init_schema.sql once).
2. Validate & coerce CSV rows → Pydantic models.
3. Upsert rows into Postgres and build / update FAISS embeddings.
"""

import asyncio, sys
from pathlib import Path
from typing import Iterable

# Add src to Python path so we can find the common module when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

from common.settings import SettingsInstance as S
from common import models
from common.structured_logging import get_logger
from common.kafka_utils import event_producer
from common.events import (BookAddedEvent, BOOK_EVENTS_TOPIC, StudentAddedEvent,
                            StudentUpdatedEvent, CheckoutAddedEvent, STUDENT_EVENTS_TOPIC,
                            CHECKOUT_EVENTS_TOPIC)
from .pipeline import run_ingestion

logger = get_logger(__name__)
TOPIC = "ingestion_metrics"

async def _publish(payload):  # fire-and-forget
    """Publish a metric payload to Kafka without awaiting acknowledgements.

    This helper is used for emitting one-off ingestion metrics such as total
    runtime or row counts.  It intentionally *does not* raise if Kafka is
    unavailable so that the ingestion process can still succeed offline.

    Parameters
    ----------
    payload : dict
        Arbitrary JSON-serialisable dictionary to send to the `ingestion_metrics`
        topic.
    """
    try:
        await event_producer.publish_event(TOPIC, payload)
        logger.debug("Metric published", extra={"payload": payload})
    except Exception as e:
        logger.error("Failed to publish metric", exc_info=True, extra={"payload": payload})

def ingest():
    """Thin wrapper that delegates to :func:`pipeline.run_ingestion`."""
    logger.info("Starting ingestion service")
    asyncio.run(run_ingestion())

if __name__ == "__main__":
    try:
        ingest()
        logger.info("Ingestion completed successfully")
    except Exception:
        logger.exception("Ingestion service failed") 
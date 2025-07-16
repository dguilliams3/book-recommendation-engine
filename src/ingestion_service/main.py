"""
Ingestion Service - Data Processing Pipeline

SERVICE PURPOSE:
    Comprehensive data ingestion pipeline that processes CSV files and OpenLibrary
    data into the recommendation system. Handles schema initialization, data
    validation, database operations, and vector index management.

KEY FUNCTIONS:
    - Schema initialization (executes 00_init_schema.sql once)
    - CSV data validation and coercion using Pydantic models
    - PostgreSQL upsert operations for books, students, checkout history
    - FAISS vector index building and updates for semantic search
    - OpenLibrary integration for external book metadata
    - Event publishing for real-time pipeline triggers

SUPPORTED DATA SOURCES:
    - catalog_sample.csv: Book catalog with metadata
    - students_sample.csv: Student profiles and reading levels
    - checkouts_sample.csv: Historical checkout data
    - OpenLibrary API: External book metadata enrichment

DEPENDENCIES:
    - PostgreSQL: Primary data storage with vector extensions
    - FAISS: Vector similarity search index
    - OpenAI: Text embeddings for semantic search
    - Kafka: Event publishing for downstream services
    - File System: CSV file processing

INTERACTION PATTERNS:
    INPUT:  CSV files and OpenLibrary data
    OUTPUT: Populated database + updated vector indexes
    EVENTS: BookAddedEvent, StudentAddedEvent, CheckoutAddedEvent

OPERATIONAL NOTES:
    - Designed for batch processing of large datasets
    - Idempotent operations for safe re-processing
    - Comprehensive error handling and logging
    - Critical for system bootstrap and data updates

⚠️  REMEMBER: Update this documentation block when modifying service functionality!
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
from common.events import (
    BookAddedEvent,
    BOOK_EVENTS_TOPIC,
    StudentAddedEvent,
    StudentUpdatedEvent,
    CheckoutAddedEvent,
    STUDENT_EVENTS_TOPIC,
    CHECKOUT_EVENTS_TOPIC,
)
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

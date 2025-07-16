"""
Log Consumer Service - Centralized Logging Infrastructure

SERVICE PURPOSE:
    Kafka consumer that aggregates structured logs from all microservices
    into centralized JSONL files for monitoring, debugging, and analytics.
    Essential for production observability and troubleshooting.

KEY FUNCTIONS:
    - Consumes log messages from Kafka 'service_logs' topic
    - Writes structured JSON logs to logs/service_logs.jsonl
    - Provides centralized log aggregation for distributed system
    - Enables correlation of events across multiple services

DEPENDENCIES:
    - Kafka: Consumes from 'service_logs' topic
    - File System: Writes to logs/ directory
    - All Services: Receives logs from every microservice

INTERACTION PATTERNS:
    INPUT:  Structured log events from all system components
    OUTPUT: Centralized JSONL log files for analysis
    EVENTS: Pure consumer - no event publishing

OPERATIONAL NOTES:
    - Critical for production monitoring and debugging
    - Log files can grow large - implement rotation in production
    - Failure here doesn't break core functionality but impacts observability
    - Should have dedicated monitoring to ensure log collection continuity

MONITORING:
    - Monitor log file growth and rotation
    - Track consumer lag to ensure real-time log processing
    - Alert on consumer failures to maintain observability

⚠️  REMEMBER: Update this documentation block when modifying service functionality!
"""

import asyncio, json
from pathlib import Path

from common.structured_logging import get_logger
from common.kafka_utils import KafkaEventConsumer
from common.settings import settings

logger = get_logger(__name__)

LOG_FILE = Path("logs/service_logs.jsonl")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)


async def _handle_log(msg: dict):
    """Write one log message (already deserialized) to the jsonl file."""
    line = json.dumps(msg, ensure_ascii=False)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


async def consume():
    logger.info("Starting log consumer service")
    logger.info(
        "Connecting to Kafka", extra={"bootstrap_servers": settings.kafka_bootstrap}
    )

    consumer = KafkaEventConsumer("service_logs", group_id="log_consumer")

    try:
        await consumer.start(_handle_log)
    except Exception:
        logger.error("Log consumer failed", exc_info=True)
        raise


if __name__ == "__main__":
    logger.info("Starting log consumer service")
    try:
        asyncio.run(consume())
    except KeyboardInterrupt:
        logger.info("Log consumer interrupted by user")
    except Exception:
        logger.error("Log consumer failed", exc_info=True)
        raise

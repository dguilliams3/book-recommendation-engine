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

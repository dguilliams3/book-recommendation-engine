"""
Metrics Consumer - System Monitoring and Analytics

SERVICE PURPOSE:
    Kafka consumer that aggregates system metrics from all services for
    monitoring, alerting, and performance analysis. Essential for production
    observability and system health tracking.

KEY FUNCTIONS:
    - Consumes metrics events from Kafka 'ingestion_metrics' topic
    - Stores metrics in Redis for real-time dashboard queries
    - Processes job completion metrics, performance counters, and system health
    - Enables Prometheus-style metrics collection and aggregation
    - Supports distributed system monitoring and alerting

METRICS PROCESSING:
    - Job execution metrics (duration, success/failure rates)
    - System performance counters (request latency, throughput)
    - Error rates and exception tracking
    - Resource utilization patterns
    - User interaction analytics

DEPENDENCIES:
    - Kafka: Consumes from 'ingestion_metrics' topic
    - Redis: Metrics storage and caching for dashboards
    - All Services: Receives metrics from entire system

INTERACTION PATTERNS:
    INPUT:  Metrics events from all system components
    OUTPUT: Aggregated metrics in Redis for monitoring dashboards
    EVENTS: Pure consumer - no event publishing

MONITORING CAPABILITIES:
    - Real-time system health monitoring
    - Performance trend analysis
    - Error rate tracking and alerting
    - Resource utilization monitoring
    - User behavior analytics

OPERATIONAL NOTES:
    - Critical for production system monitoring
    - Redis provides fast access for dashboard queries
    - Graceful degradation if Redis is unavailable
    - High-throughput processing for metrics volume

⚠️  REMEMBER: Update this documentation block when modifying service functionality!
"""

import asyncio, json
from common.kafka_utils import KafkaEventConsumer
from common.settings import settings as S
from common.structured_logging import get_logger
from common.redis_utils import get_redis_client

logger = get_logger(__name__)


async def consume():
    logger.info("Starting metrics consumer")
    logger.info("Connecting to Kafka", extra={"bootstrap_servers": S.kafka_bootstrap})

    # Initialize Redis client for metrics storage
    redis_client = None
    try:
        redis_client = get_redis_client()
        logger.info("Redis client initialized for metrics storage")
    except Exception as e:
        logger.warning(f"Redis not available for metrics storage: {e}")

    consumer = KafkaEventConsumer("ingestion_metrics", group_id="metrics_logger")

    try:
        logger.info("Starting Kafka consumer")
        message_count = 0

        async def handle(message: dict):
            nonlocal message_count
            message_count += 1
            try:
                logger.info(
                    "Received metric message",
                    extra={
                        "topic": message.get("event"),
                        "message_count": message_count,
                        "value": message,
                    },
                )

                # Store in Redis for Streamlit access
                if redis_client:
                    try:
                        event_type = message.get("event", "unknown")
                        redis_key = f"metrics:ingestion:recent"

                        # Store as JSON string in Redis list (keep last 20 metrics)
                        await redis_client.lpush(redis_key, json.dumps(message))
                        await redis_client.ltrim(redis_key, 0, 19)  # Keep only last 20

                        logger.debug(
                            "Stored metric in Redis",
                            extra={"key": redis_key, "event": event_type},
                        )
                    except Exception as redis_error:
                        logger.warning(
                            f"Failed to store metric in Redis: {redis_error}"
                        )

                # Additional structured debug data
                logger.debug("Metric fields", extra=message)

            except Exception:
                logger.error("Failed to process metric message", exc_info=True)

        await consumer.start(handle)
        logger.info("Kafka consumer started successfully")

    except Exception as e:
        logger.error("Kafka consumer error", exc_info=True)
        raise
    finally:
        logger.info("Stopping Kafka consumer")
        await consumer.stop()
        logger.info("Kafka consumer stopped successfully")


if __name__ == "__main__":
    logger.info("Starting metrics consumer service")
    try:
        asyncio.run(consume())
    except KeyboardInterrupt:
        logger.info("Metrics consumer interrupted by user")
    except Exception as e:
        logger.error("Metrics consumer failed", exc_info=True)
        raise

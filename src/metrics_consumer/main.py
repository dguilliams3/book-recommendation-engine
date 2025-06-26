import asyncio, json
from common.kafka_utils import KafkaEventConsumer
from common.settings import settings as S
from common.structured_logging import get_logger

logger = get_logger(__name__)

async def consume():
    logger.info("Starting metrics consumer")
    logger.info("Connecting to Kafka", extra={"bootstrap_servers": S.kafka_bootstrap})
    
    consumer = KafkaEventConsumer("ingestion_metrics", group_id="metrics_logger")
    
    try:
        logger.info("Starting Kafka consumer")
        message_count = 0
        async def handle(message: dict):
            nonlocal message_count
            message_count += 1
            try:
                logger.info("Received metric message", extra={
                    "topic": message.get("event"),
                    "message_count": message_count,
                    "value": message,
                })
                
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
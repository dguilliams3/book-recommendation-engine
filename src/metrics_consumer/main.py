import asyncio, json
from aiokafka import AIOKafkaConsumer
from common import SettingsInstance as S
from common.structured_logging import get_logger

logger = get_logger(__name__)

async def consume():
    logger.info("Starting metrics consumer")
    logger.info("Connecting to Kafka", extra={"bootstrap_servers": S.kafka_bootstrap})
    
    consumer = AIOKafkaConsumer(
        "ingestion_metrics", "api_metrics",
        bootstrap_servers=S.kafka_bootstrap,
        group_id="metrics_logger",
        auto_offset_reset="earliest",
    )
    
    try:
        logger.info("Starting Kafka consumer")
        await consumer.start()
        logger.info("Kafka consumer started successfully")
        
        message_count = 0
        async for msg in consumer:
            message_count += 1
            try:
                topic = msg.topic
                value = msg.value.decode()
                logger.info("Received metric message", extra={
                    "topic": topic,
                    "message_count": message_count,
                    "offset": msg.offset,
                    "partition": msg.partition,
                    "value": value
                })
                
                # Parse JSON for additional logging
                try:
                    parsed_value = json.loads(value)
                    logger.debug("Parsed metric data", extra={
                        "event": parsed_value.get("event"),
                        "timestamp": parsed_value.get("timestamp"),
                        "request_id": parsed_value.get("request_id"),
                        "duration": parsed_value.get("duration"),
                        "rows_ingested": parsed_value.get("rows_ingested"),
                    })
                except json.JSONDecodeError as e:
                    logger.warning("Failed to parse metric JSON", extra={
                        "value": value,
                        "error": str(e)
                    })
                    
            except Exception as e:
                logger.error("Failed to process metric message", exc_info=True, extra={
                    "topic": msg.topic if msg else "unknown",
                    "offset": msg.offset if msg else "unknown",
                    "partition": msg.partition if msg else "unknown",
                })
                
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
import asyncio, os
from aiokafka import AIOKafkaConsumer
from pathlib import Path
from common.logging import get_logger

logger = get_logger(__name__)

LOG_FILE = Path("logs/service_logs.jsonl")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

async def consume():
    logger.info("Starting log consumer service")
    logger.info("Connecting to Kafka", extra={"bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092")})
    
    consumer = AIOKafkaConsumer(
        "service_logs",
        bootstrap_servers=os.getenv("KAFKA_BROKERS", "kafka:9092"),
        group_id="log_consumer",
        auto_offset_reset="earliest",
    )
    
    try:
        logger.info("Starting Kafka consumer")
        await consumer.start()
        logger.info("Kafka consumer started successfully")
        logger.info("Log file location", extra={"log_file": str(LOG_FILE)})
        
        message_count = 0
        async for msg in consumer:
            message_count += 1
            try:
                line = msg.value.decode()
                logger.debug("Received log message", extra={
                    "message_count": message_count,
                    "offset": msg.offset,
                    "partition": msg.partition,
                    "line_length": len(line)
                })
                
                # Write to log file
                with open(LOG_FILE, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
                
                if message_count % 100 == 0:
                    logger.info("Processed log messages", extra={"count": message_count})
                    
            except Exception as e:
                logger.error("Failed to process log message", exc_info=True, extra={
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
    logger.info("Starting log consumer service")
    try:
        asyncio.run(consume())
    except KeyboardInterrupt:
        logger.info("Log consumer interrupted by user")
    except Exception as e:
        logger.error("Log consumer failed", exc_info=True)
        raise 
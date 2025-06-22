import asyncio, json, logging
from aiokafka import AIOKafkaConsumer
from common import SettingsInstance as S

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

async def consume():
    consumer = AIOKafkaConsumer(
        "ingestion_metrics", "api_metrics",
        bootstrap_servers=S.kafka_bootstrap,
        group_id="metrics_logger",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            logging.info(msg.topic + " -> " + msg.value.decode())
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume()) 
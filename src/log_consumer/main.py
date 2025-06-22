import asyncio, os
from aiokafka import AIOKafkaConsumer
from pathlib import Path

LOG_FILE = Path("logs/service_logs.jsonl")
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

async def consume():
    consumer = AIOKafkaConsumer(
        "service_logs",
        bootstrap_servers=os.getenv("KAFKA_BROKERS", "kafka:9092"),
        group_id="log_consumer",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            line = msg.value.decode()
            print(line)
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume()) 
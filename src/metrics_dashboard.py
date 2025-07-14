#!/usr/bin/env python3
"""
Simple metrics dashboard for viewing ingestion and API metrics.
Run this to see real-time metrics from Kafka.
"""

import asyncio
import json
from datetime import datetime
from aiokafka import AIOKafkaConsumer
from common.settings import settings as S


async def dashboard():
    print("📊 METRICS DASHBOARD")
    print("=" * 60)
    print(f"Connecting to Kafka at: {S.kafka_bootstrap}")
    print("Waiting for metrics... (Press Ctrl+C to exit)")
    print("-" * 60)

    consumer = AIOKafkaConsumer(
        "ingestion_metrics",
        "api_metrics",
        bootstrap_servers=S.kafka_bootstrap,
        group_id="dashboard",
        auto_offset_reset="latest",  # Only show new messages
    )

    try:
        await consumer.start()
        print("✅ Connected to Kafka successfully!")
        print("-" * 60)

        async for msg in consumer:
            try:
                value = msg.value.decode()
                parsed = json.loads(value)

                # Format timestamp
                ts = parsed.get("timestamp")
                if ts:
                    dt = datetime.fromtimestamp(ts)
                    time_str = dt.strftime("%H:%M:%S")
                else:
                    time_str = "N/A"

                # Display based on event type
                event = parsed.get("event", "unknown")

                if event == "ingestion_complete":
                    print(f"\n🔄 INGESTION COMPLETE [{time_str}]")
                    print(f"   ⏱️  Duration: {parsed.get('duration', 'N/A')}s")
                    print(f"   📚 Books: {parsed.get('books_processed', 'N/A')}")
                    print(f"   👥 Students: {parsed.get('students_processed', 'N/A')}")
                    print(
                        f"   📖 Checkouts: {parsed.get('checkouts_processed', 'N/A')}"
                    )
                    print(f"   🔍 FAISS Vectors: {parsed.get('rows_ingested', 'N/A')}")
                    print(f"   🆔 Request: {parsed.get('request_id', 'N/A')[:8]}...")

                elif event == "api_request":
                    print(f"\n🌐 API REQUEST [{time_str}]")
                    print(f"   👤 Student: {parsed.get('student_id', 'N/A')}")
                    print(f"   🔍 Query: {parsed.get('query', 'N/A')}")
                    print(f"   📊 Results: {parsed.get('results_count', 'N/A')}")
                    print(f"   ⏱️  Duration: {parsed.get('duration', 'N/A')}s")

                else:
                    print(f"\n📈 {event.upper()} [{time_str}]")
                    print(f"   Data: {parsed}")

                print("-" * 60)

            except Exception as e:
                print(f"❌ Error processing message: {e}")

    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
    except Exception as e:
        print(f"❌ Dashboard error: {e}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(dashboard())

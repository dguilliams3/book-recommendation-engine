"""
Shared Kafka utilities for event publishing and consumption.

Fixed AsyncIO issues by ensuring producers are created in the correct event loop context.
Each service gets its own producer instance to avoid loop conflicts.
"""

import json
import asyncio
from typing import Optional, Callable, Any, Dict
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError
import logging

from .settings import settings
from .events import BOOK_EVENTS_TOPIC, GRAPH_EVENTS_TOPIC
from .structured_logging import get_logger

logger = get_logger(__name__)

# Prometheus metrics (optional)
try:
    from prometheus_client import Counter

    _PROMETHEUS_AVAILABLE = True
    MESSAGES_PUBLISHED_TOTAL = Counter(
        "kafka_messages_published_total",
        "Kafka messages published",
        ["topic", "status"],
    )
    MESSAGES_CONSUMED_TOTAL = Counter(
        "kafka_messages_consumed_total",
        "Kafka messages consumed",
        ["topic", "status"],
    )
except ImportError:
    _PROMETHEUS_AVAILABLE = False


class KafkaEventProducer:
    """Thread-safe Kafka producer for publishing events."""

    def __init__(self):
        self._producer: Optional[AIOKafkaProducer] = None
        self._lock = asyncio.Lock()

    async def _get_producer(self) -> AIOKafkaProducer:
        """Get or create the Kafka producer."""
        if self._producer is None:
            async with self._lock:
                if self._producer is None:
                    self._producer = AIOKafkaProducer(
                        bootstrap_servers=settings.kafka_bootstrap,
                        value_serializer=lambda v: json.dumps(v, default=str).encode(
                            "utf-8"
                        ),
                        retry_backoff_ms=1000,
                    )
                    await self._producer.start()
                    logger.info("Kafka producer started")
        return self._producer

    async def publish_event(self, topic: str, event: dict) -> bool:
        """Publish an event to a Kafka topic."""
        try:
            producer = await self._get_producer()
            await producer.send_and_wait(topic, event)
            logger.debug(f"Published event to {topic}", extra={"event": event})
            if _PROMETHEUS_AVAILABLE:
                MESSAGES_PUBLISHED_TOTAL.labels(topic=topic, status="success").inc()
            return True
        except KafkaError as e:
            logger.error(
                f"Failed to publish event to {topic}",
                exc_info=True,
                extra={"event": event},
            )
            if _PROMETHEUS_AVAILABLE:
                MESSAGES_PUBLISHED_TOTAL.labels(topic=topic, status="failure").inc()
            return False
        except Exception as e:
            logger.error(
                f"Unexpected error publishing event to {topic}",
                exc_info=True,
                extra={"event": event},
            )
            if _PROMETHEUS_AVAILABLE:
                MESSAGES_PUBLISHED_TOTAL.labels(topic=topic, status="error").inc()
            return False

    async def close(self):
        """Close the Kafka producer."""
        if self._producer:
            await self._producer.stop()
            self._producer = None
            logger.info("Kafka producer stopped")


class KafkaEventConsumer:
    """Kafka consumer for processing events."""

    def __init__(self, topic: str, group_id: str):
        self.topic = topic
        self.group_id = group_id
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False

    async def start(self, message_handler: Callable[[dict], Any]):
        """Start consuming messages from the topic."""
        if self._running:
            return

        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=settings.kafka_bootstrap,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",  # Only process new messages
            enable_auto_commit=True,
        )

        await self._consumer.start()
        self._running = True
        logger.info(f"Started Kafka consumer for topic {self.topic}")

        try:
            async for message in self._consumer:
                try:
                    await message_handler(message.value)
                    if _PROMETHEUS_AVAILABLE:
                        MESSAGES_CONSUMED_TOTAL.labels(
                            topic=self.topic, status="success"
                        ).inc()
                except Exception as e:
                    logger.error(
                        f"Error processing message from {self.topic}", exc_info=True
                    )
                    if _PROMETHEUS_AVAILABLE:
                        MESSAGES_CONSUMED_TOTAL.labels(
                            topic=self.topic, status="error"
                        ).inc()
        except Exception as e:
            logger.error(f"Kafka consumer error for topic {self.topic}", exc_info=True)
        finally:
            await self.stop()

    async def stop(self):
        """Stop the Kafka consumer."""
        if self._consumer and self._running:
            await self._consumer.stop()
            self._running = False
            logger.info(f"Stopped Kafka consumer for topic {self.topic}")


# Thread-local producer storage to avoid loop conflicts
_producers: Dict[int, KafkaEventProducer] = {}
_producer_lock = asyncio.Lock()


async def get_event_producer() -> KafkaEventProducer:
    """Get or create a producer for the current event loop.

    This fixes the 'Future attached to different loop' error by ensuring
    each asyncio loop gets its own producer instance.
    """
    loop_id = id(asyncio.get_running_loop())

    if loop_id not in _producers:
        async with _producer_lock:
            if loop_id not in _producers:
                logger.debug(
                    "Creating new Kafka producer for event loop",
                    extra={"loop_id": loop_id},
                )
                _producers[loop_id] = KafkaEventProducer()

    return _producers[loop_id]


async def publish_event(topic: str, event: dict) -> bool:
    """Convenience function for publishing events.

    This is the main interface that services should use. It handles
    the producer lifecycle automatically.
    """
    try:
        producer = await get_event_producer()
        return await producer.publish_event(topic, event)
    except Exception as e:
        logger.error(
            "Unexpected error publishing event", exc_info=True, extra={"topic": topic}
        )

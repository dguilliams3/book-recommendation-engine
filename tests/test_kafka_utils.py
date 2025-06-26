import asyncio

import pytest

from common.kafka_utils import KafkaEventProducer

class FakeProducer:
    def __init__(self):
        self.sent = []
        self.started = False
    async def start(self):
        self.started = True
    async def stop(self):
        self.started = False
    async def send_and_wait(self, topic, event):
        self.sent.append((topic, event))

@pytest.mark.asyncio
async def test_publish_event(monkeypatch):
    kep = KafkaEventProducer()
    fake = FakeProducer()
    async def _get_producer(self):
        return fake
    monkeypatch.setattr(KafkaEventProducer, "_get_producer", _get_producer)

    payload = {"hello": "world"}
    ok = await kep.publish_event("test_topic", payload)
    assert ok is True
    assert fake.sent == [("test_topic", payload)] 
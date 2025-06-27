import pytest

from common import redis_utils as RU


@pytest.mark.asyncio
async def test_mark_and_check_fallback(monkeypatch):
    async def _fake_init():
        return None

    monkeypatch.setattr(RU, "_init", _fake_init)

    await RU.mark_recommended("stuX", "bookY")
    assert await RU.was_recommended("stuX", "bookY") 
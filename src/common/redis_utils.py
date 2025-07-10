"""Minimal async Redis helper with graceful fallback.

Why not aiocache/redis-py 4.x?  redis>=5 now bundles asyncio support via
``redis.asyncio``.  If Redis is unavailable or the URL env var is unset, all
helpers silently degrade to in-memory sets so the application continues to
work.
"""
from __future__ import annotations

import asyncio
from typing import Optional

try:
    import redis.asyncio as redis  # type: ignore
except ImportError:  # pragma: no cover – optional dependency
    redis = None  # type: ignore

from .settings import settings as S
from .structured_logging import get_logger

logger = get_logger(__name__)

# In-process fallback store when Redis missing/unreachable
_fallback_sets: dict[str, set[str]] = {}
_redis_client: Optional["redis.Redis"] = None


async def _init() -> Optional["redis.Redis"]:
    global _redis_client
    if redis is None:
        return None
    if _redis_client is not None:
        return _redis_client
    try:
        _redis_client = await redis.from_url(S.redis_url, encoding="utf-8", decode_responses=True)
        # test connection
        await _redis_client.ping()
        logger.info("Redis connection established", extra={"url": S.redis_url})
    except Exception:  # noqa: BLE001
        logger.warning("Redis unavailable, falling back to in-process sets", exc_info=True)
        _redis_client = None
    return _redis_client


async def mark_recommended(student_id: str, book_id: str) -> None:
    """Record that *book_id* was recommended to *student_id* (idempotent)."""
    client = await _init()
    key = f"rec:{student_id}"
    if client is None:
        _fallback_sets.setdefault(key, set()).add(book_id)
    else:
        try:
            await client.sadd(key, book_id)
        except Exception:
            logger.warning("Failed to write to Redis, using fallback", exc_info=True)
            _fallback_sets.setdefault(key, set()).add(book_id)


async def was_recommended(student_id: str, book_id: str) -> bool:
    """Return True if the book was already recommended."""
    client = await _init()
    key = f"rec:{student_id}"
    if client is None:
        return book_id in _fallback_sets.get(key, set())
    try:
        return bool(await client.sismember(key, book_id))
    except Exception:
        logger.warning("Redis read failed, using fallback", exc_info=True)
        return book_id in _fallback_sets.get(key, set())


def get_redis_client():
    """Synchronous wrapper to get Redis client for non-async contexts."""
    if redis is None:
        logger.warning("Redis not available, using fallback")
        return None
    try:
        return redis.from_url(S.redis_url, encoding="utf-8", decode_responses=True)
    except Exception:
        logger.warning("Failed to create Redis client", exc_info=True)
        return None 
"""
Cache service for the LLM microservice.

This module provides Redis-based caching with graceful fallback
for idempotency and performance optimization.
"""

import json
import hashlib
from typing import Optional, Dict, Any, Union
from datetime import datetime, timedelta
import logging

try:
    import redis
    from redis.connection import ConnectionPool

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from ..models.config import RedisConfig
from ..utils.errors import CacheError
from ..utils.retry import retry_with_exponential_backoff, REDIS_RETRY_CONFIG

logger = logging.getLogger(__name__)


class CacheService:
    """Redis-based cache service with graceful fallback."""

    def __init__(self, config: RedisConfig):
        self.config = config
        self.redis_client = None
        self.enabled = config.enabled and REDIS_AVAILABLE
        self.fallback_cache = {}  # In-memory fallback cache

        if self.enabled:
            self._initialize_redis()

    def _initialize_redis(self) -> None:
        """Initialize Redis connection with error handling."""
        try:
            if not REDIS_AVAILABLE:
                logger.warning("Redis not available, falling back to in-memory cache")
                self.enabled = False
                return

            # Create connection pool
            pool = ConnectionPool.from_url(
                self.config.url,
                max_connections=self.config.max_connections,
                socket_timeout=self.config.connection_timeout,
                socket_connect_timeout=self.config.connection_timeout,
                retry_on_timeout=True,
                health_check_interval=30,
            )

            self.redis_client = redis.Redis(
                connection_pool=pool,
                decode_responses=True,
                socket_keepalive=True,
                socket_keepalive_options={},
            )

            # Test connection
            self.redis_client.ping()
            logger.info("Redis connection established successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Redis: {e}")
            self.enabled = False
            self.redis_client = None

    def _get_cache_key(self, request_id: str) -> str:
        """Generate cache key for request ID."""
        return f"{self.config.key_prefix}{request_id}"

    def _serialize_value(self, value: Any) -> str:
        """Serialize value for cache storage."""
        try:
            return json.dumps(value, default=str)
        except (TypeError, ValueError) as e:
            logger.error(f"Failed to serialize cache value: {e}")
            raise CacheError(f"Failed to serialize cache value: {e}")

    def _deserialize_value(self, value: str) -> Any:
        """Deserialize value from cache storage."""
        try:
            return json.loads(value)
        except (TypeError, ValueError) as e:
            logger.error(f"Failed to deserialize cache value: {e}")
            raise CacheError(f"Failed to deserialize cache value: {e}")

    @retry_with_exponential_backoff(
        max_retries=REDIS_RETRY_CONFIG.max_retries,
        base_delay=REDIS_RETRY_CONFIG.base_delay,
        max_delay=REDIS_RETRY_CONFIG.max_delay,
        retry_exceptions=(redis.ConnectionError, redis.TimeoutError),
        stop_exceptions=(redis.AuthenticationError,),
    )
    def _redis_get(self, key: str) -> Optional[str]:
        """Get value from Redis with retry logic."""
        if not self.enabled or not self.redis_client:
            return None

        try:
            return self.redis_client.get(key)
        except Exception as e:
            logger.error(f"Redis GET failed: {e}")
            raise

    @retry_with_exponential_backoff(
        max_retries=REDIS_RETRY_CONFIG.max_retries,
        base_delay=REDIS_RETRY_CONFIG.base_delay,
        max_delay=REDIS_RETRY_CONFIG.max_delay,
        retry_exceptions=(redis.ConnectionError, redis.TimeoutError),
        stop_exceptions=(redis.AuthenticationError,),
    )
    def _redis_set(self, key: str, value: str, ttl_seconds: int) -> bool:
        """Set value in Redis with retry logic."""
        if not self.enabled or not self.redis_client:
            return False

        try:
            return self.redis_client.setex(key, ttl_seconds, value)
        except Exception as e:
            logger.error(f"Redis SET failed: {e}")
            raise

    def get(self, request_id: str) -> Optional[Dict[str, Any]]:
        """
        Get cached response for request ID.

        Args:
            request_id: The request ID to look up

        Returns:
            Optional[Dict[str, Any]]: Cached response or None if not found
        """
        cache_key = self._get_cache_key(request_id)

        # Try Redis first
        if self.enabled:
            try:
                cached_value = self._redis_get(cache_key)
                if cached_value:
                    logger.debug(f"Cache hit for request {request_id}")
                    return self._deserialize_value(cached_value)
            except Exception as e:
                logger.warning(f"Redis cache get failed, falling back to memory: {e}")

        # Fall back to in-memory cache
        if cache_key in self.fallback_cache:
            cached_data = self.fallback_cache[cache_key]
            # Check if expired
            if datetime.utcnow() < cached_data["expires_at"]:
                logger.debug(f"Memory cache hit for request {request_id}")
                return cached_data["value"]
            else:
                # Remove expired entry
                del self.fallback_cache[cache_key]

        logger.debug(f"Cache miss for request {request_id}")
        return None

    def set(self, request_id: str, response_data: Dict[str, Any]) -> bool:
        """
        Cache response for request ID.

        Args:
            request_id: The request ID
            response_data: The response data to cache

        Returns:
            bool: True if successfully cached, False otherwise
        """
        cache_key = self._get_cache_key(request_id)
        ttl_seconds = self.config.ttl_hours * 3600

        try:
            serialized_data = self._serialize_value(response_data)
        except CacheError:
            return False

        # Try Redis first
        if self.enabled:
            try:
                success = self._redis_set(cache_key, serialized_data, ttl_seconds)
                if success:
                    logger.debug(f"Cached response for request {request_id} in Redis")
                    return True
            except Exception as e:
                logger.warning(f"Redis cache set failed, falling back to memory: {e}")

        # Fall back to in-memory cache
        expires_at = datetime.utcnow() + timedelta(seconds=ttl_seconds)
        self.fallback_cache[cache_key] = {
            "value": response_data,
            "expires_at": expires_at,
        }

        logger.debug(f"Cached response for request {request_id} in memory")
        return True

    def delete(self, request_id: str) -> bool:
        """
        Delete cached response for request ID.

        Args:
            request_id: The request ID

        Returns:
            bool: True if successfully deleted, False otherwise
        """
        cache_key = self._get_cache_key(request_id)
        success = False

        # Try Redis first
        if self.enabled:
            try:
                if self.redis_client:
                    deleted = self.redis_client.delete(cache_key)
                    success = deleted > 0
                    if success:
                        logger.debug(
                            f"Deleted cached response for request {request_id} from Redis"
                        )
            except Exception as e:
                logger.warning(f"Redis cache delete failed: {e}")

        # Also delete from in-memory cache
        if cache_key in self.fallback_cache:
            del self.fallback_cache[cache_key]
            logger.debug(
                f"Deleted cached response for request {request_id} from memory"
            )
            success = True

        return success

    def clear_expired(self) -> int:
        """
        Clear expired entries from in-memory cache.

        Returns:
            int: Number of entries cleared
        """
        if not self.fallback_cache:
            return 0

        now = datetime.utcnow()
        expired_keys = [
            key
            for key, data in self.fallback_cache.items()
            if now >= data["expires_at"]
        ]

        for key in expired_keys:
            del self.fallback_cache[key]

        if expired_keys:
            logger.debug(
                f"Cleared {len(expired_keys)} expired entries from memory cache"
            )

        return len(expired_keys)

    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dict[str, Any]: Cache statistics
        """
        stats = {
            "enabled": self.enabled,
            "redis_available": REDIS_AVAILABLE,
            "redis_connected": self.redis_client is not None,
            "fallback_cache_size": len(self.fallback_cache),
            "config": {
                "url": self.config.url,
                "ttl_hours": self.config.ttl_hours,
                "key_prefix": self.config.key_prefix,
                "max_connections": self.config.max_connections,
            },
        }

        # Try to get Redis info
        if self.enabled and self.redis_client:
            try:
                redis_info = self.redis_client.info("memory")
                stats["redis_memory_used"] = redis_info.get(
                    "used_memory_human", "unknown"
                )
                stats["redis_connected_clients"] = redis_info.get(
                    "connected_clients", "unknown"
                )
            except Exception as e:
                logger.warning(f"Failed to get Redis stats: {e}")
                stats["redis_error"] = str(e)

        return stats

    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on cache service.

        Returns:
            Dict[str, Any]: Health check results
        """
        health = {
            "status": "healthy",
            "redis_available": REDIS_AVAILABLE,
            "redis_connected": False,
            "fallback_active": False,
        }

        if not self.enabled:
            health["status"] = "disabled"
            return health

        # Test Redis connection
        if self.redis_client:
            try:
                self.redis_client.ping()
                health["redis_connected"] = True
                health["status"] = "healthy"
            except Exception as e:
                logger.warning(f"Redis health check failed: {e}")
                health["redis_connected"] = False
                health["fallback_active"] = True
                health["status"] = "degraded"
                health["error"] = str(e)
        else:
            health["fallback_active"] = True
            health["status"] = "degraded"

        return health

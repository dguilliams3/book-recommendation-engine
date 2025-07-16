"""
Performance optimization utilities for the Book Recommendation Engine.

This module provides:
- Intelligent caching strategies
- Connection pooling and resource management
- Async operation optimization
- Performance monitoring and profiling
- Memory and CPU optimization techniques

Educational Notes:
- Caching reduces database load and improves response times
- Connection pooling prevents resource exhaustion
- Async operations improve concurrency and throughput
- Performance monitoring helps identify bottlenecks
"""

import asyncio
import functools
import hashlib
import json
import time
import weakref
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, Callable, TypeVar, Union, List
from dataclasses import dataclass, field
from collections import defaultdict
import logging

# Performance monitoring imports
try:
    import psutil
    import aioredis
    from prometheus_client import Counter, Histogram, Gauge

    PERFORMANCE_DEPS_AVAILABLE = True
except ImportError:
    PERFORMANCE_DEPS_AVAILABLE = False

logger = logging.getLogger(__name__)

# Performance metrics
if PERFORMANCE_DEPS_AVAILABLE:
    CACHE_HITS = Counter("cache_hits_total", "Total cache hits", ["cache_type"])
    CACHE_MISSES = Counter("cache_misses_total", "Total cache misses", ["cache_type"])
    QUERY_DURATION = Histogram(
        "query_duration_seconds", "Query duration", ["query_type"]
    )
    MEMORY_USAGE = Gauge("memory_usage_bytes", "Memory usage in bytes")
    CPU_USAGE = Gauge("cpu_usage_percent", "CPU usage percentage")

T = TypeVar("T")


@dataclass
class PerformanceConfig:
    """Configuration for performance optimizations."""

    # Caching configuration
    cache_ttl_seconds: int = 300  # 5 minutes default
    max_cache_size: int = 1000
    enable_query_cache: bool = True
    enable_result_cache: bool = True

    # Connection pooling
    db_pool_size: int = 10
    db_max_overflow: int = 20
    redis_pool_size: int = 10

    # Async optimization
    max_concurrent_requests: int = 100
    request_timeout_seconds: int = 30
    batch_size: int = 50

    # Memory management
    gc_threshold: int = 1000  # Objects before garbage collection
    max_memory_mb: int = 1024  # Maximum memory usage

    # Performance monitoring
    enable_profiling: bool = False
    profile_slow_queries: bool = True
    slow_query_threshold_ms: int = 1000


class InMemoryCache:
    """High-performance in-memory cache with LRU eviction."""

    def __init__(self, max_size: int = 1000, ttl_seconds: int = 300):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._access_times: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        async with self._lock:
            if key not in self._cache:
                if PERFORMANCE_DEPS_AVAILABLE:
                    CACHE_MISSES.labels(cache_type="memory").inc()
                return None

            # Check TTL
            entry = self._cache[key]
            if time.time() - entry["created_at"] > self.ttl_seconds:
                del self._cache[key]
                del self._access_times[key]
                if PERFORMANCE_DEPS_AVAILABLE:
                    CACHE_MISSES.labels(cache_type="memory").inc()
                return None

            # Update access time for LRU
            self._access_times[key] = time.time()
            if PERFORMANCE_DEPS_AVAILABLE:
                CACHE_HITS.labels(cache_type="memory").inc()

            return entry["value"]

    async def set(self, key: str, value: Any) -> None:
        """Set value in cache."""
        async with self._lock:
            # Evict if at capacity
            if len(self._cache) >= self.max_size and key not in self._cache:
                await self._evict_lru()

            self._cache[key] = {"value": value, "created_at": time.time()}
            self._access_times[key] = time.time()

    async def _evict_lru(self) -> None:
        """Evict least recently used item."""
        if not self._access_times:
            return

        lru_key = min(self._access_times.keys(), key=lambda k: self._access_times[k])
        del self._cache[lru_key]
        del self._access_times[lru_key]

    async def clear(self) -> None:
        """Clear all cache entries."""
        async with self._lock:
            self._cache.clear()
            self._access_times.clear()

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "ttl_seconds": self.ttl_seconds,
            "memory_usage_mb": sum(len(str(v)) for v in self._cache.values())
            / 1024
            / 1024,
        }


class QueryCache:
    """Intelligent query result caching."""

    def __init__(self, config: PerformanceConfig):
        self.config = config
        self.memory_cache = InMemoryCache(
            max_size=config.max_cache_size, ttl_seconds=config.cache_ttl_seconds
        )
        self._redis_pool: Optional[aioredis.ConnectionPool] = None

    async def initialize_redis(self, redis_url: str) -> None:
        """Initialize Redis connection pool."""
        if not PERFORMANCE_DEPS_AVAILABLE:
            logger.warning("Redis not available, using memory cache only")
            return

        try:
            self._redis_pool = aioredis.ConnectionPool.from_url(
                redis_url, max_connections=self.config.redis_pool_size
            )
            logger.info("Redis cache initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize Redis cache: {e}")

    def _make_cache_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate cache key from function arguments."""
        key_data = {"args": args, "kwargs": sorted(kwargs.items())}
        key_str = json.dumps(key_data, sort_keys=True, default=str)
        key_hash = hashlib.md5(key_str.encode()).hexdigest()
        return f"{prefix}:{key_hash}"

    async def get(self, key: str) -> Optional[Any]:
        """Get from cache (memory first, then Redis)."""
        # Try memory cache first
        value = await self.memory_cache.get(key)
        if value is not None:
            return value

        # Try Redis cache
        if self._redis_pool:
            try:
                async with aioredis.Redis(connection_pool=self._redis_pool) as redis:
                    cached_data = await redis.get(key)
                    if cached_data:
                        value = json.loads(cached_data)
                        # Populate memory cache
                        await self.memory_cache.set(key, value)
                        if PERFORMANCE_DEPS_AVAILABLE:
                            CACHE_HITS.labels(cache_type="redis").inc()
                        return value
            except Exception as e:
                logger.warning(f"Redis cache error: {e}")

        return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set in cache (both memory and Redis)."""
        # Set in memory cache
        await self.memory_cache.set(key, value)

        # Set in Redis cache
        if self._redis_pool:
            try:
                async with aioredis.Redis(connection_pool=self._redis_pool) as redis:
                    cached_data = json.dumps(value, default=str)
                    await redis.setex(
                        key, ttl or self.config.cache_ttl_seconds, cached_data
                    )
            except Exception as e:
                logger.warning(f"Redis cache error: {e}")


# Global cache instance
_cache_instance: Optional[QueryCache] = None


async def get_cache() -> QueryCache:
    """Get or create global cache instance."""
    global _cache_instance
    if _cache_instance is None:
        config = PerformanceConfig()
        _cache_instance = QueryCache(config)
    return _cache_instance


def cached(
    ttl: int = 300, cache_key_prefix: Optional[str] = None, bypass_cache: bool = False
):
    """Decorator for caching function results."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            if bypass_cache:
                return await func(*args, **kwargs)

            cache = await get_cache()
            prefix = cache_key_prefix or f"{func.__module__}.{func.__name__}"
            cache_key = cache._make_cache_key(prefix, *args, **kwargs)

            # Try to get from cache
            cached_result = await cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for {prefix}")
                return cached_result

            # Execute function and cache result
            logger.debug(f"Cache miss for {prefix}")
            result = await func(*args, **kwargs)
            await cache.set(cache_key, result, ttl)

            return result

        return wrapper

    return decorator


class ConnectionPool:
    """Async connection pool manager."""

    def __init__(self, config: PerformanceConfig):
        self.config = config
        self._pools: Dict[str, Any] = {}
        self._semaphores: Dict[str, asyncio.Semaphore] = {}

    async def get_db_pool(self, db_url: str):
        """Get database connection pool."""
        if db_url not in self._pools:
            try:
                import asyncpg

                self._pools[db_url] = await asyncpg.create_pool(
                    db_url,
                    min_size=self.config.db_pool_size // 2,
                    max_size=self.config.db_pool_size,
                    max_inactive_connection_lifetime=300,
                )
                logger.info(f"Database pool created for {db_url[:20]}...")
            except Exception as e:
                logger.error(f"Failed to create database pool: {e}")
                raise

        return self._pools[db_url]

    async def get_semaphore(self, resource_name: str, limit: int) -> asyncio.Semaphore:
        """Get or create semaphore for resource limiting."""
        if resource_name not in self._semaphores:
            self._semaphores[resource_name] = asyncio.Semaphore(limit)
        return self._semaphores[resource_name]

    async def close_all(self) -> None:
        """Close all connection pools."""
        for pool in self._pools.values():
            if hasattr(pool, "close"):
                await pool.close()
        self._pools.clear()
        self._semaphores.clear()


class PerformanceMonitor:
    """Performance monitoring and profiling."""

    def __init__(self, config: PerformanceConfig):
        self.config = config
        self._query_times: Dict[str, List[float]] = defaultdict(list)
        self._memory_samples: List[float] = []
        self._cpu_samples: List[float] = []

    @asynccontextmanager
    async def monitor_query(self, query_type: str):
        """Context manager for monitoring query performance."""
        start_time = time.time()
        try:
            yield
        finally:
            duration = time.time() - start_time

            # Record metrics
            self._query_times[query_type].append(duration)

            if PERFORMANCE_DEPS_AVAILABLE:
                QUERY_DURATION.labels(query_type=query_type).observe(duration)

            # Log slow queries
            if (
                self.config.profile_slow_queries
                and duration * 1000 > self.config.slow_query_threshold_ms
            ):
                logger.warning(
                    f"Slow query detected: {query_type} took {duration:.3f}s"
                )

    async def collect_system_metrics(self) -> Dict[str, Any]:
        """Collect system performance metrics."""
        metrics = {}

        if PERFORMANCE_DEPS_AVAILABLE:
            try:
                # Memory usage
                memory_info = psutil.virtual_memory()
                metrics["memory_usage_mb"] = memory_info.used / 1024 / 1024
                metrics["memory_percent"] = memory_info.percent

                # CPU usage
                cpu_percent = psutil.cpu_percent(interval=1)
                metrics["cpu_percent"] = cpu_percent

                # Update Prometheus metrics
                MEMORY_USAGE.set(memory_info.used)
                CPU_USAGE.set(cpu_percent)

            except Exception as e:
                logger.warning(f"Failed to collect system metrics: {e}")

        return metrics

    def get_query_stats(self) -> Dict[str, Dict[str, float]]:
        """Get query performance statistics."""
        stats = {}

        for query_type, times in self._query_times.items():
            if times:
                stats[query_type] = {
                    "count": len(times),
                    "avg_duration": sum(times) / len(times),
                    "min_duration": min(times),
                    "max_duration": max(times),
                    "total_duration": sum(times),
                }

        return stats


class BatchProcessor:
    """Efficient batch processing for bulk operations."""

    def __init__(self, batch_size: int = 50, max_concurrent: int = 10):
        self.batch_size = batch_size
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)

    async def process_batch(
        self,
        items: List[Any],
        processor: Callable[[List[Any]], Any],
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> List[Any]:
        """Process items in batches with concurrency control."""
        results = []
        total_items = len(items)

        # Create batches
        batches = [
            items[i : i + self.batch_size]
            for i in range(0, len(items), self.batch_size)
        ]

        async def process_single_batch(batch_idx: int, batch: List[Any]) -> Any:
            async with self._semaphore:
                try:
                    result = await processor(batch)
                    if progress_callback:
                        progress_callback(batch_idx + 1, len(batches))
                    return result
                except Exception as e:
                    logger.error(f"Batch {batch_idx} failed: {e}")
                    raise

        # Process batches concurrently
        tasks = [process_single_batch(idx, batch) for idx, batch in enumerate(batches)]

        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten results
        for result in batch_results:
            if isinstance(result, Exception):
                logger.error(f"Batch processing error: {result}")
                continue

            if isinstance(result, list):
                results.extend(result)
            else:
                results.append(result)

        return results


# Global instances
_connection_pool: Optional[ConnectionPool] = None
_performance_monitor: Optional[PerformanceMonitor] = None


async def get_connection_pool() -> ConnectionPool:
    """Get or create global connection pool."""
    global _connection_pool
    if _connection_pool is None:
        config = PerformanceConfig()
        _connection_pool = ConnectionPool(config)
    return _connection_pool


async def get_performance_monitor() -> PerformanceMonitor:
    """Get or create global performance monitor."""
    global _performance_monitor
    if _performance_monitor is None:
        config = PerformanceConfig()
        _performance_monitor = PerformanceMonitor(config)
    return _performance_monitor


# Utility functions
async def optimize_memory_usage() -> None:
    """Trigger garbage collection and memory optimization."""
    import gc

    gc.collect()

    if PERFORMANCE_DEPS_AVAILABLE:
        try:
            memory_info = psutil.virtual_memory()
            logger.info(f"Memory optimization: {memory_info.percent:.1f}% used")
        except Exception:
            pass


async def warm_up_caches() -> None:
    """Warm up caches with common queries."""
    logger.info("Warming up caches...")

    # This would typically include common queries
    # For example: popular books, common genres, etc.

    cache = await get_cache()

    # Example warm-up operations
    warmup_data = {
        "popular_genres": [
            "Fiction",
            "Non-Fiction",
            "Mystery",
            "Romance",
            "Science Fiction",
        ],
        "reading_levels": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        "common_authors": ["Stephen King", "J.K. Rowling", "Agatha Christie"],
    }

    for key, value in warmup_data.items():
        await cache.set(f"warmup:{key}", value, ttl=3600)

    logger.info("Cache warm-up completed")


# Performance testing utilities
async def benchmark_function(
    func: Callable, iterations: int = 100, *args, **kwargs
) -> Dict[str, float]:
    """Benchmark a function's performance."""
    times = []

    for _ in range(iterations):
        start_time = time.time()
        try:
            if asyncio.iscoroutinefunction(func):
                await func(*args, **kwargs)
            else:
                func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Benchmark iteration failed: {e}")
            continue

        duration = time.time() - start_time
        times.append(duration)

    if not times:
        return {"error": "All iterations failed"}

    return {
        "iterations": len(times),
        "avg_duration": sum(times) / len(times),
        "min_duration": min(times),
        "max_duration": max(times),
        "total_duration": sum(times),
        "ops_per_second": len(times) / sum(times),
    }


# Context manager for performance monitoring
@asynccontextmanager
async def performance_context(operation_name: str):
    """Context manager for comprehensive performance monitoring."""
    monitor = await get_performance_monitor()

    async with monitor.monitor_query(operation_name):
        start_metrics = await monitor.collect_system_metrics()

        try:
            yield monitor
        finally:
            end_metrics = await monitor.collect_system_metrics()

            # Log performance summary
            logger.info(
                f"Performance summary for {operation_name}",
                extra={
                    "operation": operation_name,
                    "start_memory_mb": start_metrics.get("memory_usage_mb", 0),
                    "end_memory_mb": end_metrics.get("memory_usage_mb", 0),
                    "start_cpu_percent": start_metrics.get("cpu_percent", 0),
                    "end_cpu_percent": end_metrics.get("cpu_percent", 0),
                },
            )


# Export key components
__all__ = [
    "PerformanceConfig",
    "InMemoryCache",
    "QueryCache",
    "ConnectionPool",
    "PerformanceMonitor",
    "BatchProcessor",
    "cached",
    "get_cache",
    "get_connection_pool",
    "get_performance_monitor",
    "performance_context",
    "optimize_memory_usage",
    "warm_up_caches",
    "benchmark_function",
]

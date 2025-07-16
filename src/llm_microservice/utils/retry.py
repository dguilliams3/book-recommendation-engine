"""
Retry utilities for the LLM microservice.

This module provides retry logic with exponential backoff and jitter
for handling OpenAI API rate limits and network errors.
"""

import time
import random
import asyncio
from typing import Any, Callable, Optional, Type, Union
from functools import wraps
import logging

logger = logging.getLogger(__name__)


class RetryError(Exception):
    """Raised when all retry attempts are exhausted."""

    pass


def retry_with_exponential_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    backoff_factor: float = 0.1,
    retry_exceptions: tuple = (Exception,),
    stop_exceptions: tuple = (),
) -> Callable:
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds for first retry
        max_delay: Maximum delay between retries
        exponential_base: Base for exponential backoff calculation
        jitter: Whether to add random jitter to delays
        backoff_factor: Factor for jitter calculation
        retry_exceptions: Tuple of exceptions to retry on
        stop_exceptions: Tuple of exceptions to stop retrying on

    Returns:
        Decorated function with retry logic
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except stop_exceptions as e:
                    logger.warning(f"Stopping retries due to {type(e).__name__}: {e}")
                    raise
                except retry_exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) exceeded for {func.__name__}"
                        )
                        raise RetryError(f"Max retries exceeded: {e}") from e

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base**attempt), max_delay)

                    # Add jitter to prevent thundering herd
                    if jitter:
                        jitter_range = delay * backoff_factor
                        delay += random.uniform(-jitter_range, jitter_range)

                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay:.2f}s"
                    )
                    time.sleep(delay)

            # This should never be reached, but just in case
            raise RetryError(f"Unexpected retry failure") from last_exception

        @wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except stop_exceptions as e:
                    logger.warning(f"Stopping retries due to {type(e).__name__}: {e}")
                    raise
                except retry_exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) exceeded for {func.__name__}"
                        )
                        raise RetryError(f"Max retries exceeded: {e}") from e

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base**attempt), max_delay)

                    # Add jitter to prevent thundering herd
                    if jitter:
                        jitter_range = delay * backoff_factor
                        delay += random.uniform(-jitter_range, jitter_range)

                    logger.warning(
                        f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay:.2f}s"
                    )
                    await asyncio.sleep(delay)

            # This should never be reached, but just in case
            raise RetryError(f"Unexpected retry failure") from last_exception

        # Return appropriate wrapper based on whether function is async
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def calculate_retry_delay(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
    backoff_factor: float = 0.1,
) -> float:
    """
    Calculate retry delay with exponential backoff and jitter.

    Args:
        attempt: Current attempt number (0-based)
        base_delay: Base delay in seconds
        max_delay: Maximum delay
        exponential_base: Base for exponential calculation
        jitter: Whether to add jitter
        backoff_factor: Factor for jitter calculation

    Returns:
        Calculated delay in seconds
    """
    delay = min(base_delay * (exponential_base**attempt), max_delay)

    if jitter:
        jitter_range = delay * backoff_factor
        delay += random.uniform(-jitter_range, jitter_range)

    return max(0, delay)


class RetryConfig:
    """Configuration for retry behavior."""

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        backoff_factor: float = 0.1,
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.backoff_factor = backoff_factor

    def get_delay(self, attempt: int) -> float:
        """Get retry delay for given attempt."""
        return calculate_retry_delay(
            attempt=attempt,
            base_delay=self.base_delay,
            max_delay=self.max_delay,
            exponential_base=self.exponential_base,
            jitter=self.jitter,
            backoff_factor=self.backoff_factor,
        )


# Common retry configurations for different scenarios
OPENAI_RETRY_CONFIG = RetryConfig(
    max_retries=3,
    base_delay=1.0,
    max_delay=60.0,
    exponential_base=2.0,
    jitter=True,
    backoff_factor=0.1,
)

REDIS_RETRY_CONFIG = RetryConfig(
    max_retries=2,
    base_delay=0.5,
    max_delay=5.0,
    exponential_base=2.0,
    jitter=True,
    backoff_factor=0.2,
)

KAFKA_RETRY_CONFIG = RetryConfig(
    max_retries=2,
    base_delay=0.5,
    max_delay=10.0,
    exponential_base=2.0,
    jitter=True,
    backoff_factor=0.1,
)

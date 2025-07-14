"""
Utility functions for the LLM microservice.

This package contains utility functions for:
- Retry logic with exponential backoff
- Input validation and sanitization
- Error handling helpers
"""

from .retry import retry_with_exponential_backoff
from .validation import validate_request_id, sanitize_content
from .errors import (
    LLMServiceError,
    create_error_response,
    AuthenticationError,
    DuplicateRequestError,
    RateLimitError,
    OpenAIError,
    NetworkError,
    CacheError,
    LoggingError,
    ValidationError,
)

__all__ = [
    "retry_with_exponential_backoff",
    "validate_request_id",
    "sanitize_content", 
    "LLMServiceError",
    "create_error_response",
    "AuthenticationError",
    "DuplicateRequestError",
    "RateLimitError",
    "OpenAIError",
    "NetworkError",
    "CacheError",
    "LoggingError",
    "ValidationError",
] 
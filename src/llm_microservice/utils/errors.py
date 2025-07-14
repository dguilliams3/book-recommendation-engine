"""
Error handling utilities for the LLM microservice.

This module provides structured error handling with proper HTTP status codes
and error response generation.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import logging

from ..models.responses import ErrorResponse, ErrorDetail

logger = logging.getLogger(__name__)


class LLMServiceError(Exception):
    """Base exception for LLM service errors."""
    
    def __init__(
        self,
        message: str,
        error_type: str = "internal_error",
        error_code: str = "INTERNAL_ERROR",
        field: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        http_status: int = 500,
    ):
        super().__init__(message)
        self.message = message
        self.error_type = error_type
        self.error_code = error_code
        self.field = field
        self.details = details or {}
        self.http_status = http_status


class ValidationError(LLMServiceError):
    """Exception for validation errors."""
    
    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            error_type="validation_error",
            error_code="VALIDATION_ERROR",
            field=field,
            details=details,
            http_status=400,
        )


class AuthenticationError(LLMServiceError):
    """Exception for authentication errors."""
    
    def __init__(
        self,
        message: str = "Authentication failed",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            error_type="authentication_error",
            error_code="AUTHENTICATION_ERROR",
            details=details,
            http_status=401,
        )


class DuplicateRequestError(LLMServiceError):
    """Exception for duplicate request ID errors."""
    
    def __init__(
        self,
        message: str = "Duplicate request ID",
        request_id: Optional[str] = None,
    ):
        details = {"request_id": request_id} if request_id else None
        super().__init__(
            message=message,
            error_type="duplicate_request",
            error_code="DUPLICATE_REQUEST",
            details=details,
            http_status=409,
        )


class RateLimitError(LLMServiceError):
    """Exception for rate limit errors."""
    
    def __init__(
        self,
        message: str = "Rate limit exceeded",
        retry_after: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        error_details = details or {}
        if retry_after:
            error_details["retry_after"] = retry_after
        
        super().__init__(
            message=message,
            error_type="rate_limit_error",
            error_code="RATE_LIMIT_ERROR",
            details=error_details,
            http_status=429,
        )


class OpenAIError(LLMServiceError):
    """Exception for OpenAI API errors."""
    
    def __init__(
        self,
        message: str,
        error_code: str = "OPENAI_ERROR",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            error_type="openai_error",
            error_code=error_code,
            details=details,
            http_status=503,
        )


class NetworkError(LLMServiceError):
    """Exception for network-related errors."""
    
    def __init__(
        self,
        message: str = "Network error occurred",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            error_type="network_error",
            error_code="NETWORK_ERROR",
            details=details,
            http_status=503,
        )


class CacheError(LLMServiceError):
    """Exception for cache-related errors."""
    
    def __init__(
        self,
        message: str = "Cache error occurred",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            error_type="cache_error",
            error_code="CACHE_ERROR",
            details=details,
            http_status=500,
        )


class LoggingError(LLMServiceError):
    """Exception for logging-related errors."""
    
    def __init__(
        self,
        message: str = "Logging error occurred",
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            error_type="logging_error",
            error_code="LOGGING_ERROR",
            details=details,
            http_status=500,
        )


# Error type to HTTP status code mapping
ERROR_STATUS_MAPPING = {
    "validation_error": 400,
    "authentication_error": 401,
    "duplicate_request": 409,
    "rate_limit_error": 429,
    "openai_error": 503,
    "network_error": 503,
    "cache_error": 500,
    "logging_error": 500,
    "internal_error": 500,
}


def create_error_response(
    request_id: str,
    error: Exception,
    timestamp: Optional[str] = None,
) -> ErrorResponse:
    """
    Create a structured error response from an exception.
    
    Args:
        request_id: The request ID associated with the error
        error: The exception that occurred
        timestamp: Optional timestamp (defaults to current time)
        
    Returns:
        ErrorResponse: Structured error response
    """
    if timestamp is None:
        timestamp = datetime.utcnow().isoformat() + "Z"
    
    # Handle known service errors
    if isinstance(error, LLMServiceError):
        error_detail = ErrorDetail(
            error_type=error.error_type,
            error_code=error.error_code,
            message=error.message,
            field=error.field,
            details=error.details,
        )
    else:
        # Handle unknown errors
        logger.error(f"Unexpected error: {type(error).__name__}: {error}")
        error_detail = ErrorDetail(
            error_type="internal_error",
            error_code="INTERNAL_ERROR",
            message="An unexpected error occurred",
            details={"original_error": str(error)},
        )
    
    return ErrorResponse(
        request_id=request_id,
        error=error_detail,
        timestamp=timestamp,
    )


def get_http_status_from_error(error: Exception) -> int:
    """
    Get HTTP status code from an exception.
    
    Args:
        error: The exception to get status code for
        
    Returns:
        int: HTTP status code
    """
    if isinstance(error, LLMServiceError):
        return error.http_status
    
    # Default to 500 for unknown errors
    return 500


def map_openai_error(error: Exception) -> OpenAIError:
    """
    Map OpenAI API errors to our error types.
    
    Args:
        error: The OpenAI exception
        
    Returns:
        OpenAIError: Mapped error
    """
    error_message = str(error)
    error_type = type(error).__name__
    
    # Map common OpenAI errors
    if "rate limit" in error_message.lower():
        return OpenAIError(
            message="OpenAI API rate limit exceeded",
            error_code="OPENAI_RATE_LIMIT",
            details={
                "original_error": error_message,
                "error_type": error_type,
            }
        )
    elif "invalid api key" in error_message.lower():
        return OpenAIError(
            message="Invalid OpenAI API key",
            error_code="OPENAI_INVALID_KEY",
            details={
                "original_error": error_message,
                "error_type": error_type,
            }
        )
    elif "context length" in error_message.lower():
        return OpenAIError(
            message="OpenAI context length exceeded",
            error_code="OPENAI_CONTEXT_LENGTH",
            details={
                "original_error": error_message,
                "error_type": error_type,
            }
        )
    elif "timeout" in error_message.lower():
        return OpenAIError(
            message="OpenAI API timeout",
            error_code="OPENAI_TIMEOUT",
            details={
                "original_error": error_message,
                "error_type": error_type,
            }
        )
    else:
        return OpenAIError(
            message=f"OpenAI API error: {error_message}",
            error_code="OPENAI_ERROR",
            details={
                "original_error": error_message,
                "error_type": error_type,
            }
        )


def log_error_context(
    error: Exception,
    request_id: str,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Log error with context information.
    
    Args:
        error: The error that occurred
        request_id: The request ID
        context: Additional context information
    """
    context = context or {}
    
    log_data = {
        "request_id": request_id,
        "error_type": type(error).__name__,
        "error_message": str(error),
        **context,
    }
    
    if isinstance(error, LLMServiceError):
        log_data.update({
            "service_error_type": error.error_type,
            "service_error_code": error.error_code,
            "http_status": error.http_status,
        })
    
    logger.error(f"Error occurred: {log_data}")


def sanitize_error_for_logging(error_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize error data for logging by removing sensitive information.
    
    Args:
        error_data: The error data to sanitize
        
    Returns:
        Dict[str, Any]: Sanitized error data
    """
    sanitized = error_data.copy()
    
    # Remove sensitive fields
    sensitive_fields = [
        'openai_api_key',
        'api_key',
        'password',
        'secret',
        'token',
        'authorization',
    ]
    
    def remove_sensitive(obj, path=""):
        if isinstance(obj, dict):
            return {
                k: remove_sensitive(v, f"{path}.{k}" if path else k)
                for k, v in obj.items()
                if k.lower() not in sensitive_fields
            }
        elif isinstance(obj, list):
            return [remove_sensitive(item, f"{path}[{i}]") for i, item in enumerate(obj)]
        else:
            return obj
    
    return remove_sensitive(sanitized) 
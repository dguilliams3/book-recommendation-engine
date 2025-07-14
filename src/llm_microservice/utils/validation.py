"""
Validation utilities for the LLM microservice.

This module provides input validation and sanitization functions
for security and data integrity.
"""

import re
import uuid
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)


def validate_request_id(request_id: str) -> bool:
    """
    Validate that request_id is a valid UUID v4.

    Args:
        request_id: The request ID to validate

    Returns:
        True if valid UUID v4, False otherwise
    """
    try:
        uuid_obj = uuid.UUID(request_id)
        return uuid_obj.version == 4
    except (ValueError, AttributeError):
        return False


def sanitize_content(content: str, max_length: Optional[int] = None) -> str:
    """
    Sanitize text content by removing dangerous characters and normalizing.

    Args:
        content: The content to sanitize
        max_length: Maximum allowed length (optional)

    Returns:
        Sanitized content

    Raises:
        ValueError: If content is invalid after sanitization
    """
    if not isinstance(content, str):
        raise ValueError("Content must be a string")

    # Remove null bytes and other control characters
    sanitized = content.replace("\x00", "")

    # Remove other dangerous control characters but keep common ones
    sanitized = re.sub(r"[\x01-\x08\x0B\x0C\x0E-\x1F\x7F]", "", sanitized)

    # Normalize whitespace
    sanitized = re.sub(r"\s+", " ", sanitized).strip()

    # Check length if specified
    if max_length and len(sanitized) > max_length:
        raise ValueError(f"Content too long: {len(sanitized)} > {max_length}")

    # Ensure content is not empty after sanitization
    if not sanitized:
        raise ValueError("Content cannot be empty after sanitization")

    return sanitized


def validate_openai_api_key(api_key: str) -> bool:
    """
    Validate OpenAI API key format.

    Args:
        api_key: The API key to validate

    Returns:
        True if valid format, False otherwise
    """
    if not isinstance(api_key, str):
        return False

    # OpenAI API keys start with 'sk-' followed by 32+ characters
    pattern = r"^sk-[a-zA-Z0-9]{32,}$"
    return bool(re.match(pattern, api_key))


def validate_model_name(model_name: str) -> bool:
    """
    Validate OpenAI model name format.

    Args:
        model_name: The model name to validate

    Returns:
        True if valid format, False otherwise
    """
    if not isinstance(model_name, str):
        return False

    # Valid OpenAI model names contain only alphanumeric, hyphens, and dots
    pattern = r"^[a-zA-Z0-9\-\.]+$"
    return bool(re.match(pattern, model_name))


def validate_temperature(temperature: float) -> bool:
    """
    Validate temperature parameter.

    Args:
        temperature: The temperature value to validate

    Returns:
        True if valid, False otherwise
    """
    return isinstance(temperature, (int, float)) and 0.0 <= temperature <= 1.0


def validate_max_tokens(max_tokens: int) -> bool:
    """
    Validate max_tokens parameter.

    Args:
        max_tokens: The max_tokens value to validate

    Returns:
        True if valid, False otherwise
    """
    return isinstance(max_tokens, int) and 1 <= max_tokens <= 4000


def validate_penalty_param(penalty: Optional[float]) -> bool:
    """
    Validate penalty parameters (frequency_penalty, presence_penalty).

    Args:
        penalty: The penalty value to validate

    Returns:
        True if valid, False otherwise
    """
    if penalty is None:
        return True
    return isinstance(penalty, (int, float)) and -2.0 <= penalty <= 2.0


def validate_top_p(top_p: Optional[float]) -> bool:
    """
    Validate top_p parameter.

    Args:
        top_p: The top_p value to validate

    Returns:
        True if valid, False otherwise
    """
    if top_p is None:
        return True
    return isinstance(top_p, (int, float)) and 0.0 <= top_p <= 1.0


def detect_potential_injection(content: str) -> bool:
    """
    Detect potential prompt injection attempts.

    This is a basic detection mechanism. More sophisticated
    detection would require ML models or external services.

    Args:
        content: The content to check

    Returns:
        True if potential injection detected, False otherwise
    """
    # Common injection patterns
    injection_patterns = [
        r"ignore\s+previous\s+instructions",
        r"forget\s+everything",
        r"new\s+task:",
        r"system\s*:\s*",
        r"assistant\s*:\s*",
        r"user\s*:\s*",
        r"<\s*system\s*>",
        r"<\s*assistant\s*>",
        r"<\s*user\s*>",
        r"```\s*system",
        r"```\s*assistant",
        r"```\s*user",
    ]

    content_lower = content.lower()

    for pattern in injection_patterns:
        if re.search(pattern, content_lower):
            logger.warning(f"Potential injection detected: {pattern}")
            return True

    return False


def validate_metadata(metadata: Any) -> bool:
    """
    Validate metadata dictionary.

    Args:
        metadata: The metadata to validate

    Returns:
        True if valid, False otherwise
    """
    if metadata is None:
        return True

    if not isinstance(metadata, dict):
        return False

    # Check for reasonable size and content
    if len(metadata) > 50:  # Arbitrary limit
        return False

    # Check that all keys are strings and values are JSON-serializable
    for key, value in metadata.items():
        if not isinstance(key, str):
            return False

        # Check if value is JSON-serializable (basic check)
        try:
            import json

            json.dumps(value)
        except (TypeError, ValueError):
            return False

    return True


def validate_redis_url(url: str) -> bool:
    """
    Validate Redis URL format.

    Args:
        url: The Redis URL to validate

    Returns:
        True if valid format, False otherwise
    """
    if not isinstance(url, str):
        return False

    # Basic Redis URL validation
    pattern = r"^redis://([^:]+)(:\d+)?(/\d+)?$"
    return bool(re.match(pattern, url))


def validate_kafka_servers(servers: str) -> bool:
    """
    Validate Kafka bootstrap servers format.

    Args:
        servers: The Kafka servers string to validate

    Returns:
        True if valid format, False otherwise
    """
    if not isinstance(servers, str):
        return False

    # Basic Kafka servers validation (host:port,host:port,...)
    pattern = r"^[a-zA-Z0-9\-\.]+:\d+(,[a-zA-Z0-9\-\.]+:\d+)*$"
    return bool(re.match(pattern, servers))


# ValidationError is defined in errors.py to avoid circular imports


def validate_request_comprehensive(request_data: dict) -> None:
    """
    Comprehensive validation of request data.

    Args:
        request_data: The request data to validate

    Raises:
        ValueError: If validation fails
    """
    # Validate required fields
    required_fields = ["request_id", "user_prompt", "openai_api_key"]
    for field in required_fields:
        if field not in request_data:
            raise ValueError(f"Missing required field: {field}")

    # Validate request_id
    if not validate_request_id(request_data["request_id"]):
        raise ValueError("Invalid request_id format")

    # Validate user_prompt
    try:
        sanitize_content(request_data["user_prompt"], max_length=10000)
    except ValueError as e:
        raise ValueError(f"user_prompt validation failed: {str(e)}")

    # Validate OpenAI API key
    if not validate_openai_api_key(request_data["openai_api_key"]):
        raise ValueError("Invalid OpenAI API key format")

    # Validate optional fields
    if "system_prompt" in request_data and request_data["system_prompt"]:
        try:
            sanitize_content(request_data["system_prompt"], max_length=5000)
        except ValueError as e:
            raise ValueError(f"system_prompt validation failed: {str(e)}")

    if "model" in request_data:
        if not validate_model_name(request_data["model"]):
            raise ValueError("Invalid model name format")

    if "temperature" in request_data:
        if not validate_temperature(request_data["temperature"]):
            raise ValueError("Invalid temperature value")

    if "max_tokens" in request_data:
        if not validate_max_tokens(request_data["max_tokens"]):
            raise ValueError("Invalid max_tokens value")

    if "frequency_penalty" in request_data:
        if not validate_penalty_param(request_data["frequency_penalty"]):
            raise ValueError("Invalid frequency_penalty value")

    if "presence_penalty" in request_data:
        if not validate_penalty_param(request_data["presence_penalty"]):
            raise ValueError("Invalid presence_penalty value")

    if "top_p" in request_data:
        if not validate_top_p(request_data["top_p"]):
            raise ValueError("Invalid top_p value")

    if "metadata" in request_data:
        if not validate_metadata(request_data["metadata"]):
            raise ValueError("Invalid metadata format")

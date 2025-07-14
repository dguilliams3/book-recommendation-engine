"""
Response models for the LLM microservice.

These models define the structure for responses with comprehensive metadata,
performance tracking, and error handling.
"""

from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, validator
from datetime import datetime


class TokenUsage(BaseModel):
    """Token usage information from OpenAI API calls."""

    prompt_tokens: int = Field(..., ge=0, description="Tokens used in prompt")
    completion_tokens: int = Field(..., ge=0, description="Tokens used in completion")
    total_tokens: int = Field(..., ge=0, description="Total tokens used")

    @validator("total_tokens")
    def validate_total_tokens(cls, v, values):
        """Validate total tokens equals prompt + completion tokens."""
        if "prompt_tokens" in values and "completion_tokens" in values:
            expected = values["prompt_tokens"] + values["completion_tokens"]
            if v != expected:
                raise ValueError(
                    f"total_tokens ({v}) must equal prompt_tokens + completion_tokens ({expected})"
                )
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "prompt_tokens": 25,
                "completion_tokens": 45,
                "total_tokens": 70,
            }
        }


class PerformanceMetrics(BaseModel):
    """Performance metrics for monitoring and optimization."""

    request_latency_ms: float = Field(
        ..., ge=0, description="Total request latency in milliseconds"
    )
    llm_latency_ms: float = Field(
        ..., ge=0, description="LLM API call latency in milliseconds"
    )
    cache_hit: bool = Field(..., description="Whether response was served from cache")
    retry_count: int = Field(..., ge=0, description="Number of retries performed")
    rate_limited: bool = Field(..., description="Whether request was rate limited")

    class Config:
        json_schema_extra = {
            "example": {
                "request_latency_ms": 1250.5,
                "llm_latency_ms": 1200.0,
                "cache_hit": False,
                "retry_count": 0,
                "rate_limited": False,
            }
        }


class ErrorDetail(BaseModel):
    """Detailed error information for debugging."""

    error_type: str = Field(..., description="Type of error")
    error_code: str = Field(..., description="Error code")
    message: str = Field(..., description="Human-readable error message")
    field: Optional[str] = Field(None, description="Field that caused validation error")
    details: Optional[Dict[str, Any]] = Field(
        None, description="Additional error details"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "error_type": "validation_error",
                "error_code": "INVALID_API_KEY",
                "message": "Invalid OpenAI API key format",
                "field": "openai_api_key",
                "details": {"expected_format": "sk-...", "provided": "invalid-key"},
            }
        }


class LLMResponse(BaseModel):
    """Success response model for LLM calls."""

    success: bool = Field(True, description="Request success status")
    request_id: str = Field(..., description="Original request ID")

    # Response data
    data: Dict[str, Any] = Field(..., description="Response data")

    # Metadata
    usage: TokenUsage = Field(..., description="Token usage information")
    performance: PerformanceMetrics = Field(..., description="Performance metrics")
    model: str = Field(..., description="Model used for generation")
    cached: bool = Field(..., description="Whether response was cached")

    # Timing
    timestamp: str = Field(..., description="ISO 8601 timestamp of response")

    # Error information (null for success)
    error: Optional[Dict[str, Any]] = Field(None, description="Error details if any")

    @validator("timestamp")
    def validate_timestamp(cls, v):
        """Validate timestamp format."""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
            return v
        except ValueError:
            raise ValueError("timestamp must be in ISO 8601 format")

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "request_id": "550e8400-e29b-41d4-a716-446655440000",
                "data": {
                    "response": "Quantum computing uses quantum mechanical phenomena like superposition and entanglement to process information.",
                    "confidence": 0.95,
                },
                "usage": {
                    "prompt_tokens": 25,
                    "completion_tokens": 45,
                    "total_tokens": 70,
                },
                "performance": {
                    "request_latency_ms": 1250.5,
                    "llm_latency_ms": 1200.0,
                    "cache_hit": False,
                    "retry_count": 0,
                    "rate_limited": False,
                },
                "model": "gpt-4o-mini",
                "cached": False,
                "timestamp": "2025-01-11T10:30:00.123Z",
                "error": None,
            }
        }


class ErrorResponse(BaseModel):
    """Error response model for failed requests."""

    success: bool = Field(False, description="Request success status")
    request_id: str = Field(..., description="Original request ID")
    error: ErrorDetail = Field(..., description="Error details")
    timestamp: str = Field(..., description="ISO 8601 timestamp of error")

    @validator("timestamp")
    def validate_timestamp(cls, v):
        """Validate timestamp format."""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
            return v
        except ValueError:
            raise ValueError("timestamp must be in ISO 8601 format")

    class Config:
        json_schema_extra = {
            "example": {
                "success": False,
                "request_id": "550e8400-e29b-41d4-a716-446655440000",
                "error": {
                    "error_type": "validation_error",
                    "error_code": "INVALID_API_KEY",
                    "message": "Invalid OpenAI API key format",
                    "field": "openai_api_key",
                    "details": {"expected_format": "sk-...", "provided": "invalid-key"},
                },
                "timestamp": "2025-01-11T10:30:00.123Z",
            }
        }


class HealthResponse(BaseModel):
    """Health check response model."""

    status: str = Field(..., description="Service health status")
    version: str = Field(..., description="Service version")
    timestamp: str = Field(..., description="ISO 8601 timestamp")
    dependencies: Dict[str, str] = Field(..., description="Dependency status")

    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "version": "1.0.0",
                "timestamp": "2025-01-11T10:30:00.123Z",
                "dependencies": {
                    "redis": "connected",
                    "kafka": "connected",
                    "openai": "available",
                },
            }
        }

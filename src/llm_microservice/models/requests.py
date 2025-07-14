"""
Request models for the LLM microservice.

These models define the structure and validation for incoming requests
with strict typing and comprehensive validation.
"""

from typing import Optional, Dict, Any, List
from enum import Enum
from pydantic import BaseModel, Field, validator
import uuid


class ModelName(str, Enum):
    """Supported OpenAI models."""

    GPT_4O_MINI = "gpt-4o-mini"
    GPT_4O = "gpt-4o"
    GPT_35_TURBO = "gpt-3.5-turbo"
    GPT_35_TURBO_16K = "gpt-3.5-turbo-16k"


class OutputFormat(str, Enum):
    """Supported output formats."""

    TEXT = "text"
    JSON = "json"
    MARKDOWN = "markdown"


class MessageType(str, Enum):
    """Types of messages in LangChain message arrays."""

    SYSTEM = "system"
    HUMAN = "human"
    AI = "ai"


class PreProcessedPrompt(BaseModel):
    """For users who want to handle parsing and injection protection."""

    content: str = Field(..., description="Pre-processed prompt content")
    parser_applied: bool = Field(True, description="Whether parsing was applied")
    injection_protected: bool = Field(
        True, description="Whether injection protection was applied"
    )
    original_length: int = Field(..., description="Original prompt length")
    processed_length: int = Field(..., description="Processed prompt length")

    @validator("content")
    def validate_processed_content(cls, v):
        """Validate pre-processed content."""
        if len(v) > 10000:
            raise ValueError("Pre-processed content too long")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "content": "Explain quantum computing in simple terms",
                "parser_applied": True,
                "injection_protected": True,
                "original_length": 40,
                "processed_length": 40,
            }
        }


class LangChainMessage(BaseModel):
    """For users who want to send structured messages."""

    type: MessageType = Field(..., description="Type of message")
    content: str = Field(
        ..., min_length=1, max_length=10000, description="Message content"
    )

    @validator("content")
    def sanitize_content(cls, v):
        """Sanitize message content."""
        v = v.replace("\x00", "").strip()
        if not v:
            raise ValueError("content cannot be empty after sanitization")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "type": "human",
                "content": "Explain quantum computing in simple terms",
            }
        }


class LLMRequest(BaseModel):
    """Main request model for LLM calls."""

    # Required fields
    request_id: str = Field(..., description="Unique request identifier (UUID v4)")
    user_prompt: str = Field(
        ..., min_length=1, max_length=10000, description="User's prompt/message"
    )
    openai_api_key: str = Field(
        ..., pattern=r"^sk-[a-zA-Z0-9]{32,}$", description="OpenAI API key"
    )

    # Optional fields with defaults
    system_prompt: Optional[str] = Field(
        None, max_length=5000, description="System instructions (optional)"
    )
    model: ModelName = Field(
        default=ModelName.GPT_4O_MINI, description="OpenAI model to use"
    )
    temperature: float = Field(
        default=0.7, ge=0.0, le=1.0, description="Sampling temperature"
    )
    max_tokens: int = Field(
        default=1000, ge=1, le=4000, description="Maximum tokens to generate"
    )
    output_format: OutputFormat = Field(
        default=OutputFormat.TEXT, description="Desired output format"
    )

    # Advanced options
    top_p: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Nucleus sampling parameter"
    )
    frequency_penalty: Optional[float] = Field(
        None, ge=-2.0, le=2.0, description="Frequency penalty"
    )
    presence_penalty: Optional[float] = Field(
        None, ge=-2.0, le=2.0, description="Presence penalty"
    )

    # Advanced prompt handling (optional)
    pre_processed_prompt: Optional[PreProcessedPrompt] = Field(
        None, description="Pre-processed prompt"
    )
    messages: Optional[List[LangChainMessage]] = Field(
        None, description="Structured LangChain messages"
    )

    # Prompt template configuration (legacy)
    prompt_template: Optional[Dict[str, Any]] = Field(
        None, description="Custom prompt template config"
    )

    # Metadata
    metadata: Optional[Dict[str, Any]] = Field(
        None, description="Additional metadata for logging"
    )

    @validator("request_id")
    def validate_request_id(cls, v):
        """Validate request ID is a valid UUID v4."""
        try:
            uuid.UUID(v)
            return v
        except ValueError:
            raise ValueError("request_id must be a valid UUID v4")

    @validator("user_prompt")
    def sanitize_user_prompt(cls, v):
        """Sanitize user prompt."""
        # Basic sanitization - remove null bytes, excessive whitespace
        v = v.replace("\x00", "").strip()
        if not v:
            raise ValueError("user_prompt cannot be empty after sanitization")
        return v

    @validator("system_prompt")
    def sanitize_system_prompt(cls, v):
        """Sanitize system prompt."""
        if v is not None:
            v = v.replace("\x00", "").strip()
            if not v:
                return None
        return v

    @validator("openai_api_key")
    def validate_openai_api_key(cls, v):
        """Validate OpenAI API key format."""
        if not v.startswith("sk-"):
            raise ValueError('OpenAI API key must start with "sk-"')
        if len(v) < 35:  # sk- + at least 32 characters
            raise ValueError("OpenAI API key is too short")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "request_id": "550e8400-e29b-41d4-a716-446655440000",
                "user_prompt": "Explain quantum computing in simple terms",
                "openai_api_key": "sk-1234567890abcdef1234567890abcdef1234567890abcdef",
                "model": "gpt-4o-mini",
                "temperature": 0.7,
                "max_tokens": 500,
                "output_format": "text",
                "metadata": {"source": "api", "user_id": "12345"},
            }
        }

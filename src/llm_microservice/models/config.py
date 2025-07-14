"""
Configuration models for the LLM microservice.

These models define the configuration structure with environment variable
support and validation.
"""

from typing import Optional
from pydantic import BaseModel, Field, validator
import os


class LLMConfig(BaseModel):
    """Configuration for LLM integration."""

    model_name: str = Field(..., description="OpenAI model name")
    temperature: float = Field(0.7, ge=0.0, le=1.0, description="Sampling temperature")
    max_tokens: int = Field(
        1000, ge=1, le=4000, description="Maximum tokens to generate"
    )
    top_p: Optional[float] = Field(
        None, ge=0.0, le=1.0, description="Nucleus sampling parameter"
    )
    frequency_penalty: Optional[float] = Field(
        None, ge=-2.0, le=2.0, description="Frequency penalty"
    )
    presence_penalty: Optional[float] = Field(
        None, ge=-2.0, le=2.0, description="Presence penalty"
    )

    # Retry configuration
    max_retries: int = Field(3, ge=0, le=10, description="Maximum retry attempts")
    retry_delay_base: float = Field(
        1.0, ge=0.1, le=10.0, description="Base delay for exponential backoff"
    )
    retry_delay_max: float = Field(
        60.0, ge=1.0, le=300.0, description="Maximum delay between retries"
    )

    # Timeout configuration
    request_timeout: int = Field(
        30, ge=5, le=300, description="Request timeout in seconds"
    )
    connection_timeout: int = Field(
        10, ge=1, le=60, description="Connection timeout in seconds"
    )


class RedisConfig(BaseModel):
    """Configuration for Redis caching."""

    url: str = Field(
        default="redis://localhost:6379", description="Redis connection URL"
    )
    enabled: bool = Field(default=True, description="Enable Redis caching")
    ttl_hours: int = Field(default=24, ge=1, le=168, description="Cache TTL in hours")
    key_prefix: str = Field(default="llm:response:", description="Redis key prefix")
    connection_timeout: int = Field(
        default=5, ge=1, le=30, description="Connection timeout in seconds"
    )
    max_connections: int = Field(
        default=10, ge=1, le=100, description="Maximum connections in pool"
    )


class KafkaConfig(BaseModel):
    """Configuration for Kafka logging."""

    bootstrap_servers: str = Field(
        default="localhost:9092", description="Kafka bootstrap servers"
    )
    enabled: bool = Field(default=False, description="Enable Kafka logging")
    request_topic: str = Field(
        default="llm-requests", description="Topic for request logs"
    )
    metrics_topic: str = Field(default="llm-metrics", description="Topic for metrics")
    error_topic: str = Field(default="llm-errors", description="Topic for error logs")
    producer_timeout: int = Field(
        default=10, ge=1, le=60, description="Producer timeout in seconds"
    )
    max_retries: int = Field(
        default=3, ge=0, le=10, description="Maximum producer retries"
    )


class ServiceConfig(BaseModel):
    """Main service configuration."""

    # Core settings
    app_name: str = Field(default="llm-microservice", description="Application name")
    version: str = Field(default="1.0.0", description="Service version")
    debug: bool = Field(default=False, description="Debug mode")

    # Server settings
    host: str = Field(default="0.0.0.0", description="Server host")
    port: int = Field(default=8000, ge=1, le=65535, description="Server port")

    # Authentication
    enable_auth: bool = Field(default=False, description="Enable API key validation")

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    log_file: Optional[str] = Field(
        default=None, description="Log file path (fallback for Kafka)"
    )

    # Rate limiting
    rate_limit_enabled: bool = Field(default=False, description="Enable rate limiting")
    rate_limit_requests: int = Field(
        default=100, ge=1, description="Requests per minute"
    )

    # Default model configuration
    default_model: str = Field(
        default="gpt-4o-mini", description="Default OpenAI model"
    )
    default_temperature: float = Field(
        default=0.7, ge=0.0, le=1.0, description="Default temperature"
    )
    default_max_tokens: int = Field(
        default=1000, ge=1, le=4000, description="Default max tokens"
    )

    # Service dependencies
    redis: RedisConfig = Field(
        default_factory=RedisConfig, description="Redis configuration"
    )
    kafka: KafkaConfig = Field(
        default_factory=KafkaConfig, description="Kafka configuration"
    )

    @validator("log_level")
    def validate_log_level(cls, v):
        """Validate log level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v.upper()

    @classmethod
    def from_env(cls) -> "ServiceConfig":
        """Create configuration from environment variables."""
        return cls(
            # Core settings
            app_name=os.getenv("LLM_APP_NAME", "llm-microservice"),
            version=os.getenv("LLM_VERSION", "1.0.0"),
            debug=os.getenv("LLM_DEBUG", "false").lower() == "true",
            # Server settings
            host=os.getenv("LLM_HOST", "0.0.0.0"),
            port=int(os.getenv("LLM_PORT", "8000")),
            # Authentication
            enable_auth=os.getenv("LLM_ENABLE_AUTH", "false").lower() == "true",
            # Logging
            log_level=os.getenv("LLM_LOG_LEVEL", "INFO"),
            log_file=os.getenv("LLM_LOG_FILE"),
            # Rate limiting
            rate_limit_enabled=os.getenv("LLM_RATE_LIMIT_ENABLED", "false").lower()
            == "true",
            rate_limit_requests=int(os.getenv("LLM_RATE_LIMIT_REQUESTS", "100")),
            # Default model configuration
            default_model=os.getenv("LLM_DEFAULT_MODEL", "gpt-4o-mini"),
            default_temperature=float(os.getenv("LLM_DEFAULT_TEMPERATURE", "0.7")),
            default_max_tokens=int(os.getenv("LLM_DEFAULT_MAX_TOKENS", "1000")),
            # Redis configuration
            redis=RedisConfig(
                url=os.getenv("REDIS_URL", "redis://localhost:6379"),
                enabled=os.getenv("REDIS_ENABLED", "true").lower() == "true",
                ttl_hours=int(os.getenv("REDIS_TTL_HOURS", "24")),
                key_prefix=os.getenv("REDIS_KEY_PREFIX", "llm:response:"),
                connection_timeout=int(os.getenv("REDIS_CONNECTION_TIMEOUT", "5")),
                max_connections=int(os.getenv("REDIS_MAX_CONNECTIONS", "10")),
            ),
            # Kafka configuration
            kafka=KafkaConfig(
                bootstrap_servers=os.getenv(
                    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
                ),
                enabled=os.getenv("KAFKA_ENABLED", "false").lower() == "true",
                request_topic=os.getenv("KAFKA_REQUEST_TOPIC", "llm-requests"),
                metrics_topic=os.getenv("KAFKA_METRICS_TOPIC", "llm-metrics"),
                error_topic=os.getenv("KAFKA_ERROR_TOPIC", "llm-errors"),
                producer_timeout=int(os.getenv("KAFKA_PRODUCER_TIMEOUT", "10")),
                max_retries=int(os.getenv("KAFKA_MAX_RETRIES", "3")),
            ),
        )

    class Config:
        json_schema_extra = {
            "example": {
                "app_name": "llm-microservice",
                "version": "1.0.0",
                "debug": False,
                "host": "0.0.0.0",
                "port": 8000,
                "enable_auth": False,
                "log_level": "INFO",
                "rate_limit_enabled": False,
                "rate_limit_requests": 100,
                "default_model": "gpt-4o-mini",
                "default_temperature": 0.7,
                "default_max_tokens": 1000,
                "redis": {
                    "url": "redis://localhost:6379",
                    "enabled": True,
                    "ttl_hours": 24,
                    "key_prefix": "llm:response:",
                    "connection_timeout": 5,
                    "max_connections": 10,
                },
                "kafka": {
                    "bootstrap_servers": "localhost:9092",
                    "enabled": False,
                    "request_topic": "llm-requests",
                    "metrics_topic": "llm-metrics",
                    "error_topic": "llm-errors",
                    "producer_timeout": 10,
                    "max_retries": 3,
                },
            }
        }

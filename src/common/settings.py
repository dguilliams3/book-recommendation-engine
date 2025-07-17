from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, AnyUrl
import os


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="allow")

    # --- core -----------------------------------------------------------
    project_name: str = "Elementary‑Books‑AI"
    data_dir: Path = Path("data")  # relative path for local development
    model_name: str = Field(
        os.getenv("MODEL_NAME", "gpt-4o-mini"), validation_alias="OPENAI_MODEL"
    )
    embedding_model: str = Field(
        "text-embedding-3-small", validation_alias="EMBEDDING_MODEL"
    )
    openai_api_key: str = Field(
        os.getenv("OPENAI_KEY", ""), validation_alias="OPENAI_API_KEY"
    )
    vector_store_type: str = Field(
        "faiss", validation_alias="VECTOR_STORE_TYPE"
    )  # faiss|chroma|pinecone

    # Database configuration with flexible host
    db_host: str = Field(
        "postgres", validation_alias="DB_HOST"
    )  # postgres (Docker) or localhost (local)
    db_port: int = Field(5432, validation_alias="DB_PORT")
    db_user: str = Field("books", validation_alias="DB_USER")
    db_password: str = Field("books", validation_alias="DB_PASSWORD")
    db_name: str = Field("books", validation_alias="DB_NAME")

    # Legacy DB_URL support (for backward compatibility)
    legacy_db_url: AnyUrl | None = Field(None, validation_alias="DB_URL")

    # Kafka configuration with flexible host
    kafka_host: str = Field(
        "kafka", validation_alias="KAFKA_HOST"
    )  # kafka (Docker) or localhost (local)
    kafka_port: int = Field(9092, validation_alias="KAFKA_PORT")

    # LLM Microservice configuration
    llm_service_url: str = Field(
        "http://llm_microservice:8000", validation_alias="LLM_SERVICE_URL"
    )
    llm_service_enabled: bool = Field(True, validation_alias="LLM_SERVICE_ENABLED")
    llm_fallback_enabled: bool = Field(True, validation_alias="LLM_FALLBACK_ENABLED")
    llm_request_timeout: int = Field(30, validation_alias="LLM_REQUEST_TIMEOUT")
    llm_max_retries: int = Field(3, validation_alias="LLM_MAX_RETRIES")
    llm_circuit_breaker_threshold: int = Field(5, validation_alias="LLM_CIRCUIT_BREAKER_THRESHOLD")
    llm_circuit_breaker_timeout: int = Field(60, validation_alias="LLM_CIRCUIT_BREAKER_TIMEOUT")

    # Enrichment Configuration
    max_enrichment_attempts: int = Field(3, validation_alias="MAX_ENRICHMENT_ATTEMPTS")  # Maximum enrichment attempts per book
    enrichment_retry_delay_base: float = Field(2.0, validation_alias="ENRICHMENT_RETRY_DELAY_BASE")  # Base delay for exponential backoff (seconds)
    enrichment_retry_delay_max: float = Field(60.0, validation_alias="ENRICHMENT_RETRY_DELAY_MAX")  # Maximum delay between retries (seconds)
    enrichment_timeout: float = Field(30.0, validation_alias="ENRICHMENT_TIMEOUT")  # Timeout for enrichment requests (seconds)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # If legacy DB_URL is provided, it takes precedence
        if self.legacy_db_url:
            self._db_url = str(self.legacy_db_url)
        else:
            self._db_url = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

        # If legacy KAFKA_BROKERS is provided, it takes precedence
        if self.legacy_kafka_bootstrap:
            self._kafka_bootstrap = self.legacy_kafka_bootstrap
        else:
            self._kafka_bootstrap = f"{self.kafka_host}:{self.kafka_port}"

    @property
    def db_url(self) -> str:
        """Get the database URL, constructed from components or from legacy DB_URL"""
        return self._db_url

    @property
    def kafka_bootstrap(self) -> str:
        """Get the Kafka bootstrap servers, constructed from components"""
        return self._kafka_bootstrap

    # --- derived convenience paths ----------------------------------------
    @property
    def vector_store_dir(self) -> Path:
        """Directory used for persisted vector store indexes."""
        return self.data_dir / "vector_store"

    @property
    def async_db_url(self) -> str:
        """Return SQLAlchemy URL with asyncpg driver for PostgreSQL."""
        url = self.db_url
        if url.startswith("postgresql://"):
            return url.replace("postgresql://", "postgresql+asyncpg://")
        if url.startswith("postgresql+psycopg2://"):
            return url.replace("postgresql+psycopg2://", "postgresql+asyncpg://")
        return url

    @property
    def redis_host(self) -> str:
        """Extract host from redis URL."""
        from urllib.parse import urlparse

        parsed = urlparse(self.redis_url)
        return parsed.hostname or "localhost"

    @property
    def redis_port(self) -> int:
        """Extract port from redis URL."""
        from urllib.parse import urlparse

        parsed = urlparse(self.redis_url)
        return parsed.port or 6379

    # Legacy KAFKA_BROKERS support (for backward compatibility)
    legacy_kafka_bootstrap: str | None = Field(None, validation_alias="KAFKA_BROKERS")

    google_books_api_key: str | None = Field(None, validation_alias="GOOGLE_BOOKS_KEY")
    openai_request_timeout: int = Field(20, validation_alias="OPENAI_TIMEOUT")
    # LLM call budget
    max_tokens: int = Field(600, validation_alias="MAX_TOKENS_PER_CALL")

    # Redis (optional) - use redis:6379 for Docker, localhost:6379 for local
    redis_url: str = Field(
        os.getenv("REDIS_URL", "redis://redis:6379/0"), validation_alias="REDIS_URL"
    )
    redis_max_connections: int = Field(20, validation_alias="REDIS_MAX_CONNECTIONS")
    redis_connection_timeout: int = Field(
        30, validation_alias="REDIS_CONNECTION_TIMEOUT"
    )

    # Database connection pool settings
    db_pool_size: int = Field(10, validation_alias="DB_POOL_SIZE")
    db_max_overflow: int = Field(20, validation_alias="DB_MAX_OVERFLOW")

    # batch parameters
    similarity_threshold: float = Field(0.75, validation_alias="SIMILARITY_THRESHOLD")
    half_life_days: int = Field(45, validation_alias="HALF_LIFE_DAYS")
    graph_refresh_delay_seconds: int = Field(300, validation_alias="GRAPH_REFRESH_DELAY")  # 5 minutes instead of 30 seconds

    # data file paths -------------------------------------------------------
    catalog_csv_path: Path = Field(
        Path("data/catalog_sample.csv"), validation_alias="CATALOG_CSV"
    )
    students_csv_path: Path = Field(
        Path("data/students_sample.csv"), validation_alias="STUDENTS_CSV"
    )
    checkouts_csv_path: Path = Field(
        Path("data/checkouts_sample.csv"), validation_alias="CHECKOUTS_CSV"
    )
    sql_dir: Path = Field(Path("sql"), validation_alias="SQL_DIR")

    # service ports (overridable) ---------------------------------------
    ingestion_port: int = 8001
    recommendation_api_port: int = 8000
    streamlit_port: int = 8501
    metrics_consumer_port: int = 8003
    prometheus_port: int = 9090
    user_ingest_port: int = 8004

    # UI configuration ----------------------------------------------------
    ui_api_timeout_seconds: int = Field(30, validation_alias="UI_API_TIMEOUT")
    ui_max_retries: int = Field(3, validation_alias="UI_MAX_RETRIES")
    ui_quick_timeout_seconds: int = Field(15, validation_alias="UI_QUICK_TIMEOUT")
    ui_health_timeout_seconds: int = Field(5, validation_alias="UI_HEALTH_TIMEOUT")
    ui_kafka_timeout_seconds: float = Field(5.0, validation_alias="UI_KAFKA_TIMEOUT")

    # env flags for optional workers ------------------------------------
    enable_tts: bool = Field(False, validation_alias="ENABLE_TTS")
    enable_image: bool = Field(False, validation_alias="ENABLE_IMAGE")
    enable_reader_mode: bool = Field(
        True, validation_alias="ENABLE_READER_MODE"
    )  # Reader Mode feature flag

    # Additional optional config values
    model_max_tokens: int = Field(int(os.getenv("MODEL_MAX_TOKENS", "600")))

    # Path to weights.json (relative or absolute)
    weights_path: str = Field(
        os.getenv("WEIGHTS_PATH", "src/recommendation_api/weights.json")
    )


# singleton
SettingsInstance = Settings()
# pep-8 alias
settings = SettingsInstance

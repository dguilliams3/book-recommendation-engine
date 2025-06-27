from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, AnyUrl
import os

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="allow")
    
    # --- core -----------------------------------------------------------
    project_name: str = "Elementary‑Books‑AI"
    data_dir: Path = Path("data")          # relative path for local development
    model_name: str = Field(os.getenv("MODEL_NAME", "gpt-4o-mini"), validation_alias="OPENAI_MODEL")
    embedding_model: str = Field("text-embedding-3-small", validation_alias="EMBEDDING_MODEL")
    openai_api_key: str = Field(os.getenv("OPENAI_KEY", ""), validation_alias="OPENAI_API_KEY")
    vector_store_type: str = Field("faiss", validation_alias="VECTOR_STORE_TYPE")   # faiss|chroma|pinecone
    
    # Database configuration with flexible host
    db_host: str = Field("postgres", validation_alias="DB_HOST")  # postgres (Docker) or localhost (local)
    db_port: int = Field(5432, validation_alias="DB_PORT")
    db_user: str = Field("books", validation_alias="DB_USER")
    db_password: str = Field("books", validation_alias="DB_PASSWORD")
    db_name: str = Field("books", validation_alias="DB_NAME")
    
    # Legacy DB_URL support (for backward compatibility)
    legacy_db_url: AnyUrl | None = Field(None, validation_alias="DB_URL")
    
    # Kafka configuration with flexible host
    kafka_host: str = Field("kafka", validation_alias="KAFKA_HOST")  # kafka (Docker) or localhost (local)
    kafka_port: int = Field(9092, validation_alias="KAFKA_PORT")
    
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
    
    # Legacy KAFKA_BROKERS support (for backward compatibility)
    legacy_kafka_bootstrap: str | None = Field(None, validation_alias="KAFKA_BROKERS")
    
    google_books_api_key: str | None = Field(None, validation_alias="GOOGLE_BOOKS_KEY")
    openai_request_timeout: int = Field(20, validation_alias="OPENAI_TIMEOUT")
    # LLM call budget
    max_tokens: int = Field(600, validation_alias="MAX_TOKENS_PER_CALL")

    # Redis (optional) - use redis:6379 for Docker, localhost:6379 for local
    redis_url: str = Field(os.getenv("REDIS_URL", "redis://redis:6379/0"), validation_alias="REDIS_URL")

    # batch parameters
    similarity_threshold: float = Field(0.75, validation_alias="SIMILARITY_THRESHOLD")
    half_life_days: int = Field(45, validation_alias="HALF_LIFE_DAYS")

    # service ports (overridable) ---------------------------------------
    ingestion_port: int = 8001
    api_port: int = 8000
    streamlit_port: int = 8501
    metrics_consumer_port: int = 8003

    # env flags for optional workers ------------------------------------
    enable_tts: bool = Field(False, validation_alias="ENABLE_TTS")
    enable_image: bool = Field(False, validation_alias="ENABLE_IMAGE")

    # Additional optional config values
    model_max_tokens: int = Field(int(os.getenv("MODEL_MAX_TOKENS", "600")))

    # Path to weights.json (relative or absolute)
    weights_path: str = Field(os.getenv("WEIGHTS_PATH", "src/recommendation_api/weights.json"))

# singleton
SettingsInstance = Settings()
# pep-8 alias
settings = SettingsInstance 
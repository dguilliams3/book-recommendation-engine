from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field, AnyUrl, ConfigDict

class Settings(BaseSettings):
    model_config = ConfigDict(env_file=".env", extra="allow")
    
    # --- core -----------------------------------------------------------
    project_name: str = "Elementary‑Books‑AI"
    data_dir: Path = Path("data")          # relative path for local development
    model_name: str = Field("gpt-4o-mini", env="OPENAI_MODEL")
    embedding_model: str = Field("text-embedding-3-small", env="EMBEDDING_MODEL")
    openai_api_key: str = Field(..., env="OPENAI_API_KEY")
    vector_store_type: str = Field("faiss", env="VECTOR_STORE_TYPE")   # faiss|chroma|pinecone
    
    # Database configuration with flexible host
    db_host: str = Field("postgres", env="DB_HOST")  # postgres (Docker) or localhost (local)
    db_port: int = Field(5432, env="DB_PORT")
    db_user: str = Field("books", env="DB_USER")
    db_password: str = Field("books", env="DB_PASSWORD")
    db_name: str = Field("books", env="DB_NAME")
    
    # Legacy DB_URL support (for backward compatibility)
    legacy_db_url: AnyUrl | None = Field(None, env="DB_URL")
    
    # Kafka configuration with flexible host
    kafka_host: str = Field("kafka", env="KAFKA_HOST")  # kafka (Docker) or localhost (local)
    kafka_port: int = Field(9092, env="KAFKA_PORT")
    
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
    legacy_kafka_bootstrap: str | None = Field(None, env="KAFKA_BROKERS")
    
    google_books_api_key: str | None = Field(None, env="GOOGLE_BOOKS_KEY")
    openai_request_timeout: int = Field(20, env="OPENAI_TIMEOUT")

    # batch parameters
    similarity_threshold: float = Field(0.75, env="SIMILARITY_THRESHOLD")
    half_life_days: int = Field(45, env="HALF_LIFE_DAYS")

    # service ports (overridable) ---------------------------------------
    ingestion_port: int = 8001
    api_port: int = 8000
    streamlit_port: int = 8501
    metrics_consumer_port: int = 8003

    # env flags for optional workers ------------------------------------
    enable_tts: bool = Field(False, env="ENABLE_TTS")
    enable_image: bool = Field(False, env="ENABLE_IMAGE")

# singleton
SettingsInstance = Settings()
# pep-8 alias
settings = SettingsInstance 
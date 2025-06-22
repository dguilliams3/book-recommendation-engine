from pathlib import Path
from pydantic import BaseSettings, Field, AnyUrl

class Settings(BaseSettings):
    # --- core -----------------------------------------------------------
    project_name: str = "Elementary‑Books‑AI"
    data_dir: Path = Path("/data")          # mounted volume
    model_name: str = Field("gpt-4o", env="OPENAI_MODEL")
    openai_api_key: str = Field(..., env="OPENAI_API_KEY")
    vector_store_type: str = Field("faiss", env="VECTOR_STORE_TYPE")   # faiss|chroma|pinecone
    db_url: AnyUrl = Field("postgresql+asyncpg://books:books@postgres:5432/books", env="DB_URL")
    kafka_bootstrap: str = Field("kafka:9092", env="KAFKA_BROKERS")
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

    class Config:
        env_file = ".env"        # populated from .env.template

# singleton
SettingsInstance = Settings()
# pep-8 alias
settings = SettingsInstance 
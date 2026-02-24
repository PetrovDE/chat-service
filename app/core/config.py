# app/core/config.py

from pydantic import Field, AnyUrl
from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    # Основные
    DATABASE_URL: str = Field(..., env="DATABASE_URL")
    ALEMBIC_DATABASE_URL: str = Field(..., env="ALEMBIC_DATABASE_URL")
    JWT_SECRET_KEY: str = Field(..., env="JWT_SECRET_KEY")
    JWT_ALGORITHM: str = Field("HS256", env="JWT_ALGORITHM")
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(10080, env="JWT_ACCESS_TOKEN_EXPIRE_MINUTES")

    password_min_length: int = Field(8, env="PASSWORD_MIN_LENGTH")
    allowed_origins: str = Field("http://localhost:8000,http://127.0.0.1:8000", env="ALLOWED_ORIGINS")
    app_env: str = Field("production", env="APP_ENV")
    app_secret_key: str = Field("", env="APP_SECRET_KEY")
    host: str = Field("0.0.0.0", env="HOST")
    port: int = Field(8080, env="PORT")
    supported_filetypes: str = Field("pdf,docx,txt,md", env="SUPPORTED_FILETYPES")

    # Corporate API
    CORPORATE_API_URL: str = Field("", env="CORPORATE_API_URL")
    CORPORATE_API_USERNAME: str = Field("", env="CORPORATE_API_USERNAME")
    CORPORATE_API_TOKEN: str = Field("", env="CORPORATE_API_TOKEN")

    # AI HUB Configuration
    AIHUB_URL: str = Field("", env="AIHUB_URL")
    AIHUB_KEYCLOAK_HOST: str = Field("", env="AIHUB_KEYCLOAK_HOST")
    AIHUB_USERNAME: str = Field("", env="AIHUB_USERNAME")
    AIHUB_PASSWORD: str = Field("", env="AIHUB_PASSWORD")
    AIHUB_CLIENT_ID: str = Field("", env="AIHUB_CLIENT_ID")
    AIHUB_CLIENT_SECRET: str = Field("", env="AIHUB_CLIENT_SECRET")
    AIHUB_REQUEST_TIMEOUT: int = Field(120, env="AIHUB_REQUEST_TIMEOUT")
    AIHUB_VERIFY_SSL: bool = Field(False, env="AIHUB_VERIFY_SSL")
    AIHUB_DEFAULT_MODEL: str = Field("vikhr", env="AIHUB_DEFAULT_MODEL")
    AIHUB_EMBEDDING_MODEL: str = Field("embedding-model", env="AIHUB_EMBEDDING_MODEL")
    AIHUB_CHAT_STREAM_PATH: str = Field("", env="AIHUB_CHAT_STREAM_PATH")

    # LLM/RAG defaults
    default_llm_mode: str = Field("local", env="DEFAULT_LLM_MODE")
    default_rag_model: str = Field("llama3.1:8b", env="DEFAULT_RAG_MODEL")
    max_chunks_per_file: int = Field(100, env="MAX_CHUNKS_PER_FILE")
    splitter_type: str = Field("smart", env="SPLITTER_TYPE")

    # VectorStore / RAG / LLM
    VECTORDB_PATH: str = Field(".chromadb", env="VECTORDB_PATH")
    COLLECTION_NAME: str = Field("documents", env="COLLECTION_NAME")

    # NOTE: оставляю для обратной совместимости
    EMBEDDINGS_MODEL: str = Field("nomic-embed-text:latest", env="EMBEDDINGS_MODEL")

    # FIX: разделяем chat и embeddings в Ollama
    OLLAMA_CHAT_MODEL: str = Field("llama3.2:latest", env="OLLAMA_CHAT_MODEL")
    OLLAMA_EMBED_MODEL: str = Field("nomic-embed-text:latest", env="OLLAMA_EMBED_MODEL")
    EMBEDDINGS_DIM: int = Field(0, env="EMBEDDINGS_DIM")  # 0 = auto (не проверяем строго)

    EMBEDDINGS_BASEURL: AnyUrl = Field("http://localhost:11434", env="EMBEDDINGS_BASEURL")
    CHUNK_SIZE: int = Field(2000, env="CHUNK_SIZE")
    CHUNK_OVERLAP: int = Field(400, env="CHUNK_OVERLAP")
    ENABLE_CACHE: bool = Field(True, env="ENABLE_CACHE")
    MAX_FILESIZE_MB: int = Field(50, env="MAX_FILESIZE_MB")

    # OpenAI/External APIs
    OPENAI_API_KEY: str = Field("", env="OPENAI_API_KEY")
    OPENAI_MODEL: str = Field("gpt-4", env="OPENAI_MODEL")

    # Системные параметры
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    SERVER_HOST: str = Field("127.0.0.1", env="SERVER_HOST")
    SERVER_PORT: int = Field(8000, env="SERVER_PORT")
    DEFAULT_MODEL_SOURCE: str = Field("ollama", env="DEFAULT_MODEL_SOURCE")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    def get_vectordb_path(self) -> Path:
        path = Path(self.VECTORDB_PATH)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def is_file_supported(self, filename: str) -> bool:
        allowed = (".pdf", ".docx", ".txt", ".csv", ".xlsx", ".json", ".md")
        return filename.lower().endswith(allowed)


settings = Settings()

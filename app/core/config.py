from pathlib import Path
from typing import Tuple

from pydantic import AnyUrl, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Core
    DATABASE_URL: str = Field(...)
    ALEMBIC_DATABASE_URL: str = Field(...)
    JWT_SECRET_KEY: str = Field(...)
    JWT_ALGORITHM: str = Field(default="HS256")
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=10080, ge=1)

    password_min_length: int = Field(default=8, ge=4)
    allowed_origins: str = Field(default="http://localhost:8000,http://127.0.0.1:8000")
    app_env: str = Field(default="production")
    app_secret_key: str = Field(default="")
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8080, ge=1, le=65535)
    supported_filetypes: str = Field(default="pdf,docx,txt,md")

    # Corporate API
    CORPORATE_API_URL: str = Field(default="")
    CORPORATE_API_USERNAME: str = Field(default="")
    CORPORATE_API_TOKEN: str = Field(default="")

    # AI HUB
    AIHUB_URL: str = Field(default="")
    AIHUB_KEYCLOAK_HOST: str = Field(default="")
    AIHUB_USERNAME: str = Field(default="")
    AIHUB_PASSWORD: str = Field(default="")
    AIHUB_CLIENT_ID: str = Field(default="")
    AIHUB_CLIENT_SECRET: str = Field(default="")
    AIHUB_REQUEST_TIMEOUT: int = Field(default=120, ge=1, le=900)
    AIHUB_VERIFY_SSL: bool = Field(default=False)
    AIHUB_DEFAULT_MODEL: str = Field(default="vikhr")
    AIHUB_EMBEDDING_MODEL: str = Field(default="embedding-model")
    AIHUB_CHAT_STREAM_PATH: str = Field(default="")
    AIHUB_MAX_PROMPT_CHARS: int = Field(default=50000, ge=1000)
    AIHUB_MAX_HISTORY_MESSAGE_CHARS: int = Field(default=2000, ge=100)

    # LLM/RAG defaults
    default_llm_mode: str = Field(default="local")
    default_rag_model: str = Field(default="llama3.1:8b")
    max_chunks_per_file: int = Field(default=100, ge=1)
    splitter_type: str = Field(default="smart")

    # VectorStore / RAG
    VECTORDB_PATH: str = Field(default=".chromadb")
    COLLECTION_NAME: str = Field(default="documents")
    EMBEDDINGS_MODEL: str = Field(default="nomic-embed-text:latest")
    OLLAMA_CHAT_MODEL: str = Field(default="llama3.2:latest")
    OLLAMA_EMBED_MODEL: str = Field(default="nomic-embed-text:latest")
    EMBEDDINGS_DIM: int = Field(default=0, ge=0)

    EMBEDDINGS_BASEURL: AnyUrl = Field(default="http://localhost:11434")
    CHUNK_SIZE: int = Field(default=2000, ge=100)
    CHUNK_OVERLAP: int = Field(default=400, ge=0)
    ENABLE_CACHE: bool = Field(default=True)
    MAX_FILESIZE_MB: int = Field(default=50, ge=1)

    # External APIs
    OPENAI_API_KEY: str = Field(default="")
    OPENAI_MODEL: str = Field(default="gpt-4")

    # Runtime
    LOG_LEVEL: str = Field(default="INFO")
    SERVER_HOST: str = Field(default="127.0.0.1")
    SERVER_PORT: int = Field(default=8000, ge=1, le=65535)
    DEFAULT_MODEL_SOURCE: str = Field(default="ollama")

    @field_validator("allowed_origins", mode="before")
    @classmethod
    def _normalize_allowed_origins(cls, value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return "*"
        parts = [x.strip() for x in raw.split(",") if x.strip()]
        return ",".join(parts) if parts else "*"

    @field_validator("supported_filetypes", mode="before")
    @classmethod
    def _normalize_supported_filetypes(cls, value: str) -> str:
        raw = str(value or "").strip()
        if not raw:
            return "pdf,docx,txt,md"
        parts = []
        for item in raw.split(","):
            ext = item.strip().lower().lstrip(".")
            if ext:
                parts.append(ext)
        return ",".join(parts) if parts else "pdf,docx,txt,md"

    @property
    def allowed_origins_list(self) -> list[str]:
        if self.allowed_origins.strip() == "*":
            return ["*"]
        return [x.strip() for x in self.allowed_origins.split(",") if x.strip()]

    @property
    def supported_filetypes_tuple(self) -> Tuple[str, ...]:
        return tuple(f".{x}" for x in self.supported_filetypes.split(",") if x)

    def get_vectordb_path(self) -> Path:
        path = Path(self.VECTORDB_PATH)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def is_file_supported(self, filename: str) -> bool:
        return filename.lower().endswith(self.supported_filetypes_tuple)


settings = Settings()

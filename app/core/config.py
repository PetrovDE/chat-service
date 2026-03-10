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
    supported_filetypes: str = Field(default="pdf,docx,txt,md,csv,json,xlsx,xls")

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
    AIHUB_CIRCUIT_WINDOW_SECONDS: int = Field(default=60, ge=5, le=600)
    AIHUB_CIRCUIT_MIN_REQUESTS: int = Field(default=4, ge=1, le=1000)
    AIHUB_CIRCUIT_FAILURE_RATIO: float = Field(default=0.5, ge=0.01, le=1.0)
    AIHUB_CIRCUIT_OPEN_SECONDS: int = Field(default=30, ge=1, le=3600)
    AIHUB_CIRCUIT_HALF_OPEN_MAX_REQUESTS: int = Field(default=1, ge=1, le=100)
    LLM_FALLBACK_POLICY_VERSION: str = Field(default="p1-aihub-first-v1")
    LLM_FALLBACK_ENABLED: bool = Field(default=True)
    LLM_FALLBACK_RESTRICTED_CLASSES: str = Field(default="restricted")

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
    OLLAMA_EMBED_MAX_INPUT_CHARS: int = Field(default=3500, ge=500, le=50000)
    OLLAMA_EMBED_SEGMENT_OVERLAP_CHARS: int = Field(default=250, ge=0, le=10000)
    EMBEDDINGS_DIM: int = Field(default=0, ge=0)

    EMBEDDINGS_BASEURL: AnyUrl = Field(default="http://localhost:11434")
    CHUNK_SIZE: int = Field(default=2000, ge=100)
    CHUNK_OVERLAP: int = Field(default=400, ge=0)
    EMBEDDING_CONCURRENCY: int = Field(default=6, ge=1, le=32)
    AIHUB_EMBEDDING_CONCURRENCY: int = Field(default=3, ge=1, le=16)
    RAG_FETCH_K_MULTIPLIER: int = Field(default=10, ge=2, le=50)
    RAG_FETCH_K_MIN: int = Field(default=40, ge=5, le=500)
    RAG_LEXICAL_POOL_MULTIPLIER: int = Field(default=3, ge=1, le=20)
    RAG_LEXICAL_POOL_MIN: int = Field(default=120, ge=20, le=2000)
    RAG_LEXICAL_POOL_MAX: int = Field(default=1200, ge=50, le=10000)
    RAG_FULL_FILE_MAX_CHUNKS: int = Field(default=800, ge=100, le=20000)
    RAG_DYNAMIC_TOPK_ENABLED: bool = Field(default=True)
    RAG_DYNAMIC_SHORT_QUERY_MAX_TOKENS: int = Field(default=4, ge=1, le=20)
    RAG_DYNAMIC_TOPK_SHORT_RATIO: float = Field(default=0.20, ge=0.01, le=1.0)
    RAG_DYNAMIC_TOPK_FACT_RATIO: float = Field(default=0.10, ge=0.01, le=1.0)
    RAG_DYNAMIC_TOPK_BROAD_RATIO: float = Field(default=0.30, ge=0.01, le=1.0)
    RAG_DYNAMIC_TOPK_MIN: int = Field(default=8, ge=1, le=2000)
    RAG_DYNAMIC_TOPK_MAX: int = Field(default=96, ge=4, le=5000)
    RAG_DYNAMIC_COVERAGE_MIN_RATIO: float = Field(default=0.35, ge=0.0, le=1.0)
    RAG_DYNAMIC_ESCALATION_ENABLED: bool = Field(default=True)
    RAG_DYNAMIC_ESCALATION_MULTIPLIER: float = Field(default=2.0, ge=1.1, le=10.0)
    RAG_DYNAMIC_ESCALATION_MAX_TOPK: int = Field(default=192, ge=8, le=20000)
    RAG_DYNAMIC_ESCALATE_TO_FULL_FILE_MAX_CHUNKS: int = Field(default=120, ge=10, le=5000)
    RAG_FULL_FILE_MIN_ROW_COVERAGE: float = Field(default=0.95, ge=0.0, le=1.0)
    RAG_FULL_FILE_ESCALATION_MAX_CHUNKS: int = Field(default=2000, ge=100, le=50000)
    FULL_FILE_MAP_BATCH_MAX_DOCS: int = Field(default=12, ge=2, le=100)
    FULL_FILE_MAP_BATCH_MAX_CHARS: int = Field(default=25000, ge=1000, le=50000)
    FULL_FILE_MAP_MAX_TOKENS: int = Field(default=900, ge=128, le=4096)
    XLSX_CHUNK_MAX_CHARS: int = Field(default=3500, ge=1000, le=120000)
    XLSX_CHUNK_MAX_ROWS: int = Field(default=40, ge=1, le=2000)
    XLSX_MAX_COLUMNS_PER_CHUNK: int = Field(default=0, ge=0, le=1000)
    XLSX_CELL_MAX_CHARS: int = Field(default=0, ge=0, le=200000)
    FULL_FILE_MAP_MAX_BATCHES: int = Field(default=300, ge=10, le=5000)
    FULL_FILE_DIRECT_CONTEXT_MAX_CHUNKS: int = Field(default=24, ge=4, le=500)
    FULL_FILE_DIRECT_CONTEXT_MAX_CHARS: int = Field(default=42000, ge=4000, le=180000)
    FULL_FILE_REDUCE_CONTEXT_MAX_CHARS: int = Field(default=22000, ge=4000, le=120000)
    FULL_FILE_REDUCE_TARGET_GROUPS: int = Field(default=8, ge=2, le=30)
    FULL_FILE_REDUCE_MAX_ROUNDS: int = Field(default=4, ge=1, le=12)
    CHAT_HISTORY_MAX_MESSAGES: int = Field(default=30, ge=4, le=500)
    INGESTION_BAD_CHUNK_RATIO_THRESHOLD: float = Field(default=0.35, ge=0.0, le=1.0)
    INGESTION_MAX_RETRIES: int = Field(default=3, ge=1, le=20)
    INGESTION_RETRY_BASE_SECONDS: float = Field(default=2.0, ge=0.1, le=300.0)
    INGESTION_RETRY_MAX_SECONDS: float = Field(default=60.0, ge=0.5, le=3600.0)
    INGESTION_WORKER_POLL_INTERVAL_SECONDS: float = Field(default=0.5, ge=0.05, le=30.0)
    INGESTION_WORKER_LEASE_SECONDS: float = Field(default=120.0, ge=5.0, le=86400.0)
    INGESTION_WORKER_HEARTBEAT_SECONDS: float = Field(default=5.0, ge=0.5, le=120.0)
    INGESTION_WORKER_SHUTDOWN_TIMEOUT_SECONDS: float = Field(default=15.0, ge=1.0, le=300.0)
    INGESTION_QUEUE_SQLITE_PATH: str = Field(default="uploads/.ingestion_jobs.sqlite3")
    TABULAR_RUNTIME_ROOT: str = Field(default="uploads/tabular_runtime/datasets")
    TABULAR_RUNTIME_CATALOG_PATH: str = Field(default="uploads/tabular_runtime/catalog.duckdb")
    TABULAR_SQL_TIMEOUT_SECONDS: float = Field(default=8.0, ge=0.5, le=120.0)
    TABULAR_SQL_MAX_RESULT_ROWS: int = Field(default=200, ge=10, le=10000)
    TABULAR_SQL_MAX_RESULT_BYTES: int = Field(default=200000, ge=1024, le=20000000)
    TABULAR_SQL_MAX_SCANNED_ROWS: int = Field(default=1000000, ge=100, le=100000000)
    TABULAR_SQL_MAX_CHARS: int = Field(default=4000, ge=200, le=50000)
    TABULAR_PROFILE_MAX_COLUMNS: int = Field(default=160, ge=1, le=5000)
    COMPLEX_ANALYTICS_TIMEOUT_SECONDS: float = Field(default=12.0, ge=1.0, le=300.0)
    COMPLEX_ANALYTICS_MAX_OUTPUT_CHARS: int = Field(default=16000, ge=500, le=200000)
    COMPLEX_ANALYTICS_MAX_ARTIFACTS: int = Field(default=4, ge=1, le=32)
    COMPLEX_ANALYTICS_MAX_ROWS: int = Field(default=200000, ge=1000, le=5000000)
    COMPLEX_ANALYTICS_ARTIFACT_DIR: str = Field(default="uploads/complex_analytics")
    COMPLEX_ANALYTICS_ARTIFACT_TTL_HOURS: int = Field(default=168, ge=1, le=87600)
    COMPLEX_ANALYTICS_ARTIFACT_MAX_RUN_DIRS: int = Field(default=2000, ge=10, le=200000)
    COMPLEX_ANALYTICS_CODEGEN_ENABLED: bool = Field(default=True)
    COMPLEX_ANALYTICS_CODEGEN_FORCE_LOCAL: bool = Field(default=False)
    COMPLEX_ANALYTICS_CODEGEN_PLAN_TIMEOUT_SECONDS: float = Field(default=6.0, ge=1.0, le=120.0)
    COMPLEX_ANALYTICS_CODEGEN_PLAN_TIMEOUT_SECONDS_AIHUB_POLICY: float = Field(default=14.0, ge=1.0, le=300.0)
    COMPLEX_ANALYTICS_CODEGEN_PLAN_MAX_TOKENS: int = Field(default=900, ge=128, le=8192)
    COMPLEX_ANALYTICS_CODEGEN_TIMEOUT_SECONDS: float = Field(default=8.0, ge=1.0, le=120.0)
    COMPLEX_ANALYTICS_CODEGEN_TIMEOUT_SECONDS_AIHUB_POLICY: float = Field(default=24.0, ge=1.0, le=300.0)
    COMPLEX_ANALYTICS_CODEGEN_MAX_TOKENS: int = Field(default=2200, ge=256, le=8192)
    COMPLEX_ANALYTICS_RESPONSE_TIMEOUT_SECONDS: float = Field(default=10.0, ge=1.0, le=120.0)
    COMPLEX_ANALYTICS_RESPONSE_TIMEOUT_SECONDS_AIHUB_POLICY: float = Field(default=20.0, ge=1.0, le=300.0)
    COMPLEX_ANALYTICS_RESPONSE_MAX_TOKENS: int = Field(default=1800, ge=256, le=4096)
    COMPLEX_ANALYTICS_PREFER_LOCAL_COMPOSER_FOR_BROAD_QUERY: bool = Field(default=True)
    COMPLEX_ANALYTICS_ALLOW_TEMPLATE_FALLBACK: bool = Field(default=True)
    COMPLEX_ANALYTICS_ALLOW_TEMPLATE_RUNTIME_FALLBACK: bool = Field(default=True)
    ENABLE_POST_ANSWER_SUMMARIZE: bool = Field(default=False)
    ENABLE_CACHE: bool = Field(default=True)
    MAX_FILESIZE_MB: int = Field(default=50, ge=1)

    # External APIs
    OPENAI_API_KEY: str = Field(default="")
    OPENAI_MODEL: str = Field(default="gpt-4")

    # Runtime
    LOG_LEVEL: str = Field(default="INFO")
    SERVER_HOST: str = Field(default="127.0.0.1")
    SERVER_PORT: int = Field(default=8000, ge=1, le=65535)
    DEFAULT_MODEL_SOURCE: str = Field(default="aihub")

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
            return "pdf,docx,txt,md,csv,json,xlsx,xls"
        parts = []
        for item in raw.split(","):
            ext = item.strip().lower().lstrip(".")
            if ext:
                parts.append(ext)
        return ",".join(parts) if parts else "pdf,docx,txt,md,csv,json,xlsx,xls"

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

    @property
    def llm_fallback_restricted_classes_set(self) -> set[str]:
        return {x.strip().lower() for x in self.LLM_FALLBACK_RESTRICTED_CLASSES.split(",") if x.strip()}


settings = Settings()

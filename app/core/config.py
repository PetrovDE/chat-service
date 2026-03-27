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
    supported_filetypes: str = Field(default="pdf,docx,txt,md,csv,tsv,json,xlsx,xls")

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
    AIHUB_EMBEDDING_MODEL: str = Field(default="qwen3-emb")
    AIHUB_EMBED_MODEL_CATALOG: str = Field(default="qwen3-emb,arctic")
    AIHUB_CHAT_MODEL_CATALOG: str = Field(default="")
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
    USER_FILE_QUOTA_BYTES: int = Field(default=1_073_741_824, ge=1)
    FILE_PIPELINE_VERSION_DEFAULT: str = Field(default="pipeline-v1")
    FILE_PARSER_VERSION_DEFAULT: str = Field(default="parser-v1")
    FILE_ARTIFACT_VERSION_DEFAULT: str = Field(default="artifact-v1")
    FILE_CHUNKING_STRATEGY_DEFAULT: str = Field(default="smart")
    FILE_RETRIEVAL_PROFILE_DEFAULT: str = Field(default="default")

    # Runtime storage layout (inside service folder)
    RUNTIME_ROOT: str = Field(default="runtime")
    RUNTIME_RAW_FILES_DIR: str = Field(default="runtime/raw_files")
    RUNTIME_TEMP_UPLOADS_DIR: str = Field(default="runtime/temp_uploads")
    RUNTIME_FILE_ARTIFACTS_DIR: str = Field(default="runtime/file_artifacts")
    RUNTIME_PUBLIC_UPLOADS_DIR: str = Field(default="runtime/public/uploads")
    RUNTIME_EXPORTS_DIR: str = Field(default="runtime/exports")
    RUNTIME_LOCAL_INDEX_DIR: str = Field(default="runtime/local_index")

    # VectorStore / RAG
    VECTORDB_PATH: str = Field(default="runtime/vector/chromadb")
    VECTORDB_EPHEMERAL_MODE: bool = Field(default=False)
    COLLECTION_NAME: str = Field(default="documents")
    EMBEDDINGS_MODEL: str = Field(default="nomic-embed-text:latest")
    OLLAMA_CHAT_MODEL: str = Field(default="llama3.2:latest")
    OLLAMA_EMBED_MODEL: str = Field(default="nomic-embed-text:latest")
    OLLAMA_EMBED_MODEL_CATALOG: str = Field(default="nomic-embed-text:latest,qwen3-emb,mxbai-embed-large")
    OLLAMA_CHAT_MODEL_CATALOG: str = Field(default="")
    OPENAI_EMBEDDING_MODEL: str = Field(default="text-embedding-3-small")
    MODEL_INVALID_OVERRIDE_POLICY: str = Field(default="fallback_default")
    EMBEDDING_PREFLIGHT_VALIDATE: bool = Field(default=True)
    OLLAMA_EMBED_MAX_INPUT_CHARS: int = Field(default=3500, ge=500, le=50000)
    OLLAMA_EMBED_SEGMENT_OVERLAP_CHARS: int = Field(default=250, ge=0, le=10000)
    EMBEDDING_MODEL_DIMENSIONS: str = Field(default="aihub:qwen3-emb=4096")
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
    TABULAR_ROW_GROUP_ROWS_NARROW: int = Field(default=200, ge=20, le=2000)
    TABULAR_ROW_GROUP_ROWS_MEDIUM: int = Field(default=100, ge=20, le=2000)
    TABULAR_ROW_GROUP_ROWS_WIDE: int = Field(default=50, ge=10, le=1000)
    TABULAR_ROW_GROUP_MEDIUM_COLUMNS_THRESHOLD: int = Field(default=12, ge=2, le=500)
    TABULAR_ROW_GROUP_WIDE_COLUMNS_THRESHOLD: int = Field(default=40, ge=5, le=2000)
    TABULAR_MAX_EMBEDDING_DOCS: int = Field(default=320, ge=16, le=5000)
    TABULAR_SUMMARY_TOP_COLUMNS: int = Field(default=12, ge=1, le=256)
    TABULAR_COLUMN_SUMMARY_ENABLED: bool = Field(default=True)
    TABULAR_COLUMN_SUMMARY_MAX_COLUMNS: int = Field(default=6, ge=1, le=128)
    TABULAR_WIDE_CELL_HARD_LIMIT: int = Field(default=2000, ge=200, le=50000)
    RAG_SCORE_THRESHOLD: float = Field(default=0.0, ge=0.0, le=1.0)
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
    INGESTION_QUEUE_SQLITE_PATH: str = Field(default="runtime/queue/.ingestion_jobs.sqlite3")
    TABULAR_RUNTIME_ROOT: str = Field(default="runtime/tabular_runtime/datasets")
    TABULAR_RUNTIME_CATALOG_PATH: str = Field(default="runtime/tabular_runtime/catalog.duckdb")
    TABULAR_SQL_TIMEOUT_SECONDS: float = Field(default=8.0, ge=0.5, le=120.0)
    TABULAR_SQL_MAX_RESULT_ROWS: int = Field(default=200, ge=10, le=10000)
    TABULAR_SQL_MAX_RESULT_BYTES: int = Field(default=200000, ge=1024, le=20000000)
    TABULAR_SQL_MAX_SCANNED_ROWS: int = Field(default=1000000, ge=100, le=100000000)
    TABULAR_SQL_MAX_CHARS: int = Field(default=4000, ge=200, le=50000)
    TABULAR_PROFILE_MAX_COLUMNS: int = Field(default=160, ge=1, le=5000)
    ANALYTICS_ENGINE_MODE: str = Field(default="langgraph")
    ANALYTICS_ENGINE_SHADOW: bool = Field(default=False)
    LANGSMITH_TRACING_ENABLED: bool = Field(default=False)
    LANGSMITH_PROJECT: str = Field(default="llama-service")
    LANGSMITH_TAGS: str = Field(default="tabular-langgraph")
    TABULAR_LLM_GUARDED_PLANNER_ENABLED: bool = Field(default=False)
    TABULAR_LLM_GUARDED_MAX_ATTEMPTS: int = Field(default=3, ge=1, le=5)
    TABULAR_LLM_GUARDED_PLAN_TIMEOUT_SECONDS: float = Field(default=5.0, ge=1.0, le=60.0)
    TABULAR_LLM_GUARDED_EXECUTION_TIMEOUT_SECONDS: float = Field(default=5.0, ge=1.0, le=60.0)
    TABULAR_LLM_GUARDED_PLAN_MAX_TOKENS: int = Field(default=800, ge=128, le=4096)
    TABULAR_LLM_GUARDED_EXECUTION_MAX_TOKENS: int = Field(default=700, ge=128, le=4096)
    COMPLEX_ANALYTICS_TIMEOUT_SECONDS: float = Field(default=12.0, ge=1.0, le=300.0)
    COMPLEX_ANALYTICS_MAX_OUTPUT_CHARS: int = Field(default=16000, ge=500, le=200000)
    COMPLEX_ANALYTICS_MAX_ARTIFACTS: int = Field(default=16, ge=1, le=128)
    COMPLEX_ANALYTICS_MAX_ARTIFACTS_HARD_CAP: int = Field(default=48, ge=1, le=256)
    COMPLEX_ANALYTICS_MAX_ROWS: int = Field(default=200000, ge=1000, le=5000000)
    COMPLEX_ANALYTICS_ARTIFACT_DIR: str = Field(default="runtime/public/uploads/complex_analytics")
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
    # Deprecated compatibility flags.
    # Stage-2 removed template codegen/runtime fallbacks from production execution path.
    # These settings are retained to avoid env/config breakage and are intentionally unused.
    COMPLEX_ANALYTICS_ALLOW_TEMPLATE_FALLBACK: bool = Field(default=False)
    COMPLEX_ANALYTICS_ALLOW_TEMPLATE_RUNTIME_FALLBACK: bool = Field(default=False)
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
            return "pdf,docx,txt,md,csv,tsv,json,xlsx,xls"
        parts = []
        for item in raw.split(","):
            ext = item.strip().lower().lstrip(".")
            if ext:
                parts.append(ext)
        return ",".join(parts) if parts else "pdf,docx,txt,md,csv,tsv,json,xlsx,xls"

    @field_validator("MODEL_INVALID_OVERRIDE_POLICY", mode="before")
    @classmethod
    def _normalize_invalid_override_policy(cls, value: str) -> str:
        normalized = str(value or "fallback_default").strip().lower()
        if normalized not in {"fallback_default", "error"}:
            return "fallback_default"
        return normalized

    @field_validator("ANALYTICS_ENGINE_MODE", mode="before")
    @classmethod
    def _normalize_analytics_engine_mode(cls, value: str) -> str:
        normalized = str(value or "langgraph").strip().lower()
        if normalized not in {"legacy", "langgraph"}:
            return "langgraph"
        return normalized

    @property
    def allowed_origins_list(self) -> list[str]:
        if self.allowed_origins.strip() == "*":
            return ["*"]
        return [x.strip() for x in self.allowed_origins.split(",") if x.strip()]

    @property
    def supported_filetypes_tuple(self) -> Tuple[str, ...]:
        return tuple(f".{x}" for x in self.supported_filetypes.split(",") if x)

    def get_service_root(self) -> Path:
        return Path(__file__).resolve().parents[2]

    def _resolve_runtime_path(self, path_value: str) -> Path:
        path = Path(str(path_value or "").strip() or ".").expanduser()
        if not path.is_absolute():
            path = (self.get_service_root() / path).resolve()
        return path

    def get_runtime_root(self) -> Path:
        path = self._resolve_runtime_path(self.RUNTIME_ROOT)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_raw_files_dir(self) -> Path:
        path = self._resolve_runtime_path(self.RUNTIME_RAW_FILES_DIR)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_temp_uploads_dir(self) -> Path:
        path = self._resolve_runtime_path(self.RUNTIME_TEMP_UPLOADS_DIR)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_file_artifacts_dir(self) -> Path:
        path = self._resolve_runtime_path(self.RUNTIME_FILE_ARTIFACTS_DIR)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_public_uploads_dir(self) -> Path:
        path = self._resolve_runtime_path(self.RUNTIME_PUBLIC_UPLOADS_DIR)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_exports_dir(self) -> Path:
        path = self._resolve_runtime_path(self.RUNTIME_EXPORTS_DIR)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_local_index_dir(self) -> Path:
        path = self._resolve_runtime_path(self.RUNTIME_LOCAL_INDEX_DIR)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_ingestion_queue_path(self) -> Path:
        path = self._resolve_runtime_path(self.INGESTION_QUEUE_SQLITE_PATH)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def get_tabular_runtime_root(self) -> Path:
        path = self._resolve_runtime_path(self.TABULAR_RUNTIME_ROOT)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_tabular_runtime_catalog_path(self) -> Path:
        configured = str(self.TABULAR_RUNTIME_CATALOG_PATH or "").strip()
        if configured:
            path = self._resolve_runtime_path(configured)
        else:
            path = (self.get_tabular_runtime_root().parent / "catalog.duckdb").resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def get_complex_analytics_artifact_dir(self) -> Path:
        configured = str(self.COMPLEX_ANALYTICS_ARTIFACT_DIR or "").strip()
        if configured:
            path = self._resolve_runtime_path(configured)
        else:
            path = (self.get_public_uploads_dir() / "complex_analytics").resolve()
        path.mkdir(parents=True, exist_ok=True)
        return path

    def ensure_runtime_directories(self) -> dict[str, Path]:
        return {
            "runtime_root": self.get_runtime_root(),
            "raw_files": self.get_raw_files_dir(),
            "temp_uploads": self.get_temp_uploads_dir(),
            "file_artifacts": self.get_file_artifacts_dir(),
            "public_uploads": self.get_public_uploads_dir(),
            "exports": self.get_exports_dir(),
            "local_index": self.get_local_index_dir(),
            "vectordb": self.get_vectordb_path(),
            "ingestion_queue": self.get_ingestion_queue_path(),
            "tabular_runtime_root": self.get_tabular_runtime_root(),
            "tabular_runtime_catalog": self.get_tabular_runtime_catalog_path(),
            "complex_analytics_artifacts": self.get_complex_analytics_artifact_dir(),
        }

    def get_vectordb_path(self) -> Path:
        path = self._resolve_runtime_path(self.VECTORDB_PATH)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def is_file_supported(self, filename: str) -> bool:
        return filename.lower().endswith(self.supported_filetypes_tuple)

    @property
    def llm_fallback_restricted_classes_set(self) -> set[str]:
        return {x.strip().lower() for x in self.LLM_FALLBACK_RESTRICTED_CLASSES.split(",") if x.strip()}


settings = Settings()

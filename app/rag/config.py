# app/rag/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class RAGConfig(BaseSettings):
    """Конфигурация RAG системы"""

    # Конфигурация Pydantic v2
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_prefix="RAG_"  # Все RAG настройки будут с префиксом RAG_
    )

    # === Vector Store Settings ===
    vector_db_path: str = "./chroma_db"
    collection_name: str = "documents"

    # === Embeddings Settings ===
    embeddings_model: str = "llama3.1:8b"
    embeddings_base_url: str = "http://localhost:11434"

    # === Text Splitting Settings ===
    chunk_size: int = 1000
    chunk_overlap: int = 200

    # === Retrieval Settings ===
    top_k: int = 3
    similarity_threshold: float = 0.7

    # === Document Processing Settings ===
    max_file_size_mb: int = 50

    # === RAG Generation Settings ===
    use_context_compression: bool = True
    max_context_length: int = 3000
    include_metadata: bool = True

    # === Caching ===
    enable_cache: bool = True
    cache_ttl_hours: int = 24

    def get_vector_db_path(self) -> Path:
        """Получить Path объект для векторной БД"""
        path = Path(self.vector_db_path)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def is_file_supported(self, filename: str) -> bool:
        """Проверить, поддерживается ли файл"""
        supported = {".txt", ".pdf", ".docx", ".doc", ".xlsx", ".xls", ".csv", ".md", ".json"}
        ext = Path(filename).suffix.lower()
        return ext in supported

    def validate_file_size(self, size_bytes: int) -> bool:
        """Проверить размер файла"""
        max_bytes = self.max_file_size_mb * 1024 * 1024
        return size_bytes <= max_bytes


# Глобальный экземпляр конфигурации
rag_config = RAGConfig()
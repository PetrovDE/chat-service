# app/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List
from functools import lru_cache


class Settings(BaseSettings):
    """Настройки приложения"""
    
    # Конфигурация Pydantic v2
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"  # Игнорировать дополнительные поля из .env
    )
    
    # Database
    DATABASE_URL: str
    ALEMBIC_DATABASE_URL: str | None = None
    
    # Ollama
    OLLAMA_HOST: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "llama3.1:8b"
    
    # OpenAI (optional)
    OPENAI_API_KEY: str = ""
    OPENAI_MODEL: str = "gpt-4"
    
    # JWT Authentication
    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 10080  # 7 days
    
    # Password
    PASSWORD_MIN_LENGTH: int = 8
    
    # CORS (stored as string, parsed to list)
    ALLOWED_ORIGINS: str = "http://localhost:8000,http://127.0.0.1:8000"
    
    # Model settings
    DEFAULT_MODEL_SOURCE: str = "ollama"
    
    # Server
    SERVER_HOST: str = "127.0.0.1"
    SERVER_PORT: int = 8000
    LOG_LEVEL: str = "INFO"
    
    def get_allowed_origins(self) -> List[str]:
        """Parse ALLOWED_ORIGINS string to list"""
        return [origin.strip() for origin in self.ALLOWED_ORIGINS.split(",")]
    
    def get_alembic_url(self) -> str:
        """Get Alembic database URL (sync version)"""
        if self.ALEMBIC_DATABASE_URL:
            return self.ALEMBIC_DATABASE_URL
        # Convert async URL to sync
        return self.DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance"""
    return Settings()


# Global settings instance
settings = get_settings()
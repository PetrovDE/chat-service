"""
LLM Manager with Provider Architecture
Менеджер для работы с различными провайдерами LLM
"""
import logging
from typing import Optional, Dict, Any, List, AsyncGenerator

from app.core.config import settings
from app.services.llm.providers.base import BaseLLMProvider
from app.services.llm.providers.ollama import ollama_provider
from app.services.llm.providers.openai import openai_provider
from app.services.llm.providers.aihub import aihub_provider

logger = logging.getLogger(__name__)


class LLMManager:
    """
    Unified LLM Manager with Provider Architecture
    Поддерживает: Ollama (local), OpenAI, AI HUB
    """

    def __init__(self):
        self.default_source = settings.DEFAULT_MODEL_SOURCE

        # Регистрация провайдеров
        self.providers: Dict[str, BaseLLMProvider] = {
            "ollama": ollama_provider,
            "local": ollama_provider,  # Алиас для ollama
            "openai": openai_provider,
            "aihub": aihub_provider,
        }

        logger.info(f"🚀 LLMManager initialized with providers: {list(self.providers.keys())}")
        logger.info(f"📌 Default source: {self.default_source}")

    def _get_provider(self, source: str) -> BaseLLMProvider:
        """Получить провайдер по имени"""
        provider = self.providers.get(source)
        if not provider:
            raise ValueError(f"Unknown model source: {source}. Available: {list(self.providers.keys())}")
        return provider

    async def get_available_models(self, source: str = "ollama") -> List[str]:
        """Получить список доступных моделей от провайдера"""
        try:
            provider = self._get_provider(source)
            models = await provider.get_available_models()
            logger.info(f"📋 Models from {source}: {models}")
            return models
        except Exception as e:
            logger.error(f"❌ Failed to fetch models from {source}: {e}")
            return []

    async def generate_response(
            self,
            prompt: str,
            model_source: Optional[str] = None,
            model_name: Optional[str] = None,
            temperature: float = 0.7,
            max_tokens: int = 2000,
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Генерировать полный ответ (без стриминга)"""
        source = model_source or self.default_source
        provider = self._get_provider(source)

        # Определяем модель
        if not model_name:
            if source == "ollama" or source == "local":
                # FIX: чат-модель != embedding-модель
                model_name = settings.OLLAMA_CHAT_MODEL or settings.EMBEDDINGS_MODEL
            elif source == "openai":
                model_name = settings.OPENAI_MODEL
            elif source == "aihub":
                model_name = settings.AIHUB_DEFAULT_MODEL

        logger.info(f"🔧 Generating response: source={source}, model={model_name}")

        return await provider.generate_response(
            prompt=prompt,
            model=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            conversation_history=conversation_history
        )

    async def generate_response_stream(
            self,
            prompt: str,
            model_source: Optional[str] = None,
            model_name: Optional[str] = None,
            temperature: float = 0.7,
            max_tokens: int = 2000,
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> AsyncGenerator[str, None]:
        """Генерировать ответ со стримингом"""
        source = model_source or self.default_source
        provider = self._get_provider(source)

        # Определяем модель
        if not model_name:
            if source == "ollama" or source == "local":
                # FIX: чат-модель != embedding-модель
                model_name = settings.OLLAMA_CHAT_MODEL or settings.EMBEDDINGS_MODEL
            elif source == "openai":
                model_name = settings.OPENAI_MODEL
            elif source == "aihub":
                model_name = settings.AIHUB_DEFAULT_MODEL

        logger.info(f"🔧 Streaming response: source={source}, model={model_name}")

        async for chunk in provider.generate_response_stream(
            prompt=prompt,
            model=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            conversation_history=conversation_history
        ):
            yield chunk

    async def generate_embedding(
            self,
            text: str,
            model_source: Optional[str] = None,
            model_name: Optional[str] = None
    ) -> Optional[List[float]]:
        """Генерировать эмбеддинг"""
        source = model_source or self.default_source
        provider = self._get_provider(source)

        # Определяем модель для эмбеддингов
        if not model_name:
            if source == "aihub":
                model_name = settings.AIHUB_EMBEDDING_MODEL
            elif source == "ollama" or source == "local":
                # FIX: раньше тут оставалось None → и это ломало retrieval
                model_name = settings.OLLAMA_EMBED_MODEL or settings.EMBEDDINGS_MODEL

        logger.info(f"🔮 Generating embedding: source={source}, model={model_name}")

        return await provider.generate_embedding(
            text=text,
            model=model_name
        )


# Create singleton instance
llm_manager = LLMManager()

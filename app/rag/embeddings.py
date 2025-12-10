"""
Embeddings Manager
Менеджер для генерации эмбеддингов через различные провайдеры
"""
import logging
from typing import List, Optional

from app.core.config import settings
from app.services.llm.manager import llm_manager

logger = logging.getLogger(__name__)


class EmbeddingsManager:
    """
    Менеджер эмбеддингов с поддержкой различных провайдеров
    Поддерживает: local (Ollama), corporate/aihub (AI HUB)
    """

    # Маппинг режимов на размерности эмбеддингов
    DEFAULT_DIMENSIONS = {
        "local": 4096,  # По умолчанию для Ollama
        "aihub": 1024,  # arctic модель
    }

    def __init__(
        self,
        mode: str = "local",
        model: Optional[str] = None,
        ollama_url: Optional[str] = None,
        hub_url: Optional[str] = None,
        keycloak_token: Optional[str] = None,
        system_user: Optional[str] = None
    ):
        """
        Инициализация менеджера эмбеддингов

        Args:
            mode: Режим работы ('local' или 'corporate')
            model: Модель для эмбеддингов (если не указана, используется из настроек)
            ollama_url: URL Ollama (для обратной совместимости)
            hub_url: URL HUB (для обратной совместимости)
            keycloak_token: Токен Keycloak (для обратной совместимости)
            system_user: System user (для обратной совместимости)
        """
        # Нормализация режима: corporate -> aihub (для внутреннего использования)
        self.mode = "aihub" if mode == "corporate" else mode
        self.original_mode = mode  # Сохраняем оригинальное название
        self.model = model

        # Для обратной совместимости со старым кодом
        self.ollama_url = ollama_url or settings.EMBEDDINGS_BASEURL
        self.hub_url = hub_url or settings.CORPORATE_API_URL
        self.keycloak_token = keycloak_token or settings.CORPORATE_API_TOKEN
        self.system_user = system_user or settings.CORPORATE_API_USERNAME

        logger.info(
            f"🚀 EmbeddingsManager initialized: mode={self.original_mode} "
            f"(internal: {self.mode}), model={self.model}"
        )

    def switch_mode(self, mode: str):
        """
        Переключить режим работы

        Args:
            mode: Новый режим ('local' или 'corporate')
        """
        if mode not in ["local", "corporate", "aihub"]:
            raise ValueError(f"Incorrect mode: must be 'local' or 'corporate', got '{mode}'")

        self.original_mode = mode
        self.mode = "aihub" if mode == "corporate" else mode
        logger.info(f"🔄 Switched embeddings mode to: {self.original_mode} (internal: {self.mode})")

    def switch_model(self, model: str):
        """Переключить модель"""
        self.model = model
        logger.info(f"🔄 Switched embeddings model to: {model}")

    def update_token(self, keycloak_token: str):
        """Обновить токен (для обратной совместимости)"""
        self.keycloak_token = keycloak_token
        logger.info("🔑 Updated Keycloak token")

    async def get_available_models(self) -> List[str]:
        """Получить список доступных моделей"""
        try:
            models = await llm_manager.get_available_models(source=self.mode)
            logger.info(f"📋 Available models for {self.original_mode}: {models}")
            return models
        except Exception as e:
            logger.error(f"❌ Failed to get available models: {e}")
            return []

    def get_embedding_dimension(self) -> int:
        """
        Получить ожидаемую размерность эмбеддингов для текущего режима

        Returns:
            Размерность вектора эмбеддинга
        """
        return self.DEFAULT_DIMENSIONS.get(self.mode, 1024)

    def embedd_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Получить эмбеддинги для списка текстов (синхронная версия для совместимости)

        Args:
            texts: Список текстов для эмбеддинга

        Returns:
            List[List[float]]: Список векторов эмбеддингов
        """
        import asyncio

        # Проверяем, есть ли уже running event loop
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Нет running loop - создаем новый
            return asyncio.run(self.embedd_documents_async(texts))
        else:
            # Есть running loop - используем nest_asyncio
            logger.warning("⚠️ embedd_documents called from async context, consider using embedd_documents_async")
            try:
                import nest_asyncio
                nest_asyncio.apply()
            except ImportError:
                logger.error("❌ nest_asyncio not installed, cannot run async code from sync context")
                raise RuntimeError("Please install nest_asyncio or use embedd_documents_async")
            return asyncio.run(self.embedd_documents_async(texts))

    async def embedd_documents_async(self, texts: List[str]) -> List[List[float]]:
        """
        Получить эмбеддинги для списка текстов (асинхронная версия)

        Args:
            texts: Список текстов для эмбеддинга

        Returns:
            List[List[float]]: Список векторов эмбеддингов
        """
        if not texts:
            logger.warning("⚠️ Empty texts list provided")
            return []

        # Определяем модель для эмбеддингов
        # Для AI HUB всегда используем "arctic", для остальных - текущую модель
        embedding_model = "arctic" if self.mode == "aihub" else self.model
        expected_dim = self.get_embedding_dimension()

        logger.info(
            f"🔮 Generating embeddings for {len(texts)} texts using {self.original_mode}, "
            f"model: {embedding_model}, expected dimension: {expected_dim}"
        )

        all_embeddings = []

        for idx, text in enumerate(texts):
            try:
                logger.debug(f"🔌 Requesting embedding {idx+1}/{len(texts)} ({len(text)} chars)")

                # Используем новую архитектуру провайдеров
                embedding = await llm_manager.generate_embedding(
                    text=text,
                    model_source=self.mode,  # Используем внутреннее имя (aihub)
                    model_name=embedding_model  # Для aihub всегда "arctic"
                )

                if not embedding or len(embedding) == 0:
                    logger.error(f"❌ Empty embedding returned for text {idx+1}")
                    raise RuntimeError(f"Empty embedding returned for text {idx+1}")

                # Проверка размерности
                actual_dim = len(embedding)
                if actual_dim != expected_dim:
                    logger.warning(
                        f"⚠️ Unexpected embedding dimension: expected {expected_dim}, "
                        f"got {actual_dim} for text {idx+1}"
                    )

                all_embeddings.append(embedding)
                logger.debug(f"✅ Embedding {idx+1} received: {actual_dim} dimensions")

            except Exception as e:
                logger.error(f"❌ Failed to generate embedding for text {idx+1}: {e}")
                raise RuntimeError(f"Embedding generation failed for text {idx+1}: {e}")

        logger.info(
            f"✅ Generated {len(all_embeddings)} embeddings successfully "
            f"(dimension: {len(all_embeddings[0]) if all_embeddings else 'N/A'})"
        )
        return all_embeddings


# Singleton instance
embeddings_manager = EmbeddingsManager()

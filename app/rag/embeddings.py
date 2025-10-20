# app/rag/embeddings.py
import logging
from typing import List
from langchain_ollama import OllamaEmbeddings
from app.rag.config import rag_config

logger = logging.getLogger(__name__)


class OllamaEmbeddingsManager:
    """
    Менеджер для создания векторных представлений (embeddings) через Ollama
    Использует локальную модель Llama для генерации embeddings
    """

    def __init__(
            self,
            model: str = None,
            base_url: str = None
    ):
        """
        Инициализация менеджера embeddings

        Args:
            model: Название модели Ollama (по умолчанию из config)
            base_url: URL Ollama сервера (по умолчанию из config)
        """
        self.model = model or rag_config.embeddings_model
        self.base_url = base_url or rag_config.embeddings_base_url

        # Инициализация Ollama Embeddings
        try:
            self.embeddings = OllamaEmbeddings(
                model=self.model,
                base_url=self.base_url
            )
            logger.info(f"✅ OllamaEmbeddings initialized: model={self.model}, base_url={self.base_url}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize OllamaEmbeddings: {e}")
            raise

        self._embedding_cache = {} if rag_config.enable_cache else None

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Создать embeddings для списка документов

        Args:
            texts: Список текстов для векторизации

        Returns:
            Список векторов (каждый вектор - список float)
        """
        try:
            if not texts:
                logger.warning("⚠️ Empty text list provided for embedding")
                return []

            logger.info(f"🔄 Creating embeddings for {len(texts)} documents...")

            # Проверка кэша
            if self._embedding_cache is not None:
                cached_results = []
                uncached_texts = []
                uncached_indices = []

                for i, text in enumerate(texts):
                    cache_key = hash(text)
                    if cache_key in self._embedding_cache:
                        cached_results.append((i, self._embedding_cache[cache_key]))
                    else:
                        uncached_texts.append(text)
                        uncached_indices.append(i)

                if cached_results:
                    logger.info(f"📦 Found {len(cached_results)} embeddings in cache")

                # Создать embeddings только для некэшированных текстов
                if uncached_texts:
                    new_embeddings = self.embeddings.embed_documents(uncached_texts)

                    # Сохранить в кэш
                    for text, embedding in zip(uncached_texts, new_embeddings):
                        self._embedding_cache[hash(text)] = embedding

                    # Объединить результаты
                    all_embeddings = [None] * len(texts)
                    for i, embedding in cached_results:
                        all_embeddings[i] = embedding
                    for i, embedding in zip(uncached_indices, new_embeddings):
                        all_embeddings[i] = embedding

                    return all_embeddings
                else:
                    # Все из кэша
                    return [emb for _, emb in sorted(cached_results)]

            # Без кэша - просто создаем embeddings
            embeddings = self.embeddings.embed_documents(texts)
            logger.info(f"✅ Created {len(embeddings)} embeddings successfully")
            return embeddings

        except Exception as e:
            logger.error(f"❌ Error creating embeddings: {e}")
            raise

    def embed_query(self, text: str) -> List[float]:
        """
        Создать embedding для поискового запроса

        Args:
            text: Текст запроса

        Returns:
            Вектор запроса (список float)
        """
        try:
            if not text or not text.strip():
                logger.warning("⚠️ Empty query text provided")
                return []

            logger.debug(f"🔍 Creating embedding for query: {text[:50]}...")

            # Проверка кэша
            if self._embedding_cache is not None:
                cache_key = hash(text)
                if cache_key in self._embedding_cache:
                    logger.debug("📦 Query embedding found in cache")
                    return self._embedding_cache[cache_key]

            # Создать embedding
            embedding = self.embeddings.embed_query(text)

            # Сохранить в кэш
            if self._embedding_cache is not None:
                self._embedding_cache[hash(text)] = embedding

            logger.debug(f"✅ Query embedding created (dimension: {len(embedding)})")
            return embedding

        except Exception as e:
            logger.error(f"❌ Error creating query embedding: {e}")
            raise

    def get_embedding_dimension(self) -> int:
        """
        Получить размерность векторов для текущей модели

        Returns:
            Размерность embedding вектора
        """
        try:
            # Создать тестовый embedding для определения размерности
            test_embedding = self.embed_query("test")
            dimension = len(test_embedding)
            logger.info(f"📏 Embedding dimension: {dimension}")
            return dimension
        except Exception as e:
            logger.error(f"❌ Error getting embedding dimension: {e}")
            # Для llama3.1:8b размерность обычно 4096
            return 4096

    def clear_cache(self):
        """Очистить кэш embeddings"""
        if self._embedding_cache is not None:
            cache_size = len(self._embedding_cache)
            self._embedding_cache.clear()
            logger.info(f"🗑️ Cleared embedding cache ({cache_size} items)")

    def get_cache_size(self) -> int:
        """Получить размер кэша"""
        if self._embedding_cache is not None:
            return len(self._embedding_cache)
        return 0

    def is_available(self) -> bool:
        """
        Проверить доступность Ollama сервера

        Returns:
            True если сервер доступен
        """
        try:
            # Попытка создать простой embedding
            self.embed_query("test")
            logger.info("✅ Ollama embeddings service is available")
            return True
        except Exception as e:
            logger.error(f"❌ Ollama embeddings service unavailable: {e}")
            return False
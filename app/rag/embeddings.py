"""
Embeddings Manager
Менеджер для генерации эмбеддингов через различные провайдеры
"""
import logging
import asyncio
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
        "local": 0,  # FIX: dimension is model-specific; 0 = auto (no strict check)
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
        self.original_mode = mode
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
        """Переключить режим работы"""
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
        0 = не проверяем строго (auto)
        """
        return self.DEFAULT_DIMENSIONS.get(self.mode, 0)

    def embedd_documents(self, texts: List[str]) -> List[List[float]]:
        """Синхронная обертка (для совместимости)"""
        import asyncio

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.embedd_documents_async(texts))
        else:
            logger.warning("⚠️ embedd_documents called from async context, consider using embedd_documents_async")
            try:
                import nest_asyncio
                nest_asyncio.apply()
            except ImportError:
                logger.error("❌ nest_asyncio not installed, cannot run async code from sync context")
                raise RuntimeError("Please install nest_asyncio or use embedd_documents_async")
            return asyncio.run(self.embedd_documents_async(texts))

    async def embedd_documents_async(self, texts: List[str]) -> List[List[float]]:
        """Асинхронная генерация эмбеддингов"""
        if not texts:
            logger.warning("⚠️ Empty texts list provided")
            return []

        # FIX: embedding_model никогда не должен быть None
        if self.mode == "aihub":
            embedding_model = self.model or settings.AIHUB_EMBEDDING_MODEL or settings.EMBEDDINGS_MODEL
        else:
            embedding_model = self.model or settings.OLLAMA_EMBED_MODEL or settings.EMBEDDINGS_MODEL

        # FIX: если EMBEDDINGS_DIM=0 — не проверяем строго
        expected_dim = settings.EMBEDDINGS_DIM or self.get_embedding_dimension()

        logger.info(
            f"🔮 Generating embeddings for {len(texts)} texts using {self.original_mode}, "
            f"model: {embedding_model}, expected dimension: {expected_dim}"
        )

        local_max_chars = int(getattr(settings, "OLLAMA_EMBED_MAX_INPUT_CHARS", 3500) or 3500)
        local_overlap_chars = int(getattr(settings, "OLLAMA_EMBED_SEGMENT_OVERLAP_CHARS", 250) or 250)

        def _split_for_embedding(text: str, max_chars: int, overlap_chars: int) -> List[str]:
            raw = str(text or "")
            if len(raw) <= max_chars:
                return [raw]
            overlap = max(0, min(overlap_chars, max_chars - 1))
            step = max(1, max_chars - overlap)
            out: List[str] = []
            start = 0
            while start < len(raw):
                end = min(start + max_chars, len(raw))
                seg = raw[start:end]
                if seg.strip():
                    out.append(seg)
                if end >= len(raw):
                    break
                start += step
            return out or [raw[:max_chars]]

        def _mean_pool(vectors: List[List[float]]) -> List[float]:
            if not vectors:
                return []
            dim = len(vectors[0])
            total = [0.0] * dim
            for vec in vectors:
                if len(vec) != dim:
                    raise RuntimeError("Inconsistent embedding dimensions across segments")
                for i, value in enumerate(vec):
                    total[i] += float(value)
            n = float(len(vectors))
            return [value / n for value in total]

        all_embeddings = []

        concurrency = settings.AIHUB_EMBEDDING_CONCURRENCY if self.mode == "aihub" else settings.EMBEDDING_CONCURRENCY
        semaphore = asyncio.Semaphore(max(1, int(concurrency)))
        results: List[Optional[List[float]]] = [None] * len(texts)
        segmented_inputs = 0
        segment_calls_total = 0

        async def _embed_one(idx: int, text: str) -> None:
            nonlocal segmented_inputs, segment_calls_total
            source_text = str(text or "")
            segments = [source_text]
            if self.mode in ("local", "ollama"):
                segments = _split_for_embedding(source_text, local_max_chars, local_overlap_chars)

            if len(segments) > 1:
                segmented_inputs += 1
                segment_calls_total += len(segments)
                segment_vectors: List[List[float]] = []
                for seg in segments:
                    async with semaphore:
                        seg_embedding = await llm_manager.generate_embedding(
                            text=seg,
                            model_source=self.mode,
                            model_name=embedding_model
                        )
                    if not seg_embedding or len(seg_embedding) == 0:
                        raise RuntimeError(f"Empty embedding returned for text {idx+1} segment")
                    segment_vectors.append(seg_embedding)
                embedding = _mean_pool(segment_vectors)
            else:
                async with semaphore:
                    embedding = await llm_manager.generate_embedding(
                        text=source_text,
                        model_source=self.mode,
                        model_name=embedding_model
                    )
                if not embedding or len(embedding) == 0:
                    raise RuntimeError(f"Empty embedding returned for text {idx+1}")

            actual_dim = len(embedding)
            if expected_dim and actual_dim != expected_dim:
                logger.warning(
                    f"⚠️ Unexpected embedding dimension: expected {expected_dim}, "
                    f"got {actual_dim} for text {idx+1}"
                )
            results[idx] = embedding

        try:
            await asyncio.gather(*[_embed_one(i, t) for i, t in enumerate(texts)])
        except Exception as e:
            logger.error(f"❌ Failed to generate embeddings batch: {e}")
            raise RuntimeError(f"Embedding generation failed: {e}")

        if segmented_inputs > 0:
            logger.info(
                "🔀 Segmented %d/%d embedding inputs into %d local calls (max_chars=%d overlap=%d)",
                segmented_inputs,
                len(texts),
                segment_calls_total,
                local_max_chars,
                local_overlap_chars,
            )

        for emb in results:
            if emb is None:
                raise RuntimeError("Embedding generation failed: missing result entry")
            all_embeddings.append(emb)

        logger.info(
            f"✅ Generated {len(all_embeddings)} embeddings successfully "
            f"(dimension: {len(all_embeddings[0]) if all_embeddings else 'N/A'})"
        )
        return all_embeddings


# Singleton instance
embeddings_manager = EmbeddingsManager()

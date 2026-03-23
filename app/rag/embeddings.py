"""
Embeddings manager used by ingestion and retrieval flows.
"""

from __future__ import annotations

import asyncio
import logging
from typing import List, Optional

from app.core.config import settings
from app.services.llm.manager import llm_manager

logger = logging.getLogger(__name__)


class EmbeddingsManager:
    def __init__(
        self,
        mode: str = "local",
        model: Optional[str] = None,
        ollama_url: Optional[str] = None,
        hub_url: Optional[str] = None,
        keycloak_token: Optional[str] = None,
        system_user: Optional[str] = None,
    ):
        normalized_mode = str(mode or "local").strip().lower()
        if normalized_mode == "corporate":
            normalized_mode = "aihub"
        if normalized_mode == "ollama":
            normalized_mode = "local"
        if normalized_mode not in {"local", "aihub", "openai"}:
            raise ValueError(f"Unsupported embeddings mode: {mode}")

        self.mode = normalized_mode
        self.original_mode = mode
        self.model = model

        # Backward-compatible fields used by some callers/tests.
        self.ollama_url = ollama_url or settings.EMBEDDINGS_BASEURL
        self.hub_url = hub_url or settings.CORPORATE_API_URL
        self.keycloak_token = keycloak_token or settings.CORPORATE_API_TOKEN
        self.system_user = system_user or settings.CORPORATE_API_USERNAME

        logger.info("EmbeddingsManager initialized: requested_mode=%s mode=%s model=%s", mode, self.mode, model)

    def _provider_source(self) -> str:
        if self.mode == "aihub":
            return "aihub"
        if self.mode == "openai":
            return "openai"
        return "ollama"

    def switch_mode(self, mode: str):
        normalized_mode = str(mode or "local").strip().lower()
        if normalized_mode == "corporate":
            normalized_mode = "aihub"
        if normalized_mode == "ollama":
            normalized_mode = "local"
        if normalized_mode not in {"local", "aihub", "openai"}:
            raise ValueError(f"Unsupported embeddings mode: {mode}")
        self.original_mode = mode
        self.mode = normalized_mode
        logger.info("Embeddings mode switched: requested_mode=%s mode=%s", mode, self.mode)

    def switch_model(self, model: str):
        self.model = model
        logger.info("Embeddings model switched: model=%s", model)

    def update_token(self, keycloak_token: str):
        self.keycloak_token = keycloak_token
        logger.info("Embeddings token updated")

    async def get_available_models(self) -> List[str]:
        try:
            models = await llm_manager.get_available_models(source=self._provider_source())
            logger.info("Available embedding models mode=%s count=%d", self.mode, len(models))
            return models
        except Exception as exc:
            logger.error("Failed to get embedding models mode=%s error=%s", self.mode, exc, exc_info=True)
            return []

    def embedd_documents(self, texts: List[str]) -> List[List[float]]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.embedd_documents_async(texts))
        logger.warning("embedd_documents called in async context; use embedd_documents_async instead")
        try:
            import nest_asyncio

            nest_asyncio.apply()
        except Exception as exc:
            raise RuntimeError("Install nest_asyncio or use embedd_documents_async") from exc
        return asyncio.run(self.embedd_documents_async(texts))

    async def embedd_documents_async(self, texts: List[str]) -> List[List[float]]:
        if not texts:
            return []

        provider_source = self._provider_source()
        decision = llm_manager.provider_registry.resolve_embedding_model_decision(provider_source, self.model)
        embedding_model = decision.resolved_model

        if not str(embedding_model or "").strip():
            raise ValueError(f"Embedding model is not configured for mode '{self.mode}'")

        dim_decision = llm_manager.provider_registry.resolve_embedding_dimension_decision(
            provider_source,
            embedding_model,
        )
        expected_dim: Optional[int] = dim_decision.dimension if int(dim_decision.dimension or 0) > 0 else None
        expected_dim_source = dim_decision.source
        expected_dim_reason = dim_decision.reason
        logger.info(
            (
                "Embedding batch started: mode=%s provider=%s requested_model=%s resolved_model=%s "
                "resolution_source=%s reason=%s texts=%d expected_dim=%s expected_dim_source=%s expected_dim_reason=%s"
            ),
            self.mode,
            provider_source,
            self.model,
            embedding_model,
            decision.source,
            decision.reason,
            len(texts),
            expected_dim,
            expected_dim_source,
            expected_dim_reason,
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
                for idx, value in enumerate(vec):
                    total[idx] += float(value)
            factor = float(len(vectors))
            return [value / factor for value in total]

        results: List[Optional[List[float]]] = [None] * len(texts)
        segmented_inputs = 0
        segment_calls_total = 0
        concurrency = settings.AIHUB_EMBEDDING_CONCURRENCY if self.mode == "aihub" else settings.EMBEDDING_CONCURRENCY
        semaphore = asyncio.Semaphore(max(1, int(concurrency)))
        expected_dim_lock = asyncio.Lock()

        async def _embed_one(idx: int, text: str) -> None:
            nonlocal segmented_inputs, segment_calls_total, expected_dim, expected_dim_source, expected_dim_reason
            source_text = str(text or "")
            segments = [source_text]
            if self.mode == "local":
                segments = _split_for_embedding(source_text, local_max_chars, local_overlap_chars)

            if len(segments) > 1:
                segmented_inputs += 1
                segment_calls_total += len(segments)
                segment_vectors: List[List[float]] = []
                for seg in segments:
                    async with semaphore:
                        segment_embedding = await llm_manager.generate_embedding(
                            text=seg,
                            model_source=provider_source,
                            model_name=embedding_model,
                        )
                    if not segment_embedding:
                        raise RuntimeError(f"Empty embedding returned for text index={idx} segment")
                    segment_vectors.append(segment_embedding)
                embedding = _mean_pool(segment_vectors)
            else:
                async with semaphore:
                    embedding = await llm_manager.generate_embedding(
                        text=source_text,
                        model_source=provider_source,
                        model_name=embedding_model,
                    )
                if not embedding:
                    raise RuntimeError(f"Empty embedding returned for text index={idx}")

            actual_dim = len(embedding)
            if actual_dim <= 0:
                raise RuntimeError(f"Invalid embedding dimension: provider={provider_source} model={embedding_model} idx={idx}")

            mismatch_error: Optional[str] = None
            async with expected_dim_lock:
                if expected_dim is None:
                    runtime_dim = llm_manager.provider_registry.register_runtime_embedding_dimension(
                        provider_source,
                        embedding_model,
                        actual_dim,
                    )
                    expected_dim = int(runtime_dim.dimension or actual_dim)
                    expected_dim_source = runtime_dim.source
                    expected_dim_reason = runtime_dim.reason
                    logger.info(
                        (
                            "Embedding dimension learned at runtime: mode=%s provider=%s model=%s "
                            "expected_dim=%d source=%s reason=%s idx=%d"
                        ),
                        self.mode,
                        provider_source,
                        embedding_model,
                        expected_dim,
                        expected_dim_source,
                        expected_dim_reason,
                        idx,
                    )
                elif actual_dim != expected_dim:
                    mismatch_error = (
                        "Embedding dimension mismatch: "
                        f"mode={self.mode} provider={provider_source} model={embedding_model} "
                        f"expected={expected_dim} actual={actual_dim} idx={idx} "
                        f"source={expected_dim_source} reason={expected_dim_reason}"
                    )
            if mismatch_error is not None:
                logger.error(mismatch_error)
                raise RuntimeError(mismatch_error)
            results[idx] = embedding

        try:
            await asyncio.gather(*[_embed_one(i, t) for i, t in enumerate(texts)])
        except Exception as exc:
            logger.error(
                "Embedding batch failed: mode=%s model=%s texts=%d error=%s",
                self.mode,
                embedding_model,
                len(texts),
                exc,
                exc_info=True,
            )
            raise

        if segmented_inputs > 0:
            logger.info(
                (
                    "Embedding segmentation applied: mode=%s segmented_inputs=%d total_inputs=%d "
                    "segment_calls=%d max_chars=%d overlap=%d"
                ),
                self.mode,
                segmented_inputs,
                len(texts),
                segment_calls_total,
                local_max_chars,
                local_overlap_chars,
            )

        output: List[List[float]] = []
        for emb in results:
            if emb is None:
                raise RuntimeError("Embedding generation failed: missing result entry")
            output.append(emb)

        logger.info(
            "Embedding batch completed: mode=%s provider=%s model=%s texts=%d dim=%d expected_dim=%s expected_dim_source=%s",
            self.mode,
            provider_source,
            embedding_model,
            len(output),
            len(output[0]) if output else 0,
            expected_dim,
            expected_dim_source,
        )
        return output


embeddings_manager = EmbeddingsManager()

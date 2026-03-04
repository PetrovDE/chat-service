from __future__ import annotations

from typing import Dict, Optional

from app.core.config import settings
from app.services.llm.providers.base import BaseLLMProvider


class ProviderRegistry:
    def __init__(self, providers: Dict[str, BaseLLMProvider]):
        self._providers = dict(providers)

    @staticmethod
    def normalize_source(source: Optional[str]) -> str:
        src = (source or "").strip().lower()
        if not src:
            src = (settings.DEFAULT_MODEL_SOURCE or "aihub").strip().lower()
        if src == "corporate":
            src = "aihub"
        if src == "local":
            src = "ollama"
        return src

    def get(self, source: str) -> BaseLLMProvider:
        normalized = self.normalize_source(source)
        provider = self._providers.get(normalized)
        if provider is None:
            raise ValueError(f"Unknown model source: {source}. Available: {sorted(self._providers.keys())}")
        return provider

    @staticmethod
    def resolve_chat_model(source: str, requested_model: Optional[str]) -> Optional[str]:
        if requested_model:
            return requested_model
        normalized = ProviderRegistry.normalize_source(source)
        if normalized == "aihub":
            return settings.AIHUB_DEFAULT_MODEL
        if normalized == "openai":
            return settings.OPENAI_MODEL
        return settings.OLLAMA_CHAT_MODEL or settings.EMBEDDINGS_MODEL

    @staticmethod
    def resolve_embedding_model(source: str, requested_model: Optional[str]) -> Optional[str]:
        if requested_model:
            return requested_model
        normalized = ProviderRegistry.normalize_source(source)
        if normalized == "aihub":
            return settings.AIHUB_EMBEDDING_MODEL
        if normalized == "openai":
            return None
        return settings.OLLAMA_EMBED_MODEL or settings.EMBEDDINGS_MODEL


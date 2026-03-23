from __future__ import annotations

from typing import Dict, Optional

from app.services.llm.model_resolver import (
    EmbeddingDimensionResolution,
    ModelResolution,
    ProviderModelResolver,
    normalize_provider,
)
from app.services.llm.providers.base import BaseLLMProvider


class ProviderRegistry:
    def __init__(self, providers: Dict[str, BaseLLMProvider]):
        self._providers = dict(providers)
        self._resolver = ProviderModelResolver()

    @staticmethod
    def normalize_source(source: Optional[str]) -> str:
        return normalize_provider(source)

    def get(self, source: str) -> BaseLLMProvider:
        normalized = self.normalize_source(source)
        provider = self._providers.get(normalized)
        if provider is None:
            raise ValueError(f"Unknown model source: {source}. Available: {sorted(self._providers.keys())}")
        return provider

    def resolve_chat_model_decision(self, source: str, requested_model: Optional[str]) -> ModelResolution:
        normalized = ProviderRegistry.normalize_source(source)
        return self._resolver.resolve_chat(normalized, requested_model)

    def resolve_embedding_model_decision(self, source: str, requested_model: Optional[str]) -> ModelResolution:
        normalized = ProviderRegistry.normalize_source(source)
        return self._resolver.resolve_embedding(normalized, requested_model)

    def resolve_chat_model(self, source: str, requested_model: Optional[str]) -> Optional[str]:
        return self.resolve_chat_model_decision(source, requested_model).resolved_model

    def resolve_embedding_model(self, source: str, requested_model: Optional[str]) -> Optional[str]:
        return self.resolve_embedding_model_decision(source, requested_model).resolved_model

    def resolve_embedding_dimension_decision(
        self,
        source: str,
        model_name: Optional[str],
    ) -> EmbeddingDimensionResolution:
        normalized = ProviderRegistry.normalize_source(source)
        return self._resolver.resolve_embedding_dimension(provider=normalized, model_name=model_name)

    def register_runtime_embedding_dimension(
        self,
        source: str,
        model_name: Optional[str],
        dimension: int,
    ) -> EmbeddingDimensionResolution:
        normalized = ProviderRegistry.normalize_source(source)
        return self._resolver.register_runtime_embedding_dimension(
            provider=normalized,
            model_name=model_name,
            dimension=dimension,
        )

    @property
    def model_resolver(self) -> ProviderModelResolver:
        return self._resolver

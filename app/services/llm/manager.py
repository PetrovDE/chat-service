"""
LLM Manager with Provider Architecture
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
    """Unified LLM manager with pluggable providers."""

    def __init__(self):
        self.default_source = settings.DEFAULT_MODEL_SOURCE
        self.ollama_model = settings.OLLAMA_CHAT_MODEL or settings.EMBEDDINGS_MODEL
        self.openai_model = settings.OPENAI_MODEL
        self.aihub_model = settings.AIHUB_DEFAULT_MODEL
        self.providers: Dict[str, BaseLLMProvider] = {
            "ollama": ollama_provider,
            "local": ollama_provider,
            "openai": openai_provider,
            "aihub": aihub_provider,
            "corporate": aihub_provider,  # compatibility alias
        }

        logger.info("LLMManager initialized with providers: %s", list(self.providers.keys()))
        logger.info("Default source: %s", self.default_source)

    @staticmethod
    def _normalize_source(source: Optional[str]) -> str:
        src = (source or "").strip().lower()
        if not src:
            src = (settings.DEFAULT_MODEL_SOURCE or "ollama").strip().lower()
        if src == "corporate":
            src = "aihub"
        return src

    def _get_provider(self, source: str) -> BaseLLMProvider:
        normalized = self._normalize_source(source)
        provider = self.providers.get(normalized)
        if not provider:
            raise ValueError(f"Unknown model source: {source}. Available: {list(self.providers.keys())}")
        return provider

    async def get_available_models(self, source: str = "ollama") -> List[str]:
        try:
            normalized = self._normalize_source(source)
            provider = self._get_provider(normalized)
            models = await provider.get_available_models()
            logger.info("Models from %s: %s", normalized, models)
            return models
        except Exception as e:
            logger.error("Failed to fetch models from %s: %s", source, e)
            return []

    async def generate_response(
        self,
        prompt: str,
        model_source: Optional[str] = None,
        model_name: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
    ) -> Dict[str, Any]:
        source = self._normalize_source(model_source or self.default_source)
        provider = self._get_provider(source)

        if not model_name:
            if source in ("ollama", "local"):
                model_name = settings.OLLAMA_CHAT_MODEL or settings.EMBEDDINGS_MODEL
            elif source == "openai":
                model_name = settings.OPENAI_MODEL
            elif source == "aihub":
                model_name = settings.AIHUB_DEFAULT_MODEL

        logger.info("Generating response: source=%s, model=%s", source, model_name)

        return await provider.generate_response(
            prompt=prompt,
            model=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            conversation_history=conversation_history,
            prompt_max_chars=prompt_max_chars,
        )

    async def generate_response_stream(
        self,
        prompt: str,
        model_source: Optional[str] = None,
        model_name: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
    ) -> AsyncGenerator[str, None]:
        source = self._normalize_source(model_source or self.default_source)
        provider = self._get_provider(source)

        if not model_name:
            if source in ("ollama", "local"):
                model_name = settings.OLLAMA_CHAT_MODEL or settings.EMBEDDINGS_MODEL
            elif source == "openai":
                model_name = settings.OPENAI_MODEL
            elif source == "aihub":
                model_name = settings.AIHUB_DEFAULT_MODEL

        logger.info("Streaming response: source=%s, model=%s", source, model_name)

        async for chunk in provider.generate_response_stream(
            prompt=prompt,
            model=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            conversation_history=conversation_history,
            prompt_max_chars=prompt_max_chars,
        ):
            yield chunk

    async def get_available_models_detailed(self, source: str = "ollama") -> List[Dict[str, Any]]:
        normalized = self._normalize_source(source)
        provider = self._get_provider(normalized)

        detailed_fn = getattr(provider, "get_available_models_detailed", None)
        if callable(detailed_fn):
            try:
                result = await detailed_fn()
                if isinstance(result, list):
                    return result
            except Exception as e:
                logger.warning("Detailed models fetch failed for %s: %s", normalized, e)

        names = await provider.get_available_models()
        return [{"name": n} for n in names]

    async def generate_embedding(
        self,
        text: str,
        model_source: Optional[str] = None,
        model_name: Optional[str] = None,
    ) -> Optional[List[float]]:
        source = self._normalize_source(model_source or self.default_source)
        provider = self._get_provider(source)

        if not model_name:
            if source == "aihub":
                model_name = settings.AIHUB_EMBEDDING_MODEL
            elif source in ("ollama", "local"):
                model_name = settings.OLLAMA_EMBED_MODEL or settings.EMBEDDINGS_MODEL

        logger.info("Generating embedding: source=%s, model=%s", source, model_name)

        return await provider.generate_embedding(text=text, model=model_name)


llm_manager = LLMManager()

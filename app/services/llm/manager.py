"""
LLM Manager with Provider Architecture
"""
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

from app.core.config import settings
from app.services.llm.provider_clients import ProviderRegistry
from app.services.llm.providers.base import BaseLLMProvider
from app.services.llm.providers.aihub import aihub_provider
from app.services.llm.providers.ollama import ollama_provider
from app.services.llm.providers.openai import openai_provider
from app.services.llm.reliability import CircuitBreaker, CircuitBreakerConfig
from app.services.llm.routing import FallbackPolicy, ModelRouter, RoutedStream, RoutingPolicyContext

logger = logging.getLogger(__name__)


class LLMManager:
    """Unified LLM manager with pluggable providers."""

    def __init__(self):
        self.default_source = settings.DEFAULT_MODEL_SOURCE
        self.ollama_model = settings.OLLAMA_CHAT_MODEL
        self.openai_model = settings.OPENAI_MODEL
        self.aihub_model = settings.AIHUB_DEFAULT_MODEL
        self.providers: Dict[str, BaseLLMProvider] = {"ollama": ollama_provider, "openai": openai_provider, "aihub": aihub_provider}
        self.provider_registry = ProviderRegistry(self.providers)
        self.aihub_circuit_breaker = CircuitBreaker(
            CircuitBreakerConfig(
                window_seconds=settings.AIHUB_CIRCUIT_WINDOW_SECONDS,
                min_requests=settings.AIHUB_CIRCUIT_MIN_REQUESTS,
                failure_ratio_threshold=settings.AIHUB_CIRCUIT_FAILURE_RATIO,
                open_duration_seconds=settings.AIHUB_CIRCUIT_OPEN_SECONDS,
                half_open_max_requests=settings.AIHUB_CIRCUIT_HALF_OPEN_MAX_REQUESTS,
            )
        )
        self.fallback_policy = FallbackPolicy(
            policy_version=settings.LLM_FALLBACK_POLICY_VERSION,
            enabled=settings.LLM_FALLBACK_ENABLED,
            restricted_classes=settings.llm_fallback_restricted_classes_set,
        )
        self.model_router = ModelRouter(
            provider_registry=self.provider_registry,
            fallback_policy=self.fallback_policy,
            circuit_breaker=self.aihub_circuit_breaker,
        )

        logger.info("LLMManager initialized with providers: %s", list(sorted(self.providers.keys())))
        logger.info("Default source: %s", self.default_source)

    @staticmethod
    def _normalize_source(source: Optional[str]) -> str:
        return ProviderRegistry.normalize_source(source)

    def _resolve_provider_mode(
        self,
        *,
        model_source: Optional[str],
        provider_mode: Optional[str],
    ) -> str:
        normalized_source = self._normalize_source(model_source or self.default_source)
        normalized_mode = str(provider_mode or "").strip().lower()
        if normalized_source != "aihub":
            return "explicit"
        if normalized_mode == "explicit":
            return "explicit"
        return "policy"

    def _get_provider(self, source: str) -> BaseLLMProvider:
        return self.provider_registry.get(source)

    async def get_available_models(self, source: str = "aihub", capability: Optional[str] = None) -> List[str]:
        try:
            normalized = self._normalize_source(source)
            provider = self._get_provider(normalized)
            if capability is None:
                models = await provider.get_available_models()
            else:
                try:
                    models = await provider.get_available_models(capability=capability)
                except TypeError:
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
        provider_mode: Optional[str] = None,
        model_name: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
        cannot_wait: bool = False,
        sla_critical: bool = False,
        policy_class: Optional[str] = None,
    ) -> Dict[str, Any]:
        requested_source = self._normalize_source(model_source or self.default_source)
        effective_provider_mode = self._resolve_provider_mode(model_source=model_source, provider_mode=provider_mode)
        if requested_source == "openai":
            logger.warning("OpenAI source requested, but routing policy remains AI HUB-first")
        logger.info(
            "Generating response via ModelRouter: requested_source=%s route_mode=%s model=%s cannot_wait=%s sla_critical=%s policy_class=%s",
            requested_source,
            effective_provider_mode,
            model_name,
            bool(cannot_wait),
            bool(sla_critical),
            policy_class,
        )
        return await self.model_router.generate_response(
            prompt=prompt,
            requested_source=requested_source,
            route_mode=effective_provider_mode,
            provider_selected=model_source,
            model_name=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            conversation_history=conversation_history,
            prompt_max_chars=prompt_max_chars,
            policy_context=RoutingPolicyContext(
                cannot_wait=bool(cannot_wait),
                sla_critical=bool(sla_critical),
                policy_class=policy_class,
            ),
        )

    async def create_routed_stream(
        self,
        prompt: str,
        model_source: Optional[str] = None,
        provider_mode: Optional[str] = None,
        model_name: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
        cannot_wait: bool = False,
        sla_critical: bool = False,
        policy_class: Optional[str] = None,
    ) -> RoutedStream:
        requested_source = self._normalize_source(model_source or self.default_source)
        effective_provider_mode = self._resolve_provider_mode(model_source=model_source, provider_mode=provider_mode)
        return await self.model_router.create_stream(
            prompt=prompt,
            requested_source=requested_source,
            route_mode=effective_provider_mode,
            provider_selected=model_source,
            model_name=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            conversation_history=conversation_history,
            prompt_max_chars=prompt_max_chars,
            policy_context=RoutingPolicyContext(
                cannot_wait=bool(cannot_wait),
                sla_critical=bool(sla_critical),
                policy_class=policy_class,
            ),
        )

    async def generate_response_stream(
        self,
        prompt: str,
        model_source: Optional[str] = None,
        provider_mode: Optional[str] = None,
        model_name: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
        cannot_wait: bool = False,
        sla_critical: bool = False,
        policy_class: Optional[str] = None,
    ) -> AsyncGenerator[str, None]:
        routed_stream = await self.create_routed_stream(
            prompt=prompt,
            model_source=model_source,
            provider_mode=provider_mode,
            model_name=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            conversation_history=conversation_history,
            prompt_max_chars=prompt_max_chars,
            cannot_wait=cannot_wait,
            sla_critical=sla_critical,
            policy_class=policy_class,
        )
        async for chunk in routed_stream.stream:
            yield chunk

    async def get_available_models_detailed(
        self,
        source: str = "aihub",
        capability: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        normalized = self._normalize_source(source)
        provider = self._get_provider(normalized)

        detailed_fn = getattr(provider, "get_available_models_detailed", None)
        if callable(detailed_fn):
            try:
                if capability is None:
                    result = await detailed_fn()
                else:
                    try:
                        result = await detailed_fn(capability=capability)
                    except TypeError:
                        result = await detailed_fn()
                if isinstance(result, list):
                    return result
            except Exception as e:
                logger.warning("Detailed models fetch failed for %s: %s", normalized, e)

        names = await self.get_available_models(source=normalized, capability=capability)
        return [{"name": n} for n in names]

    async def generate_embedding(
        self,
        text: str,
        model_source: Optional[str] = None,
        model_name: Optional[str] = None,
    ) -> Optional[List[float]]:
        source = self._normalize_source(model_source or self.default_source)
        provider = self._get_provider(source)
        requested_model = model_name
        decision = self.provider_registry.resolve_embedding_model_decision(source, requested_model)
        model_name = decision.resolved_model
        logger.info(
            "Embedding model resolution: source=%s requested=%s resolved=%s resolution_source=%s reason=%s",
            source,
            requested_model,
            decision.resolved_model,
            decision.source,
            decision.reason,
        )
        vector = await provider.generate_embedding(text=text, model=model_name)
        logger.info(
            "Embedding generated: provider=%s model=%s dim=%s",
            source,
            model_name,
            len(vector) if vector else 0,
        )
        return vector


llm_manager = LLMManager()

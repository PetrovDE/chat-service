import asyncio
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx

from app.services.llm.provider_clients import ProviderRegistry
from app.services.llm.providers.base import BaseLLMProvider
from app.services.llm.reliability import CircuitBreaker, CircuitBreakerConfig
from app.services.llm.routing import FallbackPolicy, ModelRouter, RoutingPolicyContext


class _AIHubProvider(BaseLLMProvider):
    def __init__(self, *, fail: bool = False):
        self.fail = bool(fail)
        self.calls = 0

    async def get_available_models(self) -> List[str]:
        return ["vikhr"]

    async def generate_response(
        self,
        prompt: str,
        model: str,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
    ) -> Dict[str, Any]:
        _ = (prompt, model, temperature, max_tokens, conversation_history, prompt_max_chars)
        self.calls += 1
        if self.fail:
            raise httpx.ReadTimeout("timeout")
        return {"response": "aihub-ok", "model": model}

    async def generate_response_stream(
        self,
        prompt: str,
        model: str,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
    ) -> AsyncGenerator[str, None]:
        _ = (prompt, model, temperature, max_tokens, conversation_history, prompt_max_chars)
        if False:
            yield ""

    async def generate_embedding(self, text: str, model: Optional[str] = None) -> Optional[List[float]]:
        _ = (text, model)
        return None


class _OllamaProvider(BaseLLMProvider):
    def __init__(self):
        self.calls = 0

    async def get_available_models(self) -> List[str]:
        return ["llama3.2"]

    async def generate_response(
        self,
        prompt: str,
        model: str,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
    ) -> Dict[str, Any]:
        _ = (prompt, model, temperature, max_tokens, conversation_history, prompt_max_chars)
        self.calls += 1
        return {"response": "ollama-ok", "model": model}

    async def generate_response_stream(
        self,
        prompt: str,
        model: str,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
    ) -> AsyncGenerator[str, None]:
        _ = (prompt, model, temperature, max_tokens, conversation_history, prompt_max_chars)
        if False:
            yield ""

    async def generate_embedding(self, text: str, model: Optional[str] = None) -> Optional[List[float]]:
        _ = (text, model)
        return None


def _router(*, aihub_fail: bool = False) -> tuple[ModelRouter, _AIHubProvider, _OllamaProvider]:
    aihub = _AIHubProvider(fail=aihub_fail)
    ollama = _OllamaProvider()
    registry = ProviderRegistry({"aihub": aihub, "ollama": ollama})
    policy = FallbackPolicy(policy_version="unit-v1", restricted_classes={"restricted"}, enabled=True)
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            window_seconds=60,
            min_requests=1,
            failure_ratio_threshold=1.0,
            open_duration_seconds=60,
            half_open_max_requests=1,
        )
    )
    return ModelRouter(provider_registry=registry, fallback_policy=policy, circuit_breaker=breaker), aihub, ollama


def test_router_explicit_ollama_route_skips_aihub():
    router, aihub, ollama = _router(aihub_fail=True)
    result = asyncio.run(
        router.generate_response(
            prompt="hi",
            requested_source="ollama",
            route_mode="explicit",
            provider_selected="local",
            model_name="llama3.2",
            temperature=0.1,
            max_tokens=64,
            conversation_history=None,
            prompt_max_chars=None,
            policy_context=RoutingPolicyContext(),
        )
    )
    assert result["model_route"] == "ollama"
    assert result["provider_effective"] == "ollama"
    assert result["aihub_attempted"] is False
    assert aihub.calls == 0
    assert ollama.calls == 1


def test_router_explicit_aihub_route_uses_aihub():
    router, aihub, ollama = _router(aihub_fail=False)
    result = asyncio.run(
        router.generate_response(
            prompt="hi",
            requested_source="aihub",
            route_mode="explicit",
            provider_selected="aihub",
            model_name="vikhr",
            temperature=0.1,
            max_tokens=64,
            conversation_history=None,
            prompt_max_chars=None,
            policy_context=RoutingPolicyContext(),
        )
    )
    assert result["model_route"] == "aihub"
    assert result["provider_effective"] == "aihub"
    assert result["aihub_attempted"] is True
    assert aihub.calls == 1
    assert ollama.calls == 0


def test_router_policy_mode_falls_back_on_aihub_timeout():
    router, aihub, ollama = _router(aihub_fail=True)
    result = asyncio.run(
        router.generate_response(
            prompt="hi",
            requested_source="aihub",
            route_mode="policy",
            provider_selected="aihub",
            model_name="vikhr",
            temperature=0.1,
            max_tokens=64,
            conversation_history=None,
            prompt_max_chars=None,
            policy_context=RoutingPolicyContext(cannot_wait=True),
        )
    )
    assert result["model_route"] == "ollama_fallback"
    assert result["provider_effective"] == "ollama"
    assert result["fallback_attempted"] is True
    assert result["aihub_attempted"] is True
    assert aihub.calls == 1
    assert ollama.calls == 1

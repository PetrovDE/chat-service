import asyncio
import time
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx
import pytest

from app.observability.metrics import reset_metrics, snapshot_metrics
from app.services.llm.exceptions import AIHubUnavailableError
from app.services.llm.provider_clients import ProviderRegistry
from app.services.llm.providers.base import BaseLLMProvider
from app.services.llm.reliability import CircuitBreaker, CircuitBreakerConfig
from app.services.llm.routing import FallbackPolicy, ModelRouter, RoutingPolicyContext


class _FailingAIHubProvider(BaseLLMProvider):
    def __init__(self, *, failure_mode: str = "timeout"):
        self.failure_mode = failure_mode
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
        if self.failure_mode == "timeout":
            raise httpx.ReadTimeout("AI HUB timeout")
        if self.failure_mode == "hub_5xx":
            req = httpx.Request("POST", "http://aihub.test/chat")
            resp = httpx.Response(503, request=req)
            raise httpx.HTTPStatusError("AI HUB 503", request=req, response=resp)
        raise RuntimeError("unexpected")

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
        _ = (prompt, temperature, max_tokens, conversation_history, prompt_max_chars)
        self.calls += 1
        return {"response": "fallback-ok", "model": model, "tokens_used": 11}

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
        yield "fallback-ok"

    async def generate_embedding(self, text: str, model: Optional[str] = None) -> Optional[List[float]]:
        _ = (text, model)
        return None


def _build_router(aihub_provider: BaseLLMProvider) -> ModelRouter:
    registry = ProviderRegistry({"aihub": aihub_provider, "ollama": _OllamaProvider()})
    policy = FallbackPolicy(policy_version="test-v1", restricted_classes={"restricted"}, enabled=True)
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            window_seconds=60,
            min_requests=1,
            failure_ratio_threshold=1.0,
            open_duration_seconds=3600,
            half_open_max_requests=1,
        )
    )
    return ModelRouter(provider_registry=registry, fallback_policy=policy, circuit_breaker=breaker)


def _build_router_with_providers(aihub_provider: BaseLLMProvider, ollama_provider: BaseLLMProvider) -> ModelRouter:
    registry = ProviderRegistry({"aihub": aihub_provider, "ollama": ollama_provider})
    policy = FallbackPolicy(policy_version="test-v1", restricted_classes={"restricted"}, enabled=True)
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            window_seconds=60,
            min_requests=1,
            failure_ratio_threshold=1.0,
            open_duration_seconds=3600,
            half_open_max_requests=1,
        )
    )
    return ModelRouter(provider_registry=registry, fallback_policy=policy, circuit_breaker=breaker)


def test_explicit_ollama_route_skips_aihub_and_returns_success():
    aihub = _FailingAIHubProvider(failure_mode="timeout")
    ollama = _OllamaProvider()
    router = _build_router_with_providers(aihub, ollama)

    result = asyncio.run(
        router.generate_response(
            prompt="hello",
            requested_source="ollama",
            route_mode="explicit",
            provider_selected="local",
            model_name="llama3.2",
            temperature=0.1,
            max_tokens=64,
            conversation_history=None,
            prompt_max_chars=None,
            policy_context=RoutingPolicyContext(cannot_wait=False, sla_critical=False, policy_class="standard"),
        )
    )

    assert result["response"] == "fallback-ok"
    assert result["model_route"] == "ollama"
    assert result["route_mode"] == "explicit"
    assert result["provider_selected"] == "local"
    assert result["provider_effective"] == "ollama"
    assert result["aihub_attempted"] is False
    assert result["fallback_attempted"] is False
    assert aihub.calls == 0
    assert ollama.calls == 1


def test_explicit_aihub_route_uses_aihub_only():
    class _HealthyAIHubProvider(_FailingAIHubProvider):
        async def generate_response(
            self,
            prompt: str,
            model: str,
            temperature: float = 0.7,
            max_tokens: int = 2000,
            conversation_history: Optional[List[Dict[str, str]]] = None,
            prompt_max_chars: Optional[int] = None,
        ) -> Dict[str, Any]:
            _ = (prompt, temperature, max_tokens, conversation_history, prompt_max_chars)
            self.calls += 1
            return {"response": "aihub-ok", "model": model, "tokens_used": 7}

    aihub = _HealthyAIHubProvider(failure_mode="timeout")
    ollama = _OllamaProvider()
    router = _build_router_with_providers(aihub, ollama)

    result = asyncio.run(
        router.generate_response(
            prompt="hello",
            requested_source="aihub",
            route_mode="explicit",
            provider_selected="aihub",
            model_name="vikhr",
            temperature=0.1,
            max_tokens=64,
            conversation_history=None,
            prompt_max_chars=None,
            policy_context=RoutingPolicyContext(cannot_wait=False, sla_critical=False, policy_class="standard"),
        )
    )

    assert result["response"] == "aihub-ok"
    assert result["model_route"] == "aihub"
    assert result["route_mode"] == "explicit"
    assert result["provider_effective"] == "aihub"
    assert result["aihub_attempted"] is True
    assert result["fallback_attempted"] is False
    assert aihub.calls == 1
    assert ollama.calls == 0


def test_aihub_timeout_triggers_controlled_fallback_when_allowed():
    reset_metrics()
    router = _build_router(_FailingAIHubProvider(failure_mode="timeout"))

    result = asyncio.run(
        router.generate_response(
            prompt="hello",
            requested_source="aihub",
            route_mode="policy",
            provider_selected="aihub",
            model_name=None,
            temperature=0.1,
            max_tokens=64,
            conversation_history=None,
            prompt_max_chars=None,
            policy_context=RoutingPolicyContext(cannot_wait=True, sla_critical=False, policy_class="standard"),
        )
    )

    assert result["response"] == "fallback-ok"
    assert result["model_route"] == "ollama_fallback"
    assert result["route_mode"] == "policy"
    assert result["fallback_reason"] == "timeout"
    assert result["fallback_allowed"] is True
    assert result["fallback_attempted"] is True
    assert result["provider_effective"] == "ollama"
    assert result["aihub_attempted"] is True
    assert result["fallback_policy_version"] == "test-v1"
    counters = snapshot_metrics()["counters"]
    assert any(
        "llama_service_llm_route_decisions_total" in key
        and "route=ollama_fallback" in key
        and "fallback_reason=timeout" in key
        for key in counters
    )


def test_aihub_outage_denies_fallback_for_restricted_policy_class():
    router = _build_router(_FailingAIHubProvider(failure_mode="hub_5xx"))

    with pytest.raises(AIHubUnavailableError):
        asyncio.run(
            router.generate_response(
                prompt="hello",
                requested_source="aihub",
                route_mode="policy",
                provider_selected="aihub",
                model_name=None,
                temperature=0.1,
                max_tokens=64,
                conversation_history=None,
                prompt_max_chars=None,
                policy_context=RoutingPolicyContext(cannot_wait=True, sla_critical=True, policy_class="restricted"),
            )
        )


def test_circuit_open_path_uses_fallback_without_aihub_call():
    failing_aihub = _FailingAIHubProvider(failure_mode="timeout")
    router = _build_router(failing_aihub)

    # Trip the circuit in advance.
    router._circuit.record_failure(now=time.time())  # noqa: SLF001

    result = asyncio.run(
        router.generate_response(
            prompt="hello",
            requested_source="aihub",
            route_mode="policy",
            provider_selected="aihub",
            model_name=None,
            temperature=0.1,
            max_tokens=64,
            conversation_history=None,
            prompt_max_chars=None,
            policy_context=RoutingPolicyContext(cannot_wait=True, sla_critical=False, policy_class="standard"),
        )
    )

    assert failing_aihub.calls == 0
    assert result["model_route"] == "ollama_fallback"
    assert result["route_mode"] == "policy"
    assert result["fallback_reason"] == "circuit_open"
    assert result["fallback_allowed"] is True
    assert result["fallback_attempted"] is True
    assert result["provider_effective"] == "ollama"
    assert result["aihub_attempted"] is False

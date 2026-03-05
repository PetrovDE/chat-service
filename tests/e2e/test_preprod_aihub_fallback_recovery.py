import asyncio
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx

from app.observability.metrics import reset_metrics, snapshot_metrics
from app.services.llm.provider_clients import ProviderRegistry
from app.services.llm.providers.base import BaseLLMProvider
from app.services.llm.reliability import CircuitBreaker, CircuitBreakerConfig
from app.services.llm.routing import FallbackPolicy, ModelRouter, RoutingPolicyContext


class _MutableClock:
    def __init__(self, start: float):
        self.current = float(start)

    def time(self) -> float:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += float(seconds)


class _SwitchableAIHubProvider(BaseLLMProvider):
    def __init__(self, *, available: bool = False):
        self.available = bool(available)
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
        if not self.available:
            raise httpx.ReadTimeout("AI HUB timeout")
        return {"response": "aihub-ok", "model": model, "tokens_used": 9}

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
        if not self.available:
            raise httpx.ReadTimeout("AI HUB timeout")
        yield "aihub-ok"

    async def generate_embedding(self, text: str, model: Optional[str] = None) -> Optional[List[float]]:
        _ = (text, model)
        return None


class _OllamaProvider(BaseLLMProvider):
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
            open_duration_seconds=30,
            half_open_max_requests=1,
        )
    )
    return ModelRouter(provider_registry=registry, fallback_policy=policy, circuit_breaker=breaker)


def test_preprod_e2e_outage_fallback_then_recovery_closes_fallback(monkeypatch):
    import app.services.llm.reliability.circuit_breaker as circuit_breaker_module

    clock = _MutableClock(start=1000.0)
    monkeypatch.setattr(circuit_breaker_module.time, "time", clock.time)

    reset_metrics()
    aihub_provider = _SwitchableAIHubProvider(available=False)
    router = _build_router(aihub_provider)
    policy_context = RoutingPolicyContext(cannot_wait=True, sla_critical=False, policy_class="standard")

    # Step 1: AI HUB outage should trigger controlled fallback.
    first = asyncio.run(
        router.generate_response(
            prompt="hello",
            requested_source="aihub",
            model_name=None,
            temperature=0.1,
            max_tokens=64,
            conversation_history=None,
            prompt_max_chars=None,
            policy_context=policy_context,
        )
    )
    assert first["model_route"] == "ollama_fallback"
    assert first["fallback_reason"] == "timeout"
    assert first["fallback_allowed"] is True
    assert router._circuit.state == "open"  # noqa: SLF001
    assert aihub_provider.calls == 1

    # Step 2: while circuit is open we should remain in fallback mode.
    clock.advance(5.0)
    second = asyncio.run(
        router.generate_response(
            prompt="hello-again",
            requested_source="aihub",
            model_name=None,
            temperature=0.1,
            max_tokens=64,
            conversation_history=None,
            prompt_max_chars=None,
            policy_context=policy_context,
        )
    )
    assert second["model_route"] == "ollama_fallback"
    assert second["fallback_reason"] == "circuit_open"
    assert second["fallback_allowed"] is True
    assert aihub_provider.calls == 1

    # Step 3: after cooldown and recovery, route returns to AI HUB primary.
    clock.advance(31.0)
    aihub_provider.available = True
    third = asyncio.run(
        router.generate_response(
            prompt="hello-recovered",
            requested_source="aihub",
            model_name=None,
            temperature=0.1,
            max_tokens=64,
            conversation_history=None,
            prompt_max_chars=None,
            policy_context=policy_context,
        )
    )
    assert third["response"] == "aihub-ok"
    assert third["model_route"] == "aihub_primary"
    assert third["fallback_reason"] == "none"
    assert third["fallback_allowed"] is False
    assert router._circuit.state == "closed"  # noqa: SLF001
    assert aihub_provider.calls == 2

    counters = snapshot_metrics()["counters"]
    assert any("llama_service_llm_route_decisions_total" in key and "route=ollama_fallback" in key for key in counters)
    assert any("llama_service_llm_route_decisions_total" in key and "route=aihub_primary" in key for key in counters)

from __future__ import annotations

import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

from app.observability.metrics import inc_counter
from app.observability.slo_metrics import observe_llm_route_decision
from app.services.llm.exceptions import AIHubUnavailableError, CircuitOpenError
from app.services.llm.provider_clients import ProviderRegistry
from app.services.llm.reliability import CircuitBreaker, classify_aihub_failure
from app.services.llm.routing.policy import FallbackPolicy
from app.services.llm.routing.types import RouteTelemetry, RoutedStream, RoutingPolicyContext

logger = logging.getLogger(__name__)


class ModelRouter:
    EXPLICIT_MODE = "explicit"
    POLICY_MODE = "policy"

    def __init__(
        self,
        *,
        provider_registry: ProviderRegistry,
        fallback_policy: FallbackPolicy,
        circuit_breaker: CircuitBreaker,
    ):
        self._providers = provider_registry
        self._policy = fallback_policy
        self._circuit = circuit_breaker

    def _new_telemetry(self, *, route_mode: str, provider_selected: Optional[str]) -> RouteTelemetry:
        return RouteTelemetry(
            model_route="aihub_primary",
            route_mode=route_mode,
            provider_selected=provider_selected,
            provider_effective="aihub",
            fallback_reason="none",
            fallback_allowed=False,
            fallback_attempted=False,
            fallback_policy_version=self._policy.policy_version,
            aihub_attempted=False,
        )

    @staticmethod
    def _normalize_for_requested_model(source: Optional[str]) -> str:
        return ProviderRegistry.normalize_source(source)

    def _resolve_models(
        self,
        requested_source: Optional[str],
        requested_model: Optional[str],
    ) -> tuple[Optional[str], Optional[str]]:
        normalized_source = self._normalize_for_requested_model(requested_source)
        aihub_requested = requested_model if normalized_source == "aihub" else None
        ollama_requested = requested_model if normalized_source == "ollama" else None
        aihub_model = self._providers.resolve_chat_model("aihub", aihub_requested)
        ollama_model = self._providers.resolve_chat_model("ollama", ollama_requested)
        return aihub_model, ollama_model

    def _observe_route(self, *, telemetry: RouteTelemetry) -> None:
        inc_counter(
            "llm_model_route_total",
            route=telemetry.model_route,
            route_mode=telemetry.route_mode,
            provider_effective=telemetry.provider_effective,
            aihub_attempted=str(bool(telemetry.aihub_attempted)).lower(),
            fallback_attempted=str(bool(telemetry.fallback_attempted)).lower(),
            fallback_reason=str(telemetry.fallback_reason or "none"),
            fallback_allowed=str(bool(telemetry.fallback_allowed)).lower(),
            fallback_policy_version=telemetry.fallback_policy_version,
        )
        observe_llm_route_decision(
            route=telemetry.model_route,
            route_mode=telemetry.route_mode,
            provider_effective=telemetry.provider_effective,
            aihub_attempted=bool(telemetry.aihub_attempted),
            fallback_attempted=bool(telemetry.fallback_attempted),
            fallback_reason=str(telemetry.fallback_reason or "none"),
            fallback_allowed=bool(telemetry.fallback_allowed),
            fallback_policy_version=telemetry.fallback_policy_version,
        )
        inc_counter("llm_aihub_circuit_state_total", state=str(self._circuit.state))

    def _build_unavailable_error(self, *, reason: str, decision_summary: str) -> AIHubUnavailableError:
        message = f"AI HUB unavailable ({reason}); fallback denied: {decision_summary}"
        return AIHubUnavailableError(message)

    @classmethod
    def _resolve_route_mode(cls, *, requested_source: str, route_mode: Optional[str]) -> str:
        normalized_source = ProviderRegistry.normalize_source(requested_source)
        normalized_mode = str(route_mode or "").strip().lower()
        if normalized_source != "aihub":
            return cls.EXPLICIT_MODE
        if normalized_mode == cls.EXPLICIT_MODE:
            return cls.EXPLICIT_MODE
        return cls.POLICY_MODE

    async def _generate_explicit(
        self,
        *,
        prompt: str,
        requested_source: str,
        requested_model: Optional[str],
        temperature: float,
        max_tokens: int,
        conversation_history: Optional[List[Dict[str, str]]],
        prompt_max_chars: Optional[int],
        telemetry: RouteTelemetry,
    ) -> Dict[str, Any]:
        normalized_source = ProviderRegistry.normalize_source(requested_source)
        provider = self._providers.get(normalized_source)
        model = self._providers.resolve_chat_model(normalized_source, requested_model)
        telemetry.provider_effective = normalized_source
        telemetry.model_route = normalized_source
        telemetry.fallback_reason = "none"
        telemetry.fallback_allowed = False
        telemetry.fallback_attempted = False
        telemetry.aihub_attempted = normalized_source == "aihub"

        result = await provider.generate_response(
            prompt=prompt,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
            conversation_history=conversation_history,
            prompt_max_chars=prompt_max_chars,
        )
        result.update(telemetry.as_dict())
        self._observe_route(telemetry=telemetry)
        logger.info(
            "ModelRouter explicit route: provider=%s aihub_attempted=%s",
            normalized_source,
            telemetry.aihub_attempted,
        )
        return result

    async def generate_response(
        self,
        *,
        prompt: str,
        requested_source: Optional[str],
        route_mode: Optional[str],
        provider_selected: Optional[str],
        model_name: Optional[str],
        temperature: float,
        max_tokens: int,
        conversation_history: Optional[List[Dict[str, str]]],
        prompt_max_chars: Optional[int],
        policy_context: RoutingPolicyContext,
    ) -> Dict[str, Any]:
        normalized_source = ProviderRegistry.normalize_source(requested_source)
        effective_route_mode = self._resolve_route_mode(
            requested_source=normalized_source,
            route_mode=route_mode,
        )
        selected_value = (
            str(provider_selected).strip().lower()
            if provider_selected is not None and str(provider_selected).strip()
            else normalized_source
        )
        telemetry = self._new_telemetry(route_mode=effective_route_mode, provider_selected=selected_value)

        if effective_route_mode == self.EXPLICIT_MODE:
            return await self._generate_explicit(
                prompt=prompt,
                requested_source=normalized_source,
                requested_model=model_name,
                temperature=temperature,
                max_tokens=max_tokens,
                conversation_history=conversation_history,
                prompt_max_chars=prompt_max_chars,
                telemetry=telemetry,
            )

        aihub_model, ollama_model = self._resolve_models(normalized_source, model_name)
        aihub_provider = self._providers.get("aihub")
        ollama_provider = self._providers.get("ollama")

        outage_reason = "none"
        last_error: Optional[Exception] = None

        allowed, deny_reason = self._circuit.allow_request()
        if not allowed:
            outage_reason = str(deny_reason or "circuit_open")
            last_error = CircuitOpenError("AI HUB circuit is open")
            telemetry.aihub_attempted = False
        else:
            try:
                telemetry.aihub_attempted = True
                result = await aihub_provider.generate_response(
                    prompt=prompt,
                    model=aihub_model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    conversation_history=conversation_history,
                    prompt_max_chars=prompt_max_chars,
                )
                self._circuit.record_success()
                telemetry.provider_effective = "aihub"
                telemetry.model_route = "aihub_primary"
                telemetry.fallback_reason = "none"
                telemetry.fallback_allowed = False
                telemetry.fallback_attempted = False
                result.update(telemetry.as_dict())
                self._observe_route(telemetry=telemetry)
                logger.info("ModelRouter route: aihub_primary")
                return result
            except Exception as exc:
                reason = classify_aihub_failure(exc)
                if reason is None:
                    raise
                outage_reason = reason
                last_error = exc
                self._circuit.record_failure()

        decision = self._policy.evaluate(context=policy_context, outage_reason=outage_reason)
        telemetry.fallback_reason = decision.outage_reason
        telemetry.fallback_allowed = bool(decision.allowed)
        telemetry.fallback_attempted = bool(decision.allowed)

        if decision.allowed:
            fallback_result = await ollama_provider.generate_response(
                prompt=prompt,
                model=ollama_model,
                temperature=temperature,
                max_tokens=max_tokens,
                conversation_history=conversation_history,
                prompt_max_chars=prompt_max_chars,
            )
            telemetry.model_route = "ollama_fallback"
            telemetry.provider_effective = "ollama"
            fallback_result.update(telemetry.as_dict())
            self._observe_route(telemetry=telemetry)
            logger.warning("ModelRouter route: ollama_fallback reason=%s", outage_reason)
            return fallback_result

        telemetry.provider_effective = "aihub"
        self._observe_route(telemetry=telemetry)
        summary = f"outage={decision.outage}, urgent={decision.urgent}, restricted={decision.restricted}"
        raise self._build_unavailable_error(reason=outage_reason, decision_summary=summary) from last_error

    async def _build_explicit_stream(
        self,
        *,
        prompt: str,
        requested_source: str,
        requested_model: Optional[str],
        temperature: float,
        max_tokens: int,
        conversation_history: Optional[List[Dict[str, str]]],
        prompt_max_chars: Optional[int],
        telemetry: RouteTelemetry,
    ) -> RoutedStream:
        normalized_source = ProviderRegistry.normalize_source(requested_source)
        provider = self._providers.get(normalized_source)
        model = self._providers.resolve_chat_model(normalized_source, requested_model)
        telemetry.provider_effective = normalized_source
        telemetry.model_route = normalized_source
        telemetry.fallback_reason = "none"
        telemetry.fallback_allowed = False
        telemetry.fallback_attempted = False
        telemetry.aihub_attempted = normalized_source == "aihub"

        async def _explicit_stream() -> AsyncGenerator[str, None]:
            async for chunk in provider.generate_response_stream(
                prompt=prompt,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
                conversation_history=conversation_history,
                prompt_max_chars=prompt_max_chars,
            ):
                yield chunk
            self._observe_route(telemetry=telemetry)
            logger.info(
                "ModelRouter stream explicit route: provider=%s aihub_attempted=%s",
                normalized_source,
                telemetry.aihub_attempted,
            )

        return RoutedStream(stream=_explicit_stream(), telemetry=telemetry)

    async def create_stream(
        self,
        *,
        prompt: str,
        requested_source: Optional[str],
        route_mode: Optional[str],
        provider_selected: Optional[str],
        model_name: Optional[str],
        temperature: float,
        max_tokens: int,
        conversation_history: Optional[List[Dict[str, str]]],
        prompt_max_chars: Optional[int],
        policy_context: RoutingPolicyContext,
    ) -> RoutedStream:
        normalized_source = ProviderRegistry.normalize_source(requested_source)
        effective_route_mode = self._resolve_route_mode(
            requested_source=normalized_source,
            route_mode=route_mode,
        )
        selected_value = (
            str(provider_selected).strip().lower()
            if provider_selected is not None and str(provider_selected).strip()
            else normalized_source
        )
        telemetry = self._new_telemetry(route_mode=effective_route_mode, provider_selected=selected_value)

        if effective_route_mode == self.EXPLICIT_MODE:
            return await self._build_explicit_stream(
                prompt=prompt,
                requested_source=normalized_source,
                requested_model=model_name,
                temperature=temperature,
                max_tokens=max_tokens,
                conversation_history=conversation_history,
                prompt_max_chars=prompt_max_chars,
                telemetry=telemetry,
            )

        aihub_model, ollama_model = self._resolve_models(normalized_source, model_name)
        aihub_provider = self._providers.get("aihub")
        ollama_provider = self._providers.get("ollama")

        async def _ollama_stream() -> AsyncGenerator[str, None]:
            async for chunk in ollama_provider.generate_response_stream(
                prompt=prompt,
                model=ollama_model,
                temperature=temperature,
                max_tokens=max_tokens,
                conversation_history=conversation_history,
                prompt_max_chars=prompt_max_chars,
            ):
                yield chunk

        allowed, deny_reason = self._circuit.allow_request()
        if not allowed:
            reason = str(deny_reason or "circuit_open")
            decision = self._policy.evaluate(context=policy_context, outage_reason=reason)
            telemetry.fallback_reason = decision.outage_reason
            telemetry.fallback_allowed = bool(decision.allowed)
            telemetry.fallback_attempted = bool(decision.allowed)
            telemetry.aihub_attempted = False
            if decision.allowed:
                telemetry.model_route = "ollama_fallback"
                telemetry.provider_effective = "ollama"
                self._observe_route(telemetry=telemetry)
                logger.warning("ModelRouter stream route: ollama_fallback reason=%s", reason)
                return RoutedStream(stream=_ollama_stream(), telemetry=telemetry)
            telemetry.provider_effective = "aihub"
            self._observe_route(telemetry=telemetry)
            summary = f"outage={decision.outage}, urgent={decision.urgent}, restricted={decision.restricted}"
            raise self._build_unavailable_error(
                reason=reason,
                decision_summary=summary,
            )

        async def _stream_with_fallback() -> AsyncGenerator[str, None]:
            emitted = False
            try:
                telemetry.aihub_attempted = True
                async for chunk in aihub_provider.generate_response_stream(
                    prompt=prompt,
                    model=aihub_model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    conversation_history=conversation_history,
                    prompt_max_chars=prompt_max_chars,
                ):
                    emitted = True
                    yield chunk
                self._circuit.record_success()
                telemetry.provider_effective = "aihub"
                telemetry.model_route = "aihub_primary"
                telemetry.fallback_reason = "none"
                telemetry.fallback_allowed = False
                telemetry.fallback_attempted = False
                self._observe_route(telemetry=telemetry)
                logger.info("ModelRouter stream route: aihub_primary")
            except Exception as exc:
                reason = classify_aihub_failure(exc)
                if reason is None:
                    self._observe_route(telemetry=telemetry)
                    raise

                self._circuit.record_failure()
                decision = self._policy.evaluate(context=policy_context, outage_reason=reason)
                telemetry.fallback_reason = decision.outage_reason
                telemetry.fallback_allowed = bool(decision.allowed)
                telemetry.fallback_attempted = bool(decision.allowed)

                if not emitted and decision.allowed:
                    telemetry.model_route = "ollama_fallback"
                    telemetry.provider_effective = "ollama"
                    logger.warning("ModelRouter stream route: ollama_fallback reason=%s", reason)
                    async for fallback_chunk in _ollama_stream():
                        yield fallback_chunk
                    self._observe_route(telemetry=telemetry)
                    return

                telemetry.provider_effective = "aihub"
                self._observe_route(telemetry=telemetry)
                summary = f"outage={decision.outage}, urgent={decision.urgent}, restricted={decision.restricted}"
                raise self._build_unavailable_error(reason=reason, decision_summary=summary) from exc

        return RoutedStream(stream=_stream_with_fallback(), telemetry=telemetry)

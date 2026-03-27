from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

from app.core.config import settings


logger = logging.getLogger(__name__)
EngineExecutor = Callable[..., Awaitable[Optional[Dict[str, Any]]]]


def _normalize_engine_mode(raw_mode: str) -> str:
    normalized = str(raw_mode or "legacy").strip().lower()
    if normalized not in {"legacy", "langgraph"}:
        return "legacy"
    return normalized


def _payload_summary(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {"status": "none", "selected_route": "unknown", "fallback_reason": "none"}
    debug = payload.get("debug") if isinstance(payload.get("debug"), dict) else {}
    return {
        "status": str(payload.get("status") or "unknown"),
        "selected_route": str(debug.get("selected_route") or "unknown"),
        "fallback_reason": str(debug.get("fallback_reason") or "none"),
    }


def _attach_engine_debug(
    *,
    payload: Optional[Dict[str, Any]],
    requested_mode: str,
    served_mode: str,
    shadow_enabled: bool,
    shadow_summary: Optional[Dict[str, Any]],
    fallback_reason: str,
) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return payload

    debug = payload.get("debug")
    if not isinstance(debug, dict):
        debug = {}
        payload["debug"] = debug

    tabular_debug = debug.get("tabular_sql")
    if not isinstance(tabular_debug, dict):
        tabular_debug = {}
        debug["tabular_sql"] = tabular_debug

    fields = {
        "analytics_engine_mode_requested": requested_mode,
        "analytics_engine_mode_served": served_mode,
        "analytics_engine_shadow_enabled": bool(shadow_enabled),
        "analytics_engine_fallback_reason": fallback_reason,
    }
    if isinstance(shadow_summary, dict):
        fields["analytics_engine_shadow"] = shadow_summary

    debug.update(fields)
    tabular_debug.update(fields)
    return payload


async def _run_executor_safe(
    *,
    executor: EngineExecutor,
    query: str,
    files: List[Any],
) -> tuple[Optional[Dict[str, Any]], Optional[Exception]]:
    try:
        payload = await executor(query=query, files=files)
        return payload, None
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return None, exc


async def execute_tabular_engine_route(
    *,
    query: str,
    files: List[Any],
    legacy_executor: EngineExecutor,
    langgraph_executor: EngineExecutor,
) -> Optional[Dict[str, Any]]:
    requested_mode = _normalize_engine_mode(str(getattr(settings, "ANALYTICS_ENGINE_MODE", "legacy") or "legacy"))
    shadow_enabled = bool(getattr(settings, "ANALYTICS_ENGINE_SHADOW", False))

    primary_executor = legacy_executor
    primary_label = "legacy"
    shadow_executor: Optional[EngineExecutor] = None
    shadow_label = "none"
    if requested_mode == "langgraph":
        primary_executor = langgraph_executor
        primary_label = "langgraph"
        if shadow_enabled:
            shadow_executor = legacy_executor
            shadow_label = "legacy"
    elif shadow_enabled:
        shadow_executor = langgraph_executor
        shadow_label = "langgraph"

    primary_payload, primary_error = await _run_executor_safe(
        executor=primary_executor,
        query=query,
        files=files,
    )

    served_mode = primary_label
    fallback_reason = "none"
    if primary_error is not None:
        logger.exception(
            "tabular_analytics_engine_primary_failed mode=%s error=%s",
            primary_label,
            type(primary_error).__name__,
        )
        if primary_label == "langgraph":
            legacy_payload, legacy_error = await _run_executor_safe(
                executor=legacy_executor,
                query=query,
                files=files,
            )
            primary_payload = legacy_payload
            served_mode = "legacy"
            fallback_reason = "langgraph_exception"
            if legacy_error is not None:
                logger.exception(
                    "tabular_analytics_engine_legacy_fallback_failed error=%s",
                    type(legacy_error).__name__,
                )
                primary_payload = None
                fallback_reason = "langgraph_and_legacy_failed"
    elif primary_payload is None and primary_label == "langgraph":
        legacy_payload, legacy_error = await _run_executor_safe(
            executor=legacy_executor,
            query=query,
            files=files,
        )
        primary_payload = legacy_payload
        served_mode = "legacy"
        fallback_reason = "langgraph_none_payload"
        if legacy_error is not None:
            logger.exception(
                "tabular_analytics_engine_legacy_fallback_failed error=%s",
                type(legacy_error).__name__,
            )
            primary_payload = None
            fallback_reason = "langgraph_none_and_legacy_failed"

    shadow_summary: Optional[Dict[str, Any]] = None
    if shadow_executor is not None:
        shadow_payload, shadow_error = await _run_executor_safe(
            executor=shadow_executor,
            query=query,
            files=files,
        )
        shadow_summary = {
            "mode": shadow_label,
            "error": type(shadow_error).__name__ if shadow_error is not None else None,
            "result": _payload_summary(shadow_payload),
        }

    logger.info(
        (
            "tabular_analytics_engine_route requested_mode=%s served_mode=%s "
            "shadow_enabled=%s fallback_reason=%s"
        ),
        requested_mode,
        served_mode,
        str(shadow_enabled).lower(),
        fallback_reason,
    )

    return _attach_engine_debug(
        payload=primary_payload,
        requested_mode=requested_mode,
        served_mode=served_mode,
        shadow_enabled=shadow_enabled,
        shadow_summary=shadow_summary,
        fallback_reason=fallback_reason,
    )

from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

from app.core.config import settings
from app.observability.context import conversation_id_ctx, request_id_ctx, user_id_ctx


logger = logging.getLogger(__name__)
EngineExecutor = Callable[..., Awaitable[Optional[Dict[str, Any]]]]


def _normalize_engine_mode(raw_mode: str) -> str:
    normalized = str(raw_mode or "langgraph").strip().lower()
    if normalized not in {"legacy", "langgraph"}:
        return "langgraph"
    return normalized


def _payload_summary(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "status": "none",
            "selected_route": "unknown",
            "fallback_reason": "none",
            "graph_run_id": None,
            "stop_reason": "none",
        }
    debug = payload.get("debug") if isinstance(payload.get("debug"), dict) else {}
    return {
        "status": str(payload.get("status") or "unknown"),
        "selected_route": str(debug.get("selected_route") or "unknown"),
        "fallback_reason": str(debug.get("fallback_reason") or "none"),
        "graph_run_id": str(debug.get("analytics_engine_graph_run_id") or "").strip() or None,
        "stop_reason": str(debug.get("analytics_engine_graph_stop_reason") or "none"),
    }


def _extract_graph_runtime(payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "analytics_engine_graph_run_id": None,
            "analytics_engine_graph_node_path": [],
            "analytics_engine_graph_attempts": 0,
            "analytics_engine_graph_stop_reason": "none",
        }
    debug = payload.get("debug") if isinstance(payload.get("debug"), dict) else {}
    tabular_debug = debug.get("tabular_sql") if isinstance(debug.get("tabular_sql"), dict) else {}

    graph_trace = debug.get("analytics_engine_graph_trace")
    node_path: List[str] = []
    if isinstance(graph_trace, list):
        for item in graph_trace:
            if not isinstance(item, dict):
                continue
            node_name = str(item.get("node") or "").strip()
            if node_name:
                node_path.append(node_name)

    graph_attempts = debug.get("analytics_engine_graph_attempts")
    if graph_attempts is None:
        graph_attempts = tabular_debug.get("repair_iteration_count")
    try:
        normalized_attempts = int(graph_attempts or (1 if node_path else 0))
    except Exception:
        normalized_attempts = 0

    stop_reason = str(
        debug.get("analytics_engine_graph_stop_reason")
        or debug.get("analytics_engine_graph_stop")
        or "none"
    )
    return {
        "analytics_engine_graph_run_id": str(debug.get("analytics_engine_graph_run_id") or "").strip() or None,
        "analytics_engine_graph_node_path": node_path,
        "analytics_engine_graph_attempts": max(0, normalized_attempts),
        "analytics_engine_graph_stop_reason": stop_reason,
    }


def _attach_engine_debug(
    *,
    payload: Optional[Dict[str, Any]],
    requested_mode: str,
    served_mode: str,
    shadow_enabled: bool,
    shadow_summary: Optional[Dict[str, Any]],
    fallback_reason: str,
    rollback_mode_used: bool,
    legacy_activation_reason: str,
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

    graph_runtime = _extract_graph_runtime(payload)
    node_path = list(graph_runtime.get("analytics_engine_graph_node_path") or [])
    graph_run_id = graph_runtime.get("analytics_engine_graph_run_id")
    graph_attempts = int(graph_runtime.get("analytics_engine_graph_attempts", 0) or 0)
    stop_reason = str(graph_runtime.get("analytics_engine_graph_stop_reason") or "none")
    if stop_reason == "none" and fallback_reason != "none":
        stop_reason = f"engine_fallback:{fallback_reason}"
    fields = {
        "analytics_engine_mode_requested": requested_mode,
        "analytics_engine_mode_served": served_mode,
        "analytics_engine_shadow_enabled": bool(shadow_enabled),
        "analytics_engine_fallback_reason": fallback_reason,
        "analytics_engine_graph_run_id": graph_run_id,
        "analytics_engine_graph_node_path": node_path,
        "analytics_engine_graph_attempts": graph_attempts,
        "analytics_engine_graph_stop_reason": stop_reason,
        "engine_mode_requested": requested_mode,
        "engine_mode_served": served_mode,
        "shadow_mode": bool(shadow_enabled),
        "engine_fallback_reason": fallback_reason,
        "analytics_engine_rollback_mode_used": bool(rollback_mode_used),
        "analytics_engine_legacy_activation_reason": legacy_activation_reason,
        "rollback_mode_used": bool(rollback_mode_used),
        "legacy_activation_reason": legacy_activation_reason,
        "graph_run_id": graph_run_id,
        "graph_node_path": node_path,
        "graph_attempts": graph_attempts,
        "stop_reason": stop_reason,
    }
    if isinstance(shadow_summary, dict):
        fields["analytics_engine_shadow"] = shadow_summary

    debug.update(fields)
    tabular_debug.update(fields)
    return payload


def _resolve_legacy_activation_reason(
    *,
    requested_mode: str,
    served_mode: str,
    fallback_reason: str,
) -> str:
    if served_mode != "legacy":
        return "none"
    if requested_mode == "legacy":
        return "explicit_rollback_mode"
    if str(fallback_reason or "none") != "none":
        return "langgraph_fail_open_fallback"
    return "legacy_served_unspecified"


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
    requested_mode = _normalize_engine_mode(str(getattr(settings, "ANALYTICS_ENGINE_MODE", "langgraph") or "langgraph"))
    shadow_enabled = bool(getattr(settings, "ANALYTICS_ENGINE_SHADOW", False))

    primary_executor = langgraph_executor
    primary_label = "langgraph"
    shadow_executor: Optional[EngineExecutor] = None
    shadow_label = "none"
    if requested_mode == "legacy":
        primary_executor = legacy_executor
        primary_label = "legacy"
        if shadow_enabled:
            shadow_executor = langgraph_executor
            shadow_label = "langgraph"
    elif shadow_enabled:
        shadow_executor = legacy_executor
        shadow_label = "legacy"

    primary_payload, primary_error = await _run_executor_safe(
        executor=primary_executor,
        query=query,
        files=files,
    )

    served_mode = primary_label
    fallback_reason = "none"
    if primary_error is not None:
        logger.exception(
            (
                "tabular_analytics_engine_primary_failed mode=%s error=%s "
                "rid=%s uid=%s cid=%s"
            ),
            primary_label,
            type(primary_error).__name__,
            request_id_ctx.get() or "-",
            user_id_ctx.get() or "-",
            conversation_id_ctx.get() or "-",
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
                    (
                        "tabular_analytics_engine_legacy_fallback_failed error=%s "
                        "rid=%s uid=%s cid=%s"
                    ),
                    type(legacy_error).__name__,
                    request_id_ctx.get() or "-",
                    user_id_ctx.get() or "-",
                    conversation_id_ctx.get() or "-",
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
                (
                    "tabular_analytics_engine_legacy_fallback_failed error=%s "
                    "rid=%s uid=%s cid=%s"
                ),
                type(legacy_error).__name__,
                request_id_ctx.get() or "-",
                user_id_ctx.get() or "-",
                conversation_id_ctx.get() or "-",
            )
            primary_payload = None
            fallback_reason = "langgraph_none_and_legacy_failed"

    rollback_mode_used = served_mode == "legacy"
    legacy_activation_reason = _resolve_legacy_activation_reason(
        requested_mode=requested_mode,
        served_mode=served_mode,
        fallback_reason=fallback_reason,
    )

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

    primary_graph = _extract_graph_runtime(primary_payload)
    primary_debug = primary_payload.get("debug") if isinstance(primary_payload, dict) and isinstance(primary_payload.get("debug"), dict) else {}
    file_id = (
        str(primary_debug.get("file_id") or primary_debug.get("scope_selected_file_id") or "").strip()
        if isinstance(primary_debug, dict)
        else ""
    )
    upload_id = str(primary_debug.get("upload_id") or "").strip() if isinstance(primary_debug, dict) else ""
    document_id = str(primary_debug.get("document_id") or "").strip() if isinstance(primary_debug, dict) else ""
    logger.info(
        (
            "tabular_analytics_engine_route requested_mode=%s served_mode=%s "
            "shadow_enabled=%s fallback_reason=%s graph_run_id=%s graph_node_path=%s "
            "graph_attempts=%s stop_reason=%s rollback_mode_used=%s legacy_activation_reason=%s "
            "rid=%s uid=%s cid=%s fid=%s upload_id=%s document_id=%s"
        ),
        requested_mode,
        served_mode,
        str(shadow_enabled).lower(),
        fallback_reason,
        primary_graph.get("analytics_engine_graph_run_id"),
        ",".join([str(node) for node in list(primary_graph.get("analytics_engine_graph_node_path") or [])]),
        primary_graph.get("analytics_engine_graph_attempts"),
        primary_graph.get("analytics_engine_graph_stop_reason"),
        str(rollback_mode_used).lower(),
        legacy_activation_reason,
        request_id_ctx.get() or "-",
        user_id_ctx.get() or "-",
        conversation_id_ctx.get() or "-",
        file_id or "-",
        upload_id or "-",
        document_id or "-",
    )

    return _attach_engine_debug(
        payload=primary_payload,
        requested_mode=requested_mode,
        served_mode=served_mode,
        shadow_enabled=shadow_enabled,
        shadow_summary=shadow_summary,
        fallback_reason=fallback_reason,
        rollback_mode_used=rollback_mode_used,
        legacy_activation_reason=legacy_activation_reason,
    )

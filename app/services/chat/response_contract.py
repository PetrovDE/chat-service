from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence


ALLOWED_MODEL_ROUTES = {"aihub_primary", "ollama_fallback", "aihub", "ollama", "openai"}
ALLOWED_ROUTE_MODES = {"explicit", "policy"}
ALLOWED_PROVIDER_EFFECTIVE = {"aihub", "ollama", "openai", "none", "unknown"}
ALLOWED_ROUTE_FALLBACK_REASONS = {"none", "timeout", "network", "hub_5xx", "circuit_open"}
ALLOWED_EXECUTION_ROUTES = {"tabular_sql", "complex_analytics", "narrative", "clarification", "unknown"}
ALLOWED_EXECUTOR_STATUS = {"not_attempted", "success", "error", "timeout", "blocked", "fallback"}
CHART_ROUTES = {"chart", "trend", "comparison"}


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _as_dict(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _derive_model_route(*, provider_effective: str) -> str:
    if provider_effective == "ollama":
        return "ollama"
    if provider_effective == "openai":
        return "openai"
    if provider_effective == "aihub":
        return "aihub_primary"
    return "aihub_primary"


def normalize_route_telemetry(route_telemetry: Mapping[str, Any]) -> Dict[str, Any]:
    raw_provider_effective = str(route_telemetry.get("provider_effective") or "aihub").strip().lower()
    provider_effective = raw_provider_effective if raw_provider_effective in ALLOWED_PROVIDER_EFFECTIVE else "unknown"

    raw_route_mode = str(route_telemetry.get("route_mode") or "policy").strip().lower()
    route_mode = raw_route_mode if raw_route_mode in ALLOWED_ROUTE_MODES else "policy"

    raw_model_route = str(route_telemetry.get("model_route") or "").strip().lower()
    model_route = raw_model_route if raw_model_route in ALLOWED_MODEL_ROUTES else _derive_model_route(
        provider_effective=provider_effective
    )

    raw_fallback_reason = str(route_telemetry.get("fallback_reason") or "none").strip().lower()
    fallback_reason = raw_fallback_reason if raw_fallback_reason in ALLOWED_ROUTE_FALLBACK_REASONS else "none"

    return {
        "model_route": model_route,
        "route_mode": route_mode,
        "provider_selected": route_telemetry.get("provider_selected"),
        "provider_effective": provider_effective,
        "fallback_reason": fallback_reason,
        "fallback_allowed": bool(route_telemetry.get("fallback_allowed", False)),
        "fallback_attempted": bool(route_telemetry.get("fallback_attempted", False)),
        "fallback_policy_version": str(route_telemetry.get("fallback_policy_version") or "unknown"),
        "aihub_attempted": bool(route_telemetry.get("aihub_attempted", False)),
    }


def normalize_execution_telemetry(
    execution_telemetry: Mapping[str, Any],
    *,
    default_execution_route: str = "unknown",
    default_executor_status: str = "not_attempted",
) -> Dict[str, Any]:
    raw_route = str(execution_telemetry.get("execution_route") or default_execution_route).strip().lower()
    execution_route = raw_route if raw_route in ALLOWED_EXECUTION_ROUTES else "unknown"

    raw_status = str(execution_telemetry.get("executor_status") or default_executor_status).strip().lower()
    executor_status = raw_status if raw_status in ALLOWED_EXECUTOR_STATUS else default_executor_status

    artifacts_count = max(0, _safe_int(execution_telemetry.get("artifacts_count", 0), default=0))

    return {
        "execution_route": execution_route,
        "executor_attempted": bool(execution_telemetry.get("executor_attempted", False)),
        "executor_status": executor_status,
        "executor_error_code": execution_telemetry.get("executor_error_code"),
        "artifacts_count": artifacts_count,
    }


def _infer_response_mode(
    *,
    execution_route: str,
    selected_route: str,
    retrieval_mode: str,
    file_resolution_status: str,
    clarification_required: bool,
    controlled_response_state: str | None,
    chart_spec_generated: bool,
) -> str:
    if controlled_response_state == "runtime_error":
        return "runtime_error"
    if clarification_required:
        return "clarification"
    if execution_route == "complex_analytics":
        return "complex_analytics"
    if execution_route == "tabular_sql":
        if selected_route in CHART_ROUTES or chart_spec_generated:
            return "chart"
        return "tabular"
    if selected_route == "general_chat" or retrieval_mode == "assistant_direct":
        return "general_chat"
    if file_resolution_status in {"conversation_match", "resolved_unique", "skipped_explicit_file_ids"}:
        return "file_aware"
    if execution_route == "narrative":
        return "narrative"
    return "unknown"


def build_response_contract(
    *,
    rag_debug: Mapping[str, Any] | None,
    execution_telemetry: Mapping[str, Any],
    artifacts: Sequence[Mapping[str, Any]] | None,
    debug_enabled: bool,
    debug_included: bool,
) -> Dict[str, Any]:
    rag_debug_payload = _as_dict(rag_debug)
    normalized_execution = normalize_execution_telemetry(execution_telemetry)
    execution_route = str(
        rag_debug_payload.get("execution_route")
        or normalized_execution.get("execution_route")
        or "unknown"
    ).strip().lower()
    if execution_route not in ALLOWED_EXECUTION_ROUTES:
        execution_route = "unknown"

    selected_route = str(rag_debug_payload.get("selected_route") or "unknown").strip().lower() or "unknown"
    retrieval_mode = str(rag_debug_payload.get("retrieval_mode") or "unknown").strip().lower() or "unknown"
    file_resolution_status = str(rag_debug_payload.get("file_resolution_status") or "not_requested").strip().lower()
    fallback_type = str(rag_debug_payload.get("fallback_type") or "none").strip().lower() or "none"
    fallback_reason = str(rag_debug_payload.get("fallback_reason") or "none").strip().lower() or "none"
    controlled_response_state_raw = str(rag_debug_payload.get("controlled_response_state") or "").strip().lower()
    controlled_response_state = controlled_response_state_raw or None
    clarification_required = bool(
        rag_debug_payload.get("requires_clarification", False) or execution_route == "clarification"
    )
    chart_spec_generated = bool(rag_debug_payload.get("chart_spec_generated", False))
    chart_artifact_available = bool(
        rag_debug_payload.get("chart_artifact_available", rag_debug_payload.get("chart_artifact_exists", False))
    )

    artifacts_list = _as_list(artifacts)
    artifacts_available = bool(artifacts_list)
    artifacts_count = max(
        0,
        _safe_int(
            normalized_execution.get("artifacts_count", len(artifacts_list)),
            default=len(artifacts_list),
        ),
    )

    response_mode = _infer_response_mode(
        execution_route=execution_route,
        selected_route=selected_route,
        retrieval_mode=retrieval_mode,
        file_resolution_status=file_resolution_status,
        clarification_required=clarification_required,
        controlled_response_state=controlled_response_state,
        chart_spec_generated=chart_spec_generated,
    )

    controlled_fallback = bool(
        clarification_required
        or fallback_type != "none"
        or fallback_reason != "none"
        or controlled_response_state not in {None, "none", "chart_render_success"}
    )

    return {
        "contract_version": "chat_response_v1",
        "response_mode": response_mode,
        "execution_route": execution_route,
        "selected_route": selected_route,
        "retrieval_mode": retrieval_mode,
        "file_resolution_status": file_resolution_status,
        "clarification_required": clarification_required,
        "controlled_fallback": controlled_fallback,
        "controlled_response_state": controlled_response_state,
        "fallback_type": fallback_type,
        "fallback_reason": fallback_reason,
        "artifacts_available": artifacts_available,
        "artifacts_count": artifacts_count,
        "chart_artifact_available": chart_artifact_available,
        "debug_enabled": bool(debug_enabled),
        "debug_included": bool(debug_included),
    }

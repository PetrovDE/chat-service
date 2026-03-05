from __future__ import annotations

from typing import Optional

from app.observability.metrics import inc_counter, set_gauge


def _to_bool_label(value: bool) -> str:
    return "true" if bool(value) else "false"


def _normalize_ratio(value: float) -> float:
    ratio = float(value)
    if ratio < 0.0:
        return 0.0
    if ratio > 1.0:
        return 1.0
    return ratio


def _coverage_bucket(ratio: float) -> str:
    if ratio < 0.5:
        return "lt_50"
    if ratio < 0.8:
        return "ge_50_lt_80"
    if ratio < 0.95:
        return "ge_80_lt_95"
    return "ge_95"


def _planner_route_class(route: str) -> str:
    normalized = str(route or "").strip().lower()
    if normalized == "deterministic_analytics":
        return "deterministic"
    if normalized == "complex_analytics":
        return "complex_analytics"
    if normalized == "narrative_retrieval":
        return "narrative"
    return "unknown"


def observe_llm_route_decision(
    *,
    route: str,
    fallback_reason: str,
    fallback_allowed: bool,
    fallback_policy_version: str,
    route_mode: str = "policy",
    provider_effective: str = "unknown",
    aihub_attempted: bool = False,
    fallback_attempted: bool = False,
) -> None:
    route_value = str(route or "unknown")
    reason_value = str(fallback_reason or "none")
    policy_version = str(fallback_policy_version or "unknown")
    allowed_label = _to_bool_label(fallback_allowed)
    route_mode_value = str(route_mode or "unknown")
    provider_effective_value = str(provider_effective or "unknown")

    inc_counter(
        "llama_service_llm_route_decisions_total",
        route=route_value,
        route_mode=route_mode_value,
        provider_effective=provider_effective_value,
        aihub_attempted=_to_bool_label(aihub_attempted),
        fallback_attempted=_to_bool_label(fallback_attempted),
        fallback_reason=reason_value,
        fallback_allowed=allowed_label,
        fallback_policy_version=policy_version,
    )
    if route_value == "ollama_fallback":
        inc_counter(
            "llama_service_llm_fallback_total",
            fallback_reason=reason_value,
            fallback_policy_version=policy_version,
        )


def observe_planner_decision(
    *,
    route: str,
    intent: str,
    requires_clarification: bool,
    metric_critical: bool,
) -> None:
    route_value = str(route or "unknown")
    route_class = _planner_route_class(route_value)
    intent_value = str(intent or "unknown")

    inc_counter(
        "llama_service_query_planner_route_total",
        route=route_value,
        route_class=route_class,
        intent=intent_value,
        requires_clarification=_to_bool_label(requires_clarification),
        metric_critical=_to_bool_label(metric_critical),
    )


def observe_ingestion_enqueue(*, mode: str, deduplicated: bool) -> None:
    inc_counter(
        "llama_service_ingestion_enqueue_total",
        mode=str(mode or "unknown"),
        result="deduplicated" if deduplicated else "enqueued",
    )


def observe_ingestion_retry() -> None:
    inc_counter("llama_service_ingestion_retries_total")


def set_ingestion_queue_snapshot(
    *,
    depth: float,
    processing: float,
    dead_letter_depth: float,
    lag_seconds: float,
    heartbeat_age_seconds: Optional[float] = None,
) -> None:
    set_gauge("llama_service_ingestion_queue_depth", float(depth))
    set_gauge("llama_service_ingestion_queue_processing", float(processing))
    set_gauge("llama_service_ingestion_dead_letter_depth", float(dead_letter_depth))
    set_gauge("llama_service_ingestion_queue_lag_seconds", float(lag_seconds))
    if heartbeat_age_seconds is not None:
        set_gauge("llama_service_ingestion_worker_heartbeat_age_seconds", float(heartbeat_age_seconds))


def observe_retrieval_coverage(
    *,
    coverage_ratio: float,
    retrieval_mode: str,
    expected_chunks: int,
    retrieved_chunks: int,
) -> None:
    ratio = _normalize_ratio(coverage_ratio)
    mode = str(retrieval_mode or "unknown")
    expected = max(0, int(expected_chunks))
    retrieved = max(0, int(retrieved_chunks))
    bucket = _coverage_bucket(ratio)

    set_gauge("llama_service_retrieval_coverage_ratio", ratio, retrieval_mode=mode)
    set_gauge("llama_service_retrieval_expected_chunks", float(expected), retrieval_mode=mode)
    set_gauge("llama_service_retrieval_retrieved_chunks", float(retrieved), retrieval_mode=mode)
    inc_counter(
        "llama_service_retrieval_coverage_events_total",
        retrieval_mode=mode,
        bucket=bucket,
    )


def observe_tabular_row_coverage(
    *,
    coverage_ratio: float,
    retrieval_mode: str,
    rows_expected_total: int,
    rows_retrieved_total: int,
) -> None:
    ratio = _normalize_ratio(coverage_ratio)
    mode = str(retrieval_mode or "unknown")
    rows_expected = max(0, int(rows_expected_total))
    rows_retrieved = max(0, int(rows_retrieved_total))
    bucket = _coverage_bucket(ratio)

    set_gauge("llama_service_tabular_row_coverage_ratio", ratio, retrieval_mode=mode)
    set_gauge("llama_service_tabular_rows_expected_total", float(rows_expected), retrieval_mode=mode)
    set_gauge("llama_service_tabular_rows_retrieved_total", float(rows_retrieved), retrieval_mode=mode)
    inc_counter(
        "llama_service_tabular_row_coverage_events_total",
        retrieval_mode=mode,
        bucket=bucket,
    )

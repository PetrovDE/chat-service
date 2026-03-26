from __future__ import annotations

from typing import Any, Dict, Tuple


def ensure_tabular_debug_containers(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    debug = payload.setdefault("debug", {})
    if not isinstance(debug, dict):
        debug = {}
        payload["debug"] = debug
    tabular_debug = debug.setdefault("tabular_sql", {})
    if not isinstance(tabular_debug, dict):
        tabular_debug = {}
        debug["tabular_sql"] = tabular_debug
    return debug, tabular_debug


def apply_tabular_debug_fields(payload: Dict[str, Any], *, fields: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    debug, tabular_debug = ensure_tabular_debug_containers(payload)
    for key, value in fields.items():
        debug[key] = value
        tabular_debug[key] = value
    return payload


def build_route_debug_fields(
    *,
    detected_intent: str,
    selected_route: str,
    requested_field_text: str | None,
    candidate_columns: Any,
    scored_candidates: Any,
    matched_column: str | None,
    match_score: Any,
    match_strategy: str | None,
    fallback_type: str,
    fallback_reason: str,
    detected_language: str,
    response_language: str,
    requested_time_grain: str | None = None,
    source_datetime_field: str | None = None,
    derived_temporal_dimension: str | None = None,
    temporal_plan_status: str = "not_requested",
    temporal_aggregation_plan: Any = None,
) -> Dict[str, Any]:
    if not isinstance(candidate_columns, list):
        candidate_columns = []
    if not isinstance(scored_candidates, list):
        scored_candidates = []
    try:
        normalized_match_score = float(match_score) if match_score is not None else None
    except Exception:
        normalized_match_score = None
    normalized_temporal_plan = temporal_aggregation_plan if isinstance(temporal_aggregation_plan, dict) else {}
    return {
        "detected_intent": str(detected_intent or "unknown"),
        "selected_route": str(selected_route or "unknown"),
        "requested_field_text": str(requested_field_text or "") or None,
        "candidate_columns": [str(item) for item in candidate_columns],
        "scored_candidates": scored_candidates,
        "matched_column": str(matched_column or "") or None,
        "match_score": normalized_match_score,
        "match_strategy": str(match_strategy or "none"),
        "fallback_type": str(fallback_type or "none"),
        "fallback_reason": str(fallback_reason or "none"),
        "detected_language": str(detected_language or "ru"),
        "response_language": str(response_language or detected_language or "ru"),
        "requested_time_grain": str(requested_time_grain or "") or None,
        "source_datetime_field": str(source_datetime_field or "") or None,
        "derived_temporal_dimension": str(derived_temporal_dimension or "") or None,
        "temporal_plan_status": str(temporal_plan_status or "not_requested"),
        "temporal_aggregation_plan": normalized_temporal_plan,
    }


def build_dataset_debug_fields(*, dataset: Any, table: Any) -> Dict[str, Any]:
    dataset_stats_raw = getattr(dataset, "column_metadata_stats", None)
    table_stats_raw = getattr(table, "column_metadata_stats", None)
    dataset_metadata_stats = dataset_stats_raw if isinstance(dataset_stats_raw, dict) else {}
    table_metadata_stats = table_stats_raw if isinstance(table_stats_raw, dict) else {}
    effective_stats = table_metadata_stats or dataset_metadata_stats
    contract_version = (
        getattr(dataset, "column_metadata_contract_version", None)
        or getattr(table, "column_metadata_contract_version", None)
    )
    return {
        "storage_engine": getattr(dataset, "engine", None),
        "dataset_id": getattr(dataset, "dataset_id", None),
        "dataset_version": getattr(dataset, "dataset_version", None),
        "dataset_provenance_id": getattr(dataset, "dataset_provenance_id", None),
        "table_name": getattr(table, "table_name", None),
        "sheet_name": getattr(table, "sheet_name", None),
        "table_version": getattr(table, "table_version", None),
        "table_provenance_id": getattr(table, "provenance_id", None),
        "table_row_count": int(getattr(table, "row_count", 0) or 0),
        "column_metadata_contract_version": contract_version,
        "column_metadata_present": bool(int(effective_stats.get("columns_with_metadata", 0) or 0) > 0),
        "column_metadata_columns_total": int(effective_stats.get("columns_total", 0) or 0),
        "column_metadata_columns_with_metadata": int(effective_stats.get("columns_with_metadata", 0) or 0),
        "column_metadata_aliases_total": int(effective_stats.get("aliases_total", 0) or 0),
        "column_metadata_sample_values_total": int(effective_stats.get("sample_values_total", 0) or 0),
        "column_metadata_aliases_trimmed_total": int(effective_stats.get("aliases_trimmed_total", 0) or 0),
        "column_metadata_sample_values_trimmed_total": int(effective_stats.get("sample_values_trimmed_total", 0) or 0),
        "column_metadata_budget_enforced": bool(effective_stats.get("metadata_budget_enforced", False)),
        "dataset_column_metadata_columns_total": int(dataset_metadata_stats.get("columns_total", 0) or 0),
        "dataset_column_metadata_columns_with_metadata": int(dataset_metadata_stats.get("columns_with_metadata", 0) or 0),
        "dataset_column_metadata_aliases_total": int(dataset_metadata_stats.get("aliases_total", 0) or 0),
        "dataset_column_metadata_sample_values_total": int(dataset_metadata_stats.get("sample_values_total", 0) or 0),
    }

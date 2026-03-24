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
) -> Dict[str, Any]:
    if not isinstance(candidate_columns, list):
        candidate_columns = []
    if not isinstance(scored_candidates, list):
        scored_candidates = []
    try:
        normalized_match_score = float(match_score) if match_score is not None else None
    except Exception:
        normalized_match_score = None
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
    }


def build_dataset_debug_fields(*, dataset: Any, table: Any) -> Dict[str, Any]:
    return {
        "storage_engine": getattr(dataset, "engine", None),
        "dataset_id": getattr(dataset, "dataset_id", None),
        "dataset_version": getattr(dataset, "dataset_version", None),
        "dataset_provenance_id": getattr(dataset, "dataset_provenance_id", None),
        "table_name": getattr(table, "table_name", None),
        "table_version": getattr(table, "table_version", None),
        "table_provenance_id": getattr(table, "provenance_id", None),
        "table_row_count": int(getattr(table, "row_count", 0) or 0),
    }


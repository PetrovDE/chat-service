from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List, Sequence


_SENSITIVE_KEYS = {
    "authorization",
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
    "token",
    "password",
    "secret",
    "client_secret",
    "cookie",
    "set-cookie",
}
_SENSITIVE_VALUE_RE = re.compile(
    r"(?i)(bearer\s+[a-z0-9._\-]+|sk-[a-z0-9]{8,}|ghp_[a-z0-9]{12,})"
)


def _json_hash(payload: Dict[str, Any]) -> str:
    try:
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    except Exception:
        raw = str(payload)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _to_compact_str_list(values: Any, *, limit: int = 24) -> List[str]:
    out: List[str] = []
    seen = set()
    if not isinstance(values, list):
        return out
    for item in values:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
        if len(out) >= limit:
            break
    return out


def _derive_retrieval_skip(payload: Dict[str, Any]) -> tuple[bool, str]:
    retrieval_mode = str(payload.get("retrieval_mode") or "").strip().lower()
    retrieval_path = str(payload.get("retrieval_path") or "").strip().lower()
    skip_reasons = {
        "assistant_direct": "general_chat_route",
        "clarification": "clarification_route",
        "no_context_files": "no_context_files",
        "file_resolution": "file_resolution_clarification",
    }
    if retrieval_mode in skip_reasons:
        return True, skip_reasons[retrieval_mode]
    if retrieval_path == "structured" and retrieval_mode in {"tabular_sql", "tabular_combined", "complex_analytics"}:
        return True, "structured_execution_path"
    if retrieval_mode == "narrative_no_retrieval":
        return False, "no_relevant_chunks"
    if retrieval_mode == "narrative_error":
        return False, "retrieval_runtime_error"
    return False, "none"


def _derive_deterministic_fallback_formatting(payload: Dict[str, Any]) -> tuple[bool, str]:
    complex_debug = payload.get("complex_analytics") if isinstance(payload.get("complex_analytics"), dict) else {}
    response_status = str(complex_debug.get("response_status") or "").strip().lower()
    response_error_code = str(complex_debug.get("response_error_code") or "").strip().lower()
    if response_status == "fallback":
        return True, response_error_code or "complex_analytics_local_formatter"
    if response_error_code in {"broad_query_local_formatter", "low_content_quality"}:
        return True, response_error_code
    return False, "none"


def _build_execution_spec_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(payload.get("execution_spec_summary"), dict):
        return dict(payload["execution_spec_summary"])
    tabular_sql = payload.get("tabular_sql") if isinstance(payload.get("tabular_sql"), dict) else {}
    measure = tabular_sql.get("measure") if isinstance(tabular_sql.get("measure"), dict) else {}
    dimension = tabular_sql.get("dimension") if isinstance(tabular_sql.get("dimension"), dict) else {}
    filters = tabular_sql.get("filters") if isinstance(tabular_sql.get("filters"), list) else []
    output_columns = tabular_sql.get("output_columns") if isinstance(tabular_sql.get("output_columns"), list) else []
    return {
        "selected_route": str(payload.get("selected_route") or "unknown"),
        "requested_output_type": str(payload.get("requested_output_type") or "table"),
        "measure_field": str(measure.get("field") or "").strip() or None,
        "dimension_field": str(dimension.get("field") or "").strip() or None,
        "filters_count": len(filters),
        "output_columns_count": len(output_columns),
    }


def _extract_filter_values(filters: Dict[str, Any], key: str) -> List[str]:
    if not isinstance(filters, dict):
        return []
    raw = filters.get(key)
    if isinstance(raw, dict):
        if isinstance(raw.get("$in"), list):
            return [str(item).strip() for item in raw.get("$in") if str(item).strip()]
        if raw.get("$eq") is not None:
            value = str(raw.get("$eq")).strip()
            return [value] if value else []
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    if raw is None:
        return []
    value = str(raw).strip()
    return [value] if value else []


def _derive_retrieval_scope(payload: Dict[str, Any]) -> Dict[str, Any]:
    if isinstance(payload.get("retrieval_scope"), dict):
        return dict(payload["retrieval_scope"])
    filters = payload.get("retrieval_filters")
    if not isinstance(filters, dict):
        filters = payload.get("filters")
    if not isinstance(filters, dict):
        filters = payload.get("where")
    if not isinstance(filters, dict):
        return {}
    file_ids = _extract_filter_values(filters, "file_id")
    processing_ids = _extract_filter_values(filters, "processing_id")
    sheet_names = _extract_filter_values(filters, "sheet_name")
    table_names = _extract_filter_values(filters, "table_name")
    chunk_types = _extract_filter_values(filters, "chunk_type")
    namespace = _extract_filter_values(filters, "namespace")
    scope: Dict[str, Any] = {}
    if file_ids:
        scope["file_ids"] = file_ids
    if processing_ids:
        scope["processing_ids"] = processing_ids
    if sheet_names:
        scope["sheet_names"] = sheet_names
    if table_names:
        scope["table_names"] = table_names
    if chunk_types:
        scope["chunk_types"] = chunk_types
    if namespace:
        scope["namespaces"] = namespace
    return scope


def _sanitize_recursive(value: Any, *, key_hint: str = "") -> Any:
    lowered_key = key_hint.strip().lower()
    if lowered_key in _SENSITIVE_KEYS:
        return "<redacted>"

    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            out[str(key)] = _sanitize_recursive(item, key_hint=str(key))
        return out
    if isinstance(value, list):
        return [_sanitize_recursive(item, key_hint=key_hint) for item in value]
    if isinstance(value, str):
        if _SENSITIVE_VALUE_RE.search(value):
            return "<redacted>"
        return value
    return value


def apply_stage4_observability_enrichment(
    *,
    payload: Dict[str, Any],
    context_docs: Sequence[Dict[str, Any]],
    rag_sources: Sequence[str],
) -> Dict[str, Any]:
    correlation = {
        "request_id": str(payload.get("request_id") or "").strip() or None,
        "conversation_id": str(payload.get("conversation_id") or "").strip() or None,
        "user_id": str(payload.get("user_id") or "").strip() or None,
        "file_id": str(payload.get("file_id") or payload.get("scope_selected_file_id") or "").strip() or None,
        "upload_id": str(payload.get("upload_id") or "").strip() or None,
        "document_id": str(payload.get("document_id") or "").strip() or None,
    }
    payload.update(correlation)

    retrieval_skipped, retrieval_skip_reason = _derive_retrieval_skip(payload)
    payload["retrieval_skipped"] = bool(retrieval_skipped)
    payload["retrieval_skip_reason"] = str(retrieval_skip_reason or "none")
    payload["retrieval_scope"] = _derive_retrieval_scope(payload)

    plan_json = payload.get("analytic_plan_json") if isinstance(payload.get("analytic_plan_json"), dict) else {}
    payload["plan_hash"] = str(payload.get("plan_hash") or (_json_hash(plan_json) if plan_json else "")).strip() or None
    payload["plan_summary"] = (
        dict(payload.get("plan_summary"))
        if isinstance(payload.get("plan_summary"), dict)
        else {
            "task_type": str(plan_json.get("task_type") or "unknown") if plan_json else "unknown",
            "selected_route": str(plan_json.get("selected_route") or payload.get("selected_route") or "unknown"),
            "requested_output_type": str(plan_json.get("requested_output_type") or payload.get("requested_output_type") or "table"),
            "measure_field": str(((plan_json.get("measure") or {}).get("field") or "")).strip() or None,
            "dimension_field": str(((plan_json.get("dimension") or {}).get("field") or "")).strip() or None,
            "filters_count": len(list(plan_json.get("filters") or [])),
        }
    )
    payload["execution_spec_summary"] = _build_execution_spec_summary(payload)
    payload["plan_validation_failures"] = _to_compact_str_list(payload.get("plan_validation_failures"))
    payload["execution_spec_validation_failures"] = _to_compact_str_list(payload.get("execution_spec_validation_failures"))
    payload["executed_tools"] = _to_compact_str_list(payload.get("executed_tools"))
    payload["tool_errors"] = _to_compact_str_list(payload.get("tool_errors"))
    payload["planner_path"] = str(payload.get("planner_mode") or "deterministic")

    context_chars = int(sum(len(str((doc.get("content") or ""))) for doc in list(context_docs or [])))
    payload["context_chars"] = context_chars
    payload["source_count"] = int(len(list(rag_sources or [])))
    payload["artifacts_present"] = bool(int(payload.get("artifacts_count", 0) or 0) > 0)
    used_deterministic_fallback, fallback_reason = _derive_deterministic_fallback_formatting(payload)
    payload["deterministic_fallback_formatting_used"] = bool(used_deterministic_fallback)
    payload["deterministic_fallback_formatting_reason"] = str(fallback_reason or "none")

    cache_supported = bool(payload.get("cache_supported", False))
    cache_active = bool(payload.get("cache_active", False)) and cache_supported
    if not cache_active:
        payload["cache_hit"] = False
        payload["cache_miss"] = False
        payload["cache_supported"] = cache_supported
        payload["cache_active"] = False
        payload["cache_status"] = "inactive"
        payload["cache_reason"] = str(payload.get("cache_reason") or "response_cache_not_implemented")

    debug_sections = payload.get("debug_sections") if isinstance(payload.get("debug_sections"), dict) else {}
    debug_sections["correlation"] = correlation
    retrieval_section = debug_sections.get("retrieval") if isinstance(debug_sections.get("retrieval"), dict) else {}
    retrieval_section.update(
        {
            "retrieval_skipped": payload["retrieval_skipped"],
            "retrieval_skip_reason": payload["retrieval_skip_reason"],
            "source_count": payload["source_count"],
            "retrieval_scope": payload.get("retrieval_scope") or {},
            "source_evidence_summary": list(rag_sources or [])[:8],
            "row_coverage_ratio": float(payload.get("row_coverage_ratio", 0.0) or 0.0),
            "rows_expected_total": int(payload.get("rows_expected_total", 0) or 0),
            "rows_retrieved_total": int(payload.get("rows_retrieved_total", 0) or 0),
        }
    )
    debug_sections["retrieval"] = retrieval_section
    planner_section = debug_sections.get("planner") if isinstance(debug_sections.get("planner"), dict) else {}
    planner_section.update(
        {
            "planner_path": payload["planner_path"],
            "plan_hash": payload["plan_hash"],
            "plan_summary": payload["plan_summary"],
            "plan_validation_failures": payload["plan_validation_failures"],
            "execution_spec_summary": payload["execution_spec_summary"],
            "execution_spec_validation_failures": payload["execution_spec_validation_failures"],
            "executed_tools": payload["executed_tools"],
            "tool_errors": payload["tool_errors"],
            "graph_run_id": payload.get("analytics_engine_graph_run_id"),
            "graph_node_path": payload.get("analytics_engine_graph_node_path") or payload.get("graph_node_path") or [],
            "graph_attempts": int(payload.get("analytics_engine_graph_attempts", 0) or payload.get("graph_attempts", 0) or 0),
            "stop_reason": str(payload.get("analytics_engine_graph_stop_reason") or payload.get("stop_reason") or "none"),
        }
    )
    debug_sections["planner"] = planner_section
    debug_sections["context"] = {
        "context_chars": payload["context_chars"],
        "context_tokens": int(payload.get("context_tokens", 0) or 0),
        "source_count": payload["source_count"],
        "artifacts_present": payload["artifacts_present"],
        "artifacts_count": int(payload.get("artifacts_count", 0) or 0),
        "chart_artifact_available": bool(payload.get("chart_artifact_available", False)),
        "deterministic_fallback_formatting_used": payload["deterministic_fallback_formatting_used"],
        "deterministic_fallback_formatting_reason": payload["deterministic_fallback_formatting_reason"],
    }
    cache_section = debug_sections.get("cache") if isinstance(debug_sections.get("cache"), dict) else {}
    cache_section.update(
        {
            "cache_hit": bool(payload.get("cache_hit", False)),
            "cache_miss": bool(payload.get("cache_miss", False)),
            "cache_supported": bool(payload.get("cache_supported", False)),
            "cache_active": bool(payload.get("cache_active", False)),
            "cache_status": str(payload.get("cache_status") or "inactive"),
            "cache_reason": str(payload.get("cache_reason") or "response_cache_not_implemented"),
            "cache_key_version": str(payload.get("cache_key_version") or "unknown"),
            "cache_key": payload.get("cache_key"),
        }
    )
    debug_sections["cache"] = cache_section
    payload["debug_sections"] = debug_sections

    return _sanitize_recursive(payload)

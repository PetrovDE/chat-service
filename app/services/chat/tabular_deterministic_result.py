from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
import uuid

from app.domain.chat.query_planner import QueryPlanDecision
from app.observability.slo_metrics import observe_retrieval_coverage, observe_tabular_row_coverage
from app.services.chat.language import apply_language_policy_to_prompt
from app.services.chat.tabular_answer_shaper import build_tabular_answer_quality_guidance
from app.services.chat.tabular_response_composer import build_chart_response_text


RagPromptResult = Tuple[str, bool, Dict[str, Any], List[Dict[str, Any]], List[str], List[str]]
logger = logging.getLogger(__name__)


def _resolve_chart_column_label(*, rag_debug: Dict[str, Any]) -> str:
    tabular_debug = rag_debug.get("tabular_sql") if isinstance(rag_debug.get("tabular_sql"), dict) else {}
    chart_spec = tabular_debug.get("chart_spec") if isinstance(tabular_debug, dict) else {}
    if isinstance(chart_spec, dict):
        for key in (
            "matched_chart_field_alias",
            "matched_chart_field",
            "requested_chart_field",
            "requested_dimension_column",
        ):
            value = str(chart_spec.get(key) or "").strip()
            if value:
                return value
    matched_column = str(rag_debug.get("matched_chart_field") or "").strip()
    if matched_column:
        return matched_column
    return "field"


def _normalize_chart_debug_fields(*, rag_debug: Dict[str, Any]) -> Dict[str, Any]:
    tabular_debug = rag_debug.get("tabular_sql") if isinstance(rag_debug.get("tabular_sql"), dict) else {}
    chart_keys = (
        "requested_chart_field",
        "matched_chart_field",
        "chart_spec_generated",
        "chart_rendered",
        "chart_artifact_path",
        "chart_artifact_id",
        "chart_artifact_available",
        "chart_artifact_exists",
        "chart_fallback_reason",
        "response_language",
    )
    for key in chart_keys:
        if rag_debug.get(key) is None and isinstance(tabular_debug, dict) and key in tabular_debug:
            rag_debug[key] = tabular_debug.get(key)
    chart_artifact_available = bool(
        rag_debug.get("chart_artifact_available", rag_debug.get("chart_artifact_exists", False))
    )
    rag_debug["chart_artifact_available"] = chart_artifact_available
    rag_debug["chart_artifact_exists"] = chart_artifact_available
    return rag_debug


def _resolve_source_scope(*, rag_debug: Dict[str, Any]) -> str:
    file_name = str(rag_debug.get("scope_selected_file_name") or "").strip()
    table_name = str(rag_debug.get("scope_selected_table_name") or "").strip()
    sheet_name = str(rag_debug.get("scope_selected_sheet_name") or "").strip()
    if not table_name:
        tabular_debug = rag_debug.get("tabular_sql") if isinstance(rag_debug.get("tabular_sql"), dict) else {}
        table_name = str(tabular_debug.get("table_name") or "").strip()
    if not sheet_name:
        tabular_debug = rag_debug.get("tabular_sql") if isinstance(rag_debug.get("tabular_sql"), dict) else {}
        sheet_name = str(tabular_debug.get("sheet_name") or "").strip()
    parts: List[str] = []
    if file_name:
        parts.append(file_name)
    if sheet_name:
        parts.append(f"sheet={sheet_name}")
    if table_name:
        parts.append(f"table={table_name}")
    return " | ".join(parts)


def _build_chart_short_circuit_response(
    *,
    preferred_lang: str,
    tabular_sql_result: Dict[str, Any],
    rag_debug: Dict[str, Any],
) -> str:
    chart_delivery_reason = str(rag_debug.get("chart_fallback_reason") or "none")
    column_label = _resolve_chart_column_label(rag_debug=rag_debug)
    tabular_debug = rag_debug.get("tabular_sql") if isinstance(rag_debug.get("tabular_sql"), dict) else {}
    result_text = str(tabular_debug.get("result") or "").strip()
    chart_artifact_available = bool(rag_debug.get("chart_artifact_available"))
    response_text = build_chart_response_text(
        preferred_lang=preferred_lang,
        column_label=column_label,
        chart_rendered=bool(rag_debug.get("chart_rendered")),
        chart_artifact_available=chart_artifact_available,
        chart_fallback_reason=chart_delivery_reason,
        result_text=result_text,
        source_scope=_resolve_source_scope(rag_debug=rag_debug),
    )
    if not response_text:
        response_text = str(tabular_sql_result.get("chart_response_text") or "").strip()
    return response_text


def build_tabular_success_route_result(
    *,
    query: str,
    user_id: uuid.UUID,
    conversation_id: uuid.UUID,
    files: List[Any],
    planner_decision: QueryPlanDecision,
    planner_decision_payload: Dict[str, Any],
    expected_chunks_total: int,
    rag_mode: Optional[str],
    top_k: int,
    preferred_lang: str,
    is_combined_intent: bool,
    tabular_sql_result: Dict[str, Any],
    processing_ids_by_file: Dict[str, str],
    combined_context_docs: List[Dict[str, Any]],
    combined_debug: Dict[str, Any],
) -> RagPromptResult:
    retrieval_mode = "tabular_combined" if is_combined_intent else "tabular_sql"
    file_ids = [str(file_obj.id) for file_obj in files]
    rag_sources = list(tabular_sql_result.get("sources") or [])
    rag_debug = dict(tabular_sql_result.get("debug") or {})
    rag_debug["planner_decision"] = planner_decision_payload
    rag_debug["file_ids"] = file_ids
    rag_debug["retrieval_mode"] = retrieval_mode
    rag_debug["rag_mode"] = rag_mode or "auto"
    rag_debug["rag_mode_effective"] = retrieval_mode
    rag_debug["execution_route"] = "tabular_sql"
    rag_debug["detected_intent"] = str(
        rag_debug.get("detected_intent")
        or rag_debug.get("intent")
        or planner_decision_payload.get("intent")
        or "tabular_sql"
    )
    rag_debug["selected_route"] = str(
        rag_debug.get("selected_route")
        or planner_decision_payload.get("route")
        or retrieval_mode
    )
    rag_debug["executor_attempted"] = False
    rag_debug["executor_status"] = "not_attempted"
    rag_debug["executor_error_code"] = None
    rag_debug["artifacts"] = list(tabular_sql_result.get("artifacts") or [])
    rag_debug["artifacts_count"] = int(len(rag_debug["artifacts"]))
    rag_debug["analytical_mode_used"] = True
    rag_debug["strategy_mode"] = "combined" if is_combined_intent else "analytical"
    selected_route_value = str(rag_debug.get("selected_route") or "")
    if selected_route_value in {"chart", "trend", "comparison"}:
        rag_debug = _normalize_chart_debug_fields(rag_debug=rag_debug)
        rag_debug["short_circuit_response"] = True
        rag_debug["short_circuit_response_text"] = _build_chart_short_circuit_response(
            preferred_lang=preferred_lang,
            tabular_sql_result=tabular_sql_result,
            rag_debug=rag_debug,
        )
        chart_artifact_exists = bool(rag_debug.get("chart_artifact_available", False))
        if chart_artifact_exists:
            rag_debug["fallback_type"] = "none"
            rag_debug["fallback_reason"] = "none"
            rag_debug["controlled_response_state"] = "chart_render_success"
        else:
            rag_debug["fallback_type"] = "tabular_chart_render_failed"
            rag_debug["fallback_reason"] = str(rag_debug.get("chart_fallback_reason") or "chart_render_failed")
            rag_debug["controlled_response_state"] = "chart_render_failed"
            rag_debug["artifacts"] = []
            rag_debug["artifacts_count"] = 0
    else:
        rag_debug["fallback_type"] = "none"
        rag_debug["fallback_reason"] = "none"

    if combined_debug:
        rag_debug["combined_scope"] = combined_debug
    rag_debug["retrieval_policy"] = {
        "mode": retrieval_mode,
        "query_profile": planner_decision.intent,
        "requested_top_k": top_k,
        "effective_top_k": 0,
        "expected_chunks_total": expected_chunks_total,
        "escalation": {"attempted": False, "applied": False, "reason": None},
        "row_escalation": {"attempted": False, "applied": False, "reason": None},
    }
    rag_debug["retrieved_chunks_total"] = len(combined_context_docs) if is_combined_intent else expected_chunks_total
    rag_debug["coverage"] = {
        "expected_chunks": expected_chunks_total,
        "retrieved_chunks": rag_debug["retrieved_chunks_total"],
        "ratio": (
            float(rag_debug["retrieved_chunks_total"] / expected_chunks_total)
            if expected_chunks_total > 0
            else 0.0
        ),
        "complete": bool(expected_chunks_total == 0 or rag_debug["retrieved_chunks_total"] >= expected_chunks_total),
    }
    rag_debug["rows_expected_total"] = int(tabular_sql_result.get("rows_expected_total", 0) or 0)
    rag_debug["rows_retrieved_total"] = int(tabular_sql_result.get("rows_retrieved_total", 0) or 0)
    rag_debug["rows_used_map_total"] = int(tabular_sql_result.get("rows_used_map_total", 0) or 0)
    rag_debug["rows_used_reduce_total"] = int(tabular_sql_result.get("rows_used_reduce_total", 0) or 0)
    rag_debug["row_coverage_ratio"] = float(tabular_sql_result.get("row_coverage_ratio", 0.0) or 0.0)
    rag_debug["truncated"] = False

    observe_retrieval_coverage(
        coverage_ratio=float(rag_debug["coverage"]["ratio"]),
        retrieval_mode=retrieval_mode,
        expected_chunks=int(rag_debug["coverage"]["expected_chunks"]),
        retrieved_chunks=int(rag_debug["coverage"]["retrieved_chunks"]),
    )
    observe_tabular_row_coverage(
        coverage_ratio=float(rag_debug.get("row_coverage_ratio", 0.0) or 0.0),
        retrieval_mode=retrieval_mode,
        rows_expected_total=int(rag_debug.get("rows_expected_total", 0) or 0),
        rows_retrieved_total=int(rag_debug.get("rows_retrieved_total", 0) or 0),
    )

    contextual_evidence = ""
    if is_combined_intent and combined_context_docs:
        lines: List[str] = []
        for idx, item in enumerate(combined_context_docs[:6], start=1):
            meta = item.get("metadata") or {}
            sheet = str(meta.get("sheet_name") or "")
            chunk_type = str(meta.get("chunk_type") or "")
            content = str(item.get("content") or "").strip()
            if not content:
                continue
            label = f"[{idx}] sheet={sheet or 'n/a'} type={chunk_type or 'chunk'}"
            lines.append(f"{label}\n{content[:300]}")
        if lines:
            contextual_evidence = "Semantic evidence for combined route:\n" + "\n\n".join(lines) + "\n\n"

    quality_guidance = build_tabular_answer_quality_guidance(
        selected_route=selected_route_value,
        tabular_sql_result=tabular_sql_result,
        rag_sources=rag_sources,
    )

    final_prompt = apply_language_policy_to_prompt(
        preferred_lang=preferred_lang,
        prompt=(
            "You are a data analyst.\n"
            "Use deterministic tabular context below as source of truth.\n"
            "Do not change numbers from SQL output.\n"
            f"{quality_guidance}\n\n"
            f"User question:\n{query}\n\n"
            f"{contextual_evidence}"
            f"{tabular_sql_result.get('prompt_context')}\n\n"
            "Final answer:"
        ),
    )

    avg_similarity = 0.0
    if combined_context_docs:
        avg_similarity = float(
            sum(float(item.get("similarity_score", 0.0) or 0.0) for item in combined_context_docs)
            / max(1, len(combined_context_docs))
        )
    logger.info(
        (
            "rag_trace route=%s strategy=%s analytical_mode_used=true retrieval_mode=%s "
            "retrieval_k=%d retrieval_hits=%d avg_similarity=%.4f context_tokens=0 "
            "uid=%s chat_id=%s file_ids=%s processing_ids=%s"
        ),
        "deterministic_analytics",
        rag_debug.get("strategy_mode"),
        retrieval_mode,
        top_k,
        len(combined_context_docs),
        avg_similarity,
        str(user_id),
        str(conversation_id),
        ",".join(file_ids),
        ",".join(processing_ids_by_file.values()),
    )
    return final_prompt, True, rag_debug, combined_context_docs, [], rag_sources

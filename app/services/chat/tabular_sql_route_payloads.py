from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence

from app.services.chat.language import detect_preferred_response_language
from app.services.chat.tabular_debug_contract import (
    apply_tabular_debug_fields,
    build_dataset_debug_fields,
    build_route_debug_fields,
    ensure_tabular_debug_containers,
)
from app.services.chat.tabular_intent_router import (
    TabularIntentDecision,
    suggest_relevant_alternative_columns,
)
from app.services.chat.tabular_response_composer import (
    build_missing_column_message,
    build_scope_clarification_message,
)
from app.services.tabular.sql_execution import ResolvedTabularDataset, ResolvedTabularTable


def build_route_debug_payload(
    *,
    decision: TabularIntentDecision,
    detected_language: str,
) -> Dict[str, Any]:
    selected_route = str(decision.selected_route or "")
    fallback_reason = str(decision.fallback_reason or "none")
    fallback_type = "unsupported_missing_column" if selected_route == "unsupported_missing_column" else "none"
    return build_route_debug_fields(
        detected_intent=str(decision.detected_intent or "unknown"),
        selected_route=selected_route,
        requested_field_text=decision.requested_field_text,
        candidate_columns=list(decision.candidate_columns),
        scored_candidates=list(decision.scored_candidates),
        matched_column=decision.matched_column,
        match_score=decision.match_score,
        match_strategy=decision.match_strategy,
        fallback_type=fallback_type,
        fallback_reason=fallback_reason,
        detected_language=detected_language,
        response_language=detected_language,
    )


def apply_route_debug(
    *,
    payload: Dict[str, Any],
    decision: TabularIntentDecision,
    detected_language: str,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    route_debug = build_route_debug_payload(decision=decision, detected_language=detected_language)
    payload = apply_tabular_debug_fields(payload, fields=route_debug)
    debug, tabular_debug = ensure_tabular_debug_containers(payload)
    debug["matched_columns"] = list(decision.matched_columns)
    debug["unmatched_requested_fields"] = list(decision.unmatched_requested_fields)
    tabular_debug["matched_columns"] = list(decision.matched_columns)
    tabular_debug["unmatched_requested_fields"] = list(decision.unmatched_requested_fields)
    return payload


def build_missing_column_response(
    *,
    query: str,
    decision: TabularIntentDecision,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
) -> Dict[str, Any]:
    preferred_lang = detect_preferred_response_language(query)
    requested_fields = list(decision.unmatched_requested_fields)
    if not requested_fields and decision.requested_field_text:
        requested_fields = [str(decision.requested_field_text)]
    alternatives = _build_missing_column_alternatives(decision=decision, table=table, limit=6)
    clarification_prompt = build_missing_column_message(
        preferred_lang=preferred_lang,
        requested_fields=requested_fields,
        alternatives=alternatives,
        ambiguous=False,
    )

    payload = {
        "status": "error",
        "clarification_prompt": clarification_prompt,
        "prompt_context": (
            "Deterministic tabular routing blocked by schema validation.\n"
            f"route=unsupported_missing_column\n"
            f"unmatched_requested_fields={json.dumps(requested_fields, ensure_ascii=False)}\n"
            f"matched_columns={json.dumps(decision.matched_columns, ensure_ascii=False)}\n"
            f"available_columns={json.dumps(list(table.columns), ensure_ascii=False)}"
        ),
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_missing_column",
            "deterministic_path": True,
            "tabular_sql": {
                **build_dataset_debug_fields(dataset=dataset, table=table),
                "executed_sql": None,
                "sql": None,
                "result": None,
                "policy_decision": {"allowed": False, "reason": "missing_required_columns"},
                "guardrail_flags": [],
                "sql_guardrails": {"valid": False, "reason": "missing_required_columns"},
            },
        },
        "sources": [
            (
                f"{getattr(target_file, 'original_filename', 'unknown')} "
                f"| table={table.table_name} | dataset_v={dataset.dataset_version} "
                "| sql_error=missing_required_columns"
            )
        ],
        "rows_expected_total": int(table.row_count or 0),
        "rows_retrieved_total": 0,
        "rows_used_map_total": 0,
        "rows_used_reduce_total": 0,
        "row_coverage_ratio": 0.0,
    }
    payload = apply_tabular_debug_fields(
        payload,
        fields={
            "requested_field_text": decision.requested_field_text,
            "candidate_columns": list(decision.candidate_columns),
            "scored_candidates": list(decision.scored_candidates),
            "matched_column": decision.matched_column,
            "match_score": decision.match_score,
            "match_strategy": decision.match_strategy or "none",
            "controlled_response_state": "missing_column",
        },
    )
    return apply_route_debug(payload=payload, decision=decision, detected_language=preferred_lang)


def _build_missing_column_alternatives(
    *,
    decision: TabularIntentDecision,
    table: ResolvedTabularTable,
    limit: int,
) -> List[str]:
    ranked = [str(item) for item in list(decision.candidate_columns or []) if str(item or "").strip()]
    broad = suggest_relevant_alternative_columns(table, limit=max(1, int(limit) * 2))
    merged: List[str] = []
    seen = set()
    for item in [*ranked, *broad]:
        value = str(item or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        merged.append(value)
        if len(merged) >= max(1, int(limit)):
            break
    return merged


def build_scope_clarification_response(
    *,
    query: str,
    scope_kind: str,
    scope_options: Sequence[str],
    scope_debug: Dict[str, Any],
) -> Dict[str, Any]:
    preferred_lang = detect_preferred_response_language(query)
    clarification_prompt = build_scope_clarification_message(
        preferred_lang=preferred_lang,
        scope_kind=scope_kind,
        scope_options=scope_options,
    )
    payload = {
        "status": "error",
        "clarification_prompt": clarification_prompt,
        "prompt_context": (
            "Deterministic tabular scope resolution requires clarification.\n"
            f"scope_kind={scope_kind}\n"
            f"scope_options={json.dumps(list(scope_options), ensure_ascii=False)}"
        ),
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_scope_selection",
            "deterministic_path": True,
            "tabular_sql": {
                "executed_sql": None,
                "sql": None,
                "result": None,
                "policy_decision": {"allowed": False, "reason": "scope_ambiguity"},
                "guardrail_flags": [],
                "sql_guardrails": {"valid": False, "reason": "scope_ambiguity"},
            },
        },
        "sources": [],
        "rows_expected_total": 0,
        "rows_retrieved_total": 0,
        "rows_used_map_total": 0,
        "rows_used_reduce_total": 0,
        "row_coverage_ratio": 0.0,
    }
    fields = {
        "selected_route": "ambiguous_data_scope",
        "detected_intent": "tabular_scope_selection",
        "fallback_type": "tabular_scope_ambiguity",
        "fallback_reason": "scope_ambiguity",
        "controlled_response_state": "scope_ambiguity",
        "detected_language": preferred_lang,
        "response_language": preferred_lang,
        "scope_kind": str(scope_kind or "dataset_scope"),
        "scope_options": [str(item) for item in list(scope_options or [])],
    }
    if isinstance(scope_debug, dict):
        fields.update(
            {
                "scope_selection_status": str(scope_debug.get("scope_selection_status") or "scope_ambiguity"),
                "scope_selected_file_id": scope_debug.get("scope_selected_file_id"),
                "scope_selected_file_name": scope_debug.get("scope_selected_file_name"),
                "scope_selected_table_name": scope_debug.get("scope_selected_table_name"),
                "scope_selected_sheet_name": scope_debug.get("scope_selected_sheet_name"),
                "scope_file_candidates": list(scope_debug.get("scope_file_candidates") or []),
                "table_scope_candidates": list(scope_debug.get("table_scope_candidates") or []),
            }
        )
    return apply_tabular_debug_fields(payload, fields=fields)


def build_schema_question_payload(
    *,
    query: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    decision: TabularIntentDecision,
) -> Dict[str, Any]:
    aliases = table.column_aliases if isinstance(table.column_aliases, dict) else {}
    preferred_lang = detect_preferred_response_language(query)
    schema_payload = {
        "table_name": table.table_name,
        "row_count": int(table.row_count or 0),
        "columns": list(table.columns),
        "column_aliases": {str(key): str(value) for key, value in aliases.items()},
    }
    prompt_context = "Deterministic table schema (source of truth):\n" + json.dumps(
        schema_payload,
        ensure_ascii=False,
        indent=2,
    )
    payload = {
        "status": "ok",
        "prompt_context": prompt_context,
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_schema_question",
            "deterministic_path": True,
            "tabular_sql": {
                **build_dataset_debug_fields(dataset=dataset, table=table),
                "executed_sql": [],
                "sql_guardrails": {"valid": True, "reason": "schema_only_route"},
                "schema_payload": schema_payload,
            },
        },
        "sources": [
            (
                f"{getattr(target_file, 'original_filename', 'unknown')} "
                f"| table={table.table_name} | dataset_v={dataset.dataset_version} "
                f"| table_v={table.table_version} | schema"
            )
        ],
        "rows_expected_total": int(table.row_count or 0),
        "rows_retrieved_total": int(table.row_count or 0),
        "rows_used_map_total": int(table.row_count or 0),
        "rows_used_reduce_total": int(table.row_count or 0),
        "row_coverage_ratio": 1.0 if int(table.row_count or 0) > 0 else 0.0,
    }
    return apply_route_debug(payload=payload, decision=decision, detected_language=preferred_lang)

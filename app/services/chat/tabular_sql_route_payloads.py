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
from app.services.chat.tabular_schema_summary_shaper import build_schema_summary_context
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
        requested_time_grain=decision.requested_time_grain,
        source_datetime_field=decision.source_datetime_field,
        derived_temporal_dimension=decision.derived_grouping_dimension,
        temporal_plan_status=decision.temporal_plan_status,
        temporal_aggregation_plan=dict(decision.temporal_aggregation_plan or {}),
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
    existing_debug = payload.get("debug", {}) if isinstance(payload.get("debug"), dict) else {}
    for field_name in (
        "requested_time_grain",
        "source_datetime_field",
        "derived_temporal_dimension",
    ):
        existing_value = existing_debug.get(field_name)
        if existing_value is None:
            continue
        if isinstance(existing_value, str) and not existing_value.strip():
            continue
        if isinstance(existing_value, (list, dict)) and not existing_value:
            continue
        route_debug[field_name] = existing_value
    existing_status = str(existing_debug.get("temporal_plan_status") or "").strip()
    if existing_status and existing_status != "not_requested":
        route_debug["temporal_plan_status"] = existing_status
    existing_plan = existing_debug.get("temporal_aggregation_plan")
    if isinstance(existing_plan, dict) and existing_plan:
        merged_plan = dict(route_debug.get("temporal_aggregation_plan") or {})
        for key, value in existing_plan.items():
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            if isinstance(value, (list, dict)) and not value:
                continue
            merged_plan[key] = value
        route_debug["temporal_aggregation_plan"] = merged_plan
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
            "requested_time_grain": decision.requested_time_grain,
            "source_datetime_field": decision.source_datetime_field,
            "derived_temporal_dimension": decision.derived_grouping_dimension,
            "temporal_plan_status": decision.temporal_plan_status,
            "temporal_aggregation_plan": dict(decision.temporal_aggregation_plan or {}),
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
    table: ResolvedTabularTable | None,
    target_file: Any,
    decision: TabularIntentDecision,
) -> Dict[str, Any]:
    preferred_lang = detect_preferred_response_language(query)
    selected_table = table
    selected_aliases = selected_table.column_aliases if (selected_table and isinstance(selected_table.column_aliases, dict)) else {}
    selected_columns = list(selected_table.columns) if selected_table is not None else []
    selected_row_count = int(selected_table.row_count or 0) if selected_table is not None else 0
    selected_table_name = str(selected_table.table_name) if selected_table is not None else None
    selected_sheet_name = str(selected_table.sheet_name) if selected_table is not None else None

    schema_summary_context = build_schema_summary_context(
        query=query,
        dataset=dataset,
        target_file=target_file,
        selected_table=selected_table,
    )
    schema_payload = {
        "table_name": selected_table_name,
        "sheet_name": selected_sheet_name,
        "row_count": selected_row_count,
        "columns": selected_columns,
        "column_aliases": {str(key): str(value) for key, value in selected_aliases.items()},
        **schema_summary_context,
    }
    prompt_context = "Deterministic schema/file summary context (source of truth):\n" + json.dumps(
        schema_payload,
        ensure_ascii=False,
        indent=2,
    )
    rows_total = int(schema_summary_context.get("rows_total", selected_row_count) or 0)
    source_label = (
        f"{getattr(target_file, 'original_filename', 'unknown')} "
        f"| table={selected_table_name} | dataset_v={dataset.dataset_version} "
        f"| table_v={selected_table.table_version} | schema"
        if selected_table is not None
        else (
            f"{getattr(target_file, 'original_filename', 'unknown')} "
            f"| dataset_v={dataset.dataset_version} | schema_multi_table"
        )
    )
    dataset_debug_fields = (
        build_dataset_debug_fields(dataset=dataset, table=selected_table)
        if selected_table is not None
        else {
            "storage_engine": getattr(dataset, "engine", None),
            "dataset_id": getattr(dataset, "dataset_id", None),
            "dataset_version": getattr(dataset, "dataset_version", None),
            "dataset_provenance_id": getattr(dataset, "dataset_provenance_id", None),
            "table_name": None,
            "sheet_name": None,
            "table_version": None,
            "table_provenance_id": None,
            "table_row_count": 0,
            "column_metadata_contract_version": getattr(dataset, "column_metadata_contract_version", None),
            "column_metadata_present": bool(
                int((getattr(dataset, "column_metadata_stats", {}) or {}).get("columns_with_metadata", 0) or 0) > 0
            ),
            "column_metadata_columns_total": int(
                (getattr(dataset, "column_metadata_stats", {}) or {}).get("columns_total", 0) or 0
            ),
            "column_metadata_columns_with_metadata": int(
                (getattr(dataset, "column_metadata_stats", {}) or {}).get("columns_with_metadata", 0) or 0
            ),
            "column_metadata_aliases_total": int(
                (getattr(dataset, "column_metadata_stats", {}) or {}).get("aliases_total", 0) or 0
            ),
            "column_metadata_sample_values_total": int(
                (getattr(dataset, "column_metadata_stats", {}) or {}).get("sample_values_total", 0) or 0
            ),
            "column_metadata_aliases_trimmed_total": int(
                (getattr(dataset, "column_metadata_stats", {}) or {}).get("aliases_trimmed_total", 0) or 0
            ),
            "column_metadata_sample_values_trimmed_total": int(
                (getattr(dataset, "column_metadata_stats", {}) or {}).get("sample_values_trimmed_total", 0) or 0
            ),
            "column_metadata_budget_enforced": bool(
                (getattr(dataset, "column_metadata_stats", {}) or {}).get("metadata_budget_enforced", False)
            ),
            "dataset_column_metadata_columns_total": int(
                (getattr(dataset, "column_metadata_stats", {}) or {}).get("columns_total", 0) or 0
            ),
            "dataset_column_metadata_columns_with_metadata": int(
                (getattr(dataset, "column_metadata_stats", {}) or {}).get("columns_with_metadata", 0) or 0
            ),
            "dataset_column_metadata_aliases_total": int(
                (getattr(dataset, "column_metadata_stats", {}) or {}).get("aliases_total", 0) or 0
            ),
            "dataset_column_metadata_sample_values_total": int(
                (getattr(dataset, "column_metadata_stats", {}) or {}).get("sample_values_total", 0) or 0
            ),
        }
    )
    payload = {
        "status": "ok",
        "prompt_context": prompt_context,
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_schema_question",
            "deterministic_path": True,
            "tabular_sql": {
                **dataset_debug_fields,
                "executed_sql": [],
                "sql_guardrails": {"valid": True, "reason": "schema_only_route"},
                "schema_payload": schema_payload,
            },
        },
        "sources": [source_label],
        "rows_expected_total": rows_total,
        "rows_retrieved_total": rows_total,
        "rows_used_map_total": rows_total,
        "rows_used_reduce_total": rows_total,
        "row_coverage_ratio": 1.0 if rows_total > 0 else 0.0,
    }
    return apply_route_debug(payload=payload, decision=decision, detected_language=preferred_lang)

from __future__ import annotations

import asyncio
import json
import logging
from time import perf_counter
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.core.config import settings
from app.observability.metrics import inc_counter, observe_ms
from app.services.chat.tabular_chart_delivery import render_chart_artifact
from app.services.chat.language import detect_preferred_response_language
from app.services.chat.tabular_debug_contract import (
    apply_tabular_debug_fields,
    build_dataset_debug_fields,
    build_route_debug_fields,
    ensure_tabular_debug_containers,
)
from app.services.chat.tabular_intent_router import (
    TabularIntentDecision,
    classify_tabular_query,
    detect_legacy_tabular_intent,
    suggest_relevant_alternative_columns,
)
from app.services.chat.tabular_query_parser import parse_tabular_query
from app.services.chat.tabular_response_composer import (
    build_chart_response_text,
    build_chart_unmatched_field_message,
    build_execution_error_message,
    build_missing_column_message,
    build_timeout_message,
)
from app.services.chat.tabular_schema_resolver import (
    find_direct_column_mentions,
    normalize_text,
    resolve_requested_field,
)
from app.services.chat.tabular_sql_pipeline import (
    build_profile_payload_pipeline,
    build_tabular_error_result_pipeline,
    execute_aggregate_sync_pipeline,
    execute_lookup_sync_pipeline,
    execute_profile_sync_pipeline,
)
from app.services.tabular import (
    GuardrailsConfig,
    SQL_ERROR_EXECUTION_FAILED,
    SQL_ERROR_TIMEOUT,
    SQLExecutionLimits,
    SQLGuardrails,
    TabularExecutionSession,
    TabularSQLException,
    resolve_tabular_dataset,
    rows_to_result_text,
    to_tabular_error_payload,
)
from app.services.tabular.sql_execution import ResolvedTabularDataset, ResolvedTabularTable

logger = logging.getLogger(__name__)

_COUNT_HINTS = ("count", "how many", "сколько", "количество", "число")
_SUM_HINTS = ("sum", "total", "сумм", "итого")
_AVG_HINTS = ("avg", "average", "mean", "средн")
_MIN_HINTS = ("min", "миним")
_MAX_HINTS = ("max", "максим")
_GROUP_HINTS = ("group by", "by ", "по ")
_MONTH_HINTS = ("month", "months", "месяц", "месяцы", "помесяч", "по месяц")
_DATETIME_TOKENS = ("date", "time", "month", "year", "day", "дата", "время", "месяц", "год", "день")


def is_tabular_aggregate_intent(query: str) -> bool:
    return detect_tabular_intent(query) == "aggregate"


def detect_tabular_intent(query: str) -> Optional[str]:
    return detect_legacy_tabular_intent(query)


def _norm(text: str) -> str:
    return normalize_text(text)


def _quote_ident(name: str) -> str:
    return '"' + str(name or "").replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _extract_row_tuples(rows: Sequence[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
    return [tuple(row) for row in rows]


def _first_int(rows: Sequence[Tuple[Any, ...]], default: int = 0) -> int:
    if not rows:
        return int(default)
    first = rows[0][0] if rows[0] else default
    try:
        return int(first)
    except Exception:
        try:
            return int(float(first))
        except Exception:
            return int(default)


def _choose_operation(query: str) -> str:
    q = _norm(query)
    if any(h in q for h in _SUM_HINTS):
        return "sum"
    if any(h in q for h in _AVG_HINTS):
        return "avg"
    if any(h in q for h in _MIN_HINTS):
        return "min"
    if any(h in q for h in _MAX_HINTS):
        return "max"
    return "count"


def _select_table_for_query(query: str, dataset: ResolvedTabularDataset) -> Optional[ResolvedTabularTable]:
    if not dataset.tables:
        return None

    q_norm = _norm(query)
    for table in dataset.tables:
        table_norm = _norm(table.table_name)
        sheet_norm = _norm(table.sheet_name)
        if (table_norm and table_norm in q_norm) or (sheet_norm and sheet_norm in q_norm):
            return table

    return max(dataset.tables, key=lambda item: int(item.row_count or 0))


def _build_guardrails() -> SQLGuardrails:
    return SQLGuardrails(
        GuardrailsConfig(
            max_sql_chars=int(settings.TABULAR_SQL_MAX_CHARS),
            max_result_rows=int(settings.TABULAR_SQL_MAX_RESULT_ROWS),
            max_scanned_rows=int(settings.TABULAR_SQL_MAX_SCANNED_ROWS),
            max_result_bytes=int(settings.TABULAR_SQL_MAX_RESULT_BYTES),
        )
    )


def _build_execution_limits() -> SQLExecutionLimits:
    return SQLExecutionLimits(
        max_result_rows=int(settings.TABULAR_SQL_MAX_RESULT_ROWS),
        max_result_bytes=int(settings.TABULAR_SQL_MAX_RESULT_BYTES),
    )


def _run_guarded_query(
    *,
    session: TabularExecutionSession,
    guardrails: SQLGuardrails,
    sql: str,
    estimated_scan_rows: int,
    timeout_seconds: float,
) -> Tuple[List[Tuple[Any, ...]], str, Dict[str, Any]]:
    guarded_sql, guard_debug = guardrails.enforce(sql, estimated_scan_rows=estimated_scan_rows)
    rows = session.execute(guarded_sql, timeout_seconds=timeout_seconds)
    return _extract_row_tuples(rows), guarded_sql, guard_debug


def _resolve_required_field(
    *,
    field_text: Optional[str],
    table: ResolvedTabularTable,
    detail_reason: str,
    expected_dtype_family: Optional[str] = None,
) -> Dict[str, Any]:
    resolution = resolve_requested_field(
        requested_field_text=field_text,
        table=table,
        expected_dtype_family=expected_dtype_family,
    )
    if resolution.status == "matched" and resolution.matched_column:
        return {
            "column": str(resolution.matched_column),
            "requested_field_text": resolution.requested_field_text,
            "candidate_columns": list(resolution.candidate_columns),
            "scored_candidates": list(resolution.scored_candidates),
            "match_score": resolution.match_score,
            "match_strategy": resolution.match_strategy,
        }
    raise TabularSQLException(
        code=SQL_ERROR_EXECUTION_FAILED,
        message="Requested field was not matched to schema",
        details={
            "requested_field_text": str(field_text or "").strip() or None,
            "candidate_columns": list(resolution.candidate_columns),
            "scored_candidates": list(resolution.scored_candidates),
            "fallback_reason": detail_reason,
        },
    )


def _build_sql(
    *,
    query: str,
    table: ResolvedTabularTable,
) -> Tuple[str, Dict[str, Any]]:
    parsed = parse_tabular_query(query)
    operation = parsed.operation or _choose_operation(query)
    direct_mentions = find_direct_column_mentions(query, table)
    q_norm = _norm(query)

    metric_column: Optional[str] = None
    group_by_column: Optional[str] = None
    requested_field_text = parsed.requested_field_text
    candidate_columns: List[str] = []
    scored_candidates: List[Dict[str, Any]] = []
    match_score: Optional[float] = None
    match_strategy: Optional[str] = None

    if operation in {"sum", "avg", "min", "max"}:
        if requested_field_text:
            metric_resolution = _resolve_required_field(
                field_text=requested_field_text,
                table=table,
                detail_reason="missing_metric_column",
                expected_dtype_family="numeric",
            )
            metric_column = metric_resolution["column"]
            candidate_columns = list(metric_resolution["candidate_columns"])
            scored_candidates = list(metric_resolution["scored_candidates"])
            match_score = metric_resolution["match_score"]
            match_strategy = metric_resolution["match_strategy"]
        elif direct_mentions:
            if len(direct_mentions) == 1:
                metric_column = str(direct_mentions[0])
                match_score = 1.0
                match_strategy = "direct_column_mention"
            else:
                raise TabularSQLException(
                    code=SQL_ERROR_EXECUTION_FAILED,
                    message="Metric operation matched multiple candidate columns",
                    details={
                        "requested_field_text": None,
                        "fallback_reason": "ambiguous_metric_column",
                        "candidate_columns": [str(item) for item in direct_mentions],
                    },
                )
        else:
            raise TabularSQLException(
                code=SQL_ERROR_EXECUTION_FAILED,
                message="Metric operation requires a matched column",
                details={
                    "requested_field_text": None,
                    "fallback_reason": "missing_metric_column",
                    "candidate_columns": [str(col) for col in list(table.columns)],
                },
            )

    if parsed.group_by_field_text:
        group_resolution = _resolve_required_field(
            field_text=parsed.group_by_field_text,
            table=table,
            detail_reason="missing_group_by_column",
            expected_dtype_family="categorical",
        )
        group_by_column = group_resolution["column"]
    elif operation == "count" and direct_mentions and any(h in q_norm for h in _GROUP_HINTS):
        if len(direct_mentions) == 1:
            group_by_column = str(direct_mentions[0])
        else:
            raise TabularSQLException(
                code=SQL_ERROR_EXECUTION_FAILED,
                message="Count group-by matched multiple candidate columns",
                details={
                    "requested_field_text": None,
                    "fallback_reason": "ambiguous_group_by_column",
                    "candidate_columns": [str(item) for item in direct_mentions],
                },
            )

    table_q = _quote_ident(table.table_name)
    if operation == "count":
        if group_by_column:
            gq = _quote_ident(group_by_column)
            sql = (
                f"SELECT {gq} AS group_key, COUNT(*) AS value "
                f"FROM {table_q} "
                f"GROUP BY {gq} "
                f"ORDER BY value DESC LIMIT 50"
            )
        else:
            sql = f"SELECT COUNT(*) AS value FROM {table_q}"
    else:
        assert metric_column is not None
        metric_q = _quote_ident(metric_column)
        numeric_expr = f"CAST(REPLACE(NULLIF(TRIM({metric_q}), ''), ',', '.') AS DOUBLE)"
        sql_op = {"sum": "SUM", "avg": "AVG", "min": "MIN", "max": "MAX"}[operation]
        agg_expr = f"ROUND({sql_op}({numeric_expr}), 6)"
        where_clause = f"WHERE TRIM(COALESCE(CAST({metric_q} AS VARCHAR), '')) <> ''"
        if group_by_column:
            gq = _quote_ident(group_by_column)
            sql = (
                f"SELECT {gq} AS group_key, {agg_expr} AS value "
                f"FROM {table_q} "
                f"{where_clause} "
                f"GROUP BY {gq} "
                f"ORDER BY value DESC LIMIT 50"
            )
        else:
            sql = f"SELECT {agg_expr} AS value FROM {table_q} {where_clause}"

    matched_columns = [item for item in [metric_column, group_by_column] if item]
    matched_columns.extend(direct_mentions)
    matched_columns = list(dict.fromkeys([str(item) for item in matched_columns]))
    return sql, {
        "operation": operation,
        "group_by_column": group_by_column,
        "metric_column": metric_column,
        "matched_columns": matched_columns,
        "requested_field_text": requested_field_text,
        "candidate_columns": candidate_columns,
        "scored_candidates": scored_candidates,
        "matched_column": metric_column,
        "match_score": match_score,
        "match_strategy": match_strategy,
        "retrieval_filters": {"group_by_column": group_by_column} if group_by_column else None,
    }


def _build_lookup_sql(
    *,
    query: str,
    table: ResolvedTabularTable,
) -> Tuple[str, Dict[str, Any]]:
    parsed = parse_tabular_query(query)
    direct_mentions = find_direct_column_mentions(query, table)
    result_columns = list(direct_mentions[:8]) if direct_mentions else list(table.columns[:8])
    if not result_columns:
        result_columns = list(table.columns[:4])
    if not result_columns:
        result_columns = ["*"]

    lookup_value = parsed.lookup_value_text
    filter_column: Optional[str] = None
    candidate_columns: List[str] = []
    scored_candidates: List[Dict[str, Any]] = []
    match_score: Optional[float] = None
    match_strategy: Optional[str] = None

    if parsed.lookup_field_text:
        lookup_resolution = _resolve_required_field(
            field_text=parsed.lookup_field_text,
            table=table,
            detail_reason="missing_lookup_filter_column",
            expected_dtype_family="categorical",
        )
        filter_column = lookup_resolution["column"]
        candidate_columns = list(lookup_resolution["candidate_columns"])
        scored_candidates = list(lookup_resolution["scored_candidates"])
        match_score = lookup_resolution["match_score"]
        match_strategy = lookup_resolution["match_strategy"]
    elif direct_mentions:
        if len(direct_mentions) == 1:
            filter_column = str(direct_mentions[0])
            match_score = 1.0
            match_strategy = "direct_column_mention"
        elif lookup_value:
            raise TabularSQLException(
                code=SQL_ERROR_EXECUTION_FAILED,
                message="Lookup filter value matched multiple candidate columns",
                details={
                    "requested_field_text": None,
                    "fallback_reason": "ambiguous_lookup_filter_column",
                    "candidate_columns": [str(item) for item in direct_mentions],
                },
            )
    elif lookup_value:
        raise TabularSQLException(
            code=SQL_ERROR_EXECUTION_FAILED,
            message="Lookup filter value was provided without a matched filter column",
            details={
                "requested_field_text": None,
                "fallback_reason": "missing_lookup_filter_column",
                "candidate_columns": [str(col) for col in list(table.columns)],
            },
        )

    where_clause = ""
    retrieval_filters: Dict[str, Any] = {}
    if lookup_value and filter_column:
        val = str(lookup_value).strip().lower()
        where_clause = (
            f"WHERE LOWER(TRIM(COALESCE(CAST({_quote_ident(filter_column)} AS VARCHAR), ''))) "
            f"LIKE {_sql_literal('%' + val + '%')}"
        )
        retrieval_filters = {"where": {str(filter_column): {"like": f"%{val}%"}}}

    order_column = result_columns[0] if result_columns else None
    if result_columns == ["*"]:
        select_cols = "*"
    else:
        select_cols = ", ".join([_quote_ident(col) for col in result_columns if col != "*"])
    sql = f"SELECT {select_cols} FROM {_quote_ident(table.table_name)} {where_clause}".strip()
    if order_column and order_column != "*":
        sql += f" ORDER BY {_quote_ident(order_column)}"
    sql += " LIMIT 30"

    matched_columns = []
    if filter_column:
        matched_columns.append(str(filter_column))
    matched_columns.extend([str(item) for item in direct_mentions])
    matched_columns = list(dict.fromkeys(matched_columns))
    return sql, {
        "operation": "lookup",
        "lookup_value": lookup_value,
        "filter_column": filter_column,
        "result_columns": result_columns,
        "matched_columns": matched_columns,
        "requested_field_text": parsed.lookup_field_text,
        "candidate_columns": candidate_columns,
        "scored_candidates": scored_candidates,
        "matched_column": filter_column,
        "match_score": match_score,
        "match_strategy": match_strategy,
        "retrieval_filters": retrieval_filters if retrieval_filters else None,
    }


def _execute_aggregate_sync(
    *,
    query: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    timeout_seconds: float,
) -> Dict[str, Any]:
    return execute_aggregate_sync_pipeline(
        query=query,
        dataset=dataset,
        table=table,
        target_file=target_file,
        timeout_seconds=timeout_seconds,
        build_guardrails_fn=_build_guardrails,
        build_execution_limits_fn=_build_execution_limits,
        build_sql_fn=lambda q, t: _build_sql(query=q, table=t),
        run_guarded_query_fn=_run_guarded_query,
        quote_ident_fn=_quote_ident,
        first_int_fn=lambda rows, default: _first_int(rows, default=default),
        observe_ms_fn=observe_ms,
        rows_to_result_text_fn=rows_to_result_text,
    )


def _execute_lookup_sync(
    *,
    query: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    timeout_seconds: float,
) -> Dict[str, Any]:
    return execute_lookup_sync_pipeline(
        query=query,
        dataset=dataset,
        table=table,
        target_file=target_file,
        timeout_seconds=timeout_seconds,
        build_guardrails_fn=_build_guardrails,
        build_execution_limits_fn=_build_execution_limits,
        build_sql_fn=lambda q, t: _build_lookup_sql(query=q, table=t),
        run_guarded_query_fn=_run_guarded_query,
        observe_ms_fn=observe_ms,
        rows_to_result_text_fn=rows_to_result_text,
    )


def _build_profile_payload(
    *,
    session: TabularExecutionSession,
    guardrails: SQLGuardrails,
    table: ResolvedTabularTable,
    max_columns: int,
    timeout_seconds: float,
) -> Tuple[str, Dict[str, Any], int]:
    return build_profile_payload_pipeline(
        session=session,
        guardrails=guardrails,
        table=table,
        max_columns=max_columns,
        timeout_seconds=timeout_seconds,
        run_guarded_query_fn=_run_guarded_query,
        quote_ident_fn=_quote_ident,
        first_int_fn=lambda rows, default: _first_int(rows, default=default),
        extract_row_tuples_fn=_extract_row_tuples,
    )


def _execute_profile_sync(
    *,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    timeout_seconds: float,
) -> Dict[str, Any]:
    return execute_profile_sync_pipeline(
        dataset=dataset,
        table=table,
        target_file=target_file,
        timeout_seconds=timeout_seconds,
        max_columns=int(settings.TABULAR_PROFILE_MAX_COLUMNS),
        build_guardrails_fn=_build_guardrails,
        build_execution_limits_fn=_build_execution_limits,
        build_profile_payload_fn=_build_profile_payload,
        quote_ident_fn=_quote_ident,
    )


def _build_tabular_error_result(
    *,
    query: str,
    intent_kind: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    error_payload: Dict[str, Any],
) -> Dict[str, Any]:
    payload = build_tabular_error_result_pipeline(
        intent_kind=intent_kind,
        dataset=dataset,
        table=table,
        target_file=target_file,
        error_payload=error_payload,
        sql_error_execution_failed=SQL_ERROR_EXECUTION_FAILED,
        sql_error_timeout=SQL_ERROR_TIMEOUT,
    )
    preferred_lang = detect_preferred_response_language(query)
    error_code = str(error_payload.get("code") or SQL_ERROR_EXECUTION_FAILED)
    details = error_payload.get("details") if isinstance(error_payload.get("details"), dict) else {}
    requested_chart_field = str(details.get("requested_chart_field") or "").strip()
    requested_field_text = str(details.get("requested_field_text") or "").strip()
    candidate_columns = details.get("candidate_columns") if isinstance(details.get("candidate_columns"), list) else []
    scored_candidates = details.get("scored_candidates") if isinstance(details.get("scored_candidates"), list) else []

    if requested_chart_field:
        payload["clarification_prompt"] = build_chart_unmatched_field_message(
            preferred_lang=preferred_lang,
            requested_field=requested_chart_field,
        )
        payload = apply_tabular_debug_fields(
            payload,
            fields={
                "requested_field_text": requested_chart_field,
                "matched_column": None,
                "match_score": None,
                "match_strategy": "none",
                "candidate_columns": list(candidate_columns),
                "scored_candidates": list(scored_candidates),
                "chart_spec_generated": False,
                "chart_rendered": False,
                "chart_artifact_path": None,
                "chart_artifact_id": None,
                "chart_artifact_available": False,
                "chart_artifact_exists": False,
                "chart_fallback_reason": "requested_field_not_matched",
                "response_language": preferred_lang,
            },
        )
        return payload

    if error_code == SQL_ERROR_TIMEOUT:
        payload["clarification_prompt"] = build_timeout_message(preferred_lang=preferred_lang)
    elif requested_field_text or candidate_columns:
        payload["clarification_prompt"] = build_missing_column_message(
            preferred_lang=preferred_lang,
            requested_fields=[requested_field_text] if requested_field_text else [],
            alternatives=[str(item) for item in candidate_columns],
            ambiguous=False,
        )
    else:
        payload["clarification_prompt"] = build_execution_error_message(preferred_lang=preferred_lang)

    payload = apply_tabular_debug_fields(
        payload,
        fields={
            "requested_field_text": requested_field_text or None,
            "candidate_columns": [str(item) for item in candidate_columns],
            "scored_candidates": list(scored_candidates),
        },
    )
    return payload


def _route_debug_payload(
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


def _apply_route_debug(
    *,
    payload: Dict[str, Any],
    decision: TabularIntentDecision,
    detected_language: str,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    route_debug = _route_debug_payload(decision=decision, detected_language=detected_language)
    payload = apply_tabular_debug_fields(payload, fields=route_debug)
    debug, tabular_debug = ensure_tabular_debug_containers(payload)
    debug["matched_columns"] = list(decision.matched_columns)
    debug["unmatched_requested_fields"] = list(decision.unmatched_requested_fields)
    tabular_debug["matched_columns"] = list(decision.matched_columns)
    tabular_debug["unmatched_requested_fields"] = list(decision.unmatched_requested_fields)
    return payload


def _build_missing_column_response(
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
    alternatives = suggest_relevant_alternative_columns(table, limit=6)
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
        },
    )
    return _apply_route_debug(payload=payload, decision=decision, detected_language=preferred_lang)


def _build_schema_question_payload(
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
    return _apply_route_debug(payload=payload, decision=decision, detected_language=preferred_lang)


def _is_datetime_like_column(column_name: str, table: ResolvedTabularTable) -> bool:
    aliases = table.column_aliases if isinstance(table.column_aliases, dict) else {}
    combined = " ".join([_norm(column_name), _norm(str(aliases.get(column_name, "")))])
    return any(token in combined for token in _DATETIME_TOKENS)


def _extract_requested_chart_field(query: str) -> Optional[str]:
    parsed = parse_tabular_query(query)
    if parsed.route in {"chart", "trend", "comparison"}:
        return str(parsed.requested_field_text or "").strip() or None
    return None


def _choose_chart_dimension_column(
    *,
    query: str,
    table: ResolvedTabularTable,
    decision: TabularIntentDecision,
) -> Tuple[Optional[str], Dict[str, Any]]:
    requested_field = _extract_requested_chart_field(query)
    if requested_field:
        q_norm = _norm(query)
        expected_dtype_family = "datetime" if any(token in q_norm for token in _MONTH_HINTS) else "categorical"
        resolution = resolve_requested_field(
            requested_field_text=requested_field,
            table=table,
            expected_dtype_family=expected_dtype_family,
        )
        if resolution.status == "matched" and resolution.matched_column:
            return str(resolution.matched_column), {
                "candidate_columns": list(resolution.candidate_columns),
                "scored_candidates": list(resolution.scored_candidates),
                "match_score": resolution.match_score,
                "match_strategy": resolution.match_strategy,
                "requested_field_text": requested_field,
                "fallback_reason": "none",
            }
        fallback_reason = "requested_field_ambiguous" if resolution.status == "ambiguous" else "requested_field_not_matched"
        return None, {
            "candidate_columns": list(resolution.candidate_columns),
            "scored_candidates": list(resolution.scored_candidates),
            "match_score": resolution.match_score,
            "match_strategy": resolution.match_strategy,
            "requested_field_text": requested_field,
            "fallback_reason": fallback_reason,
        }

    if decision.matched_column:
        return str(decision.matched_column), {
            "candidate_columns": list(decision.candidate_columns),
            "scored_candidates": list(decision.scored_candidates),
            "match_score": decision.match_score,
            "match_strategy": decision.match_strategy,
            "requested_field_text": decision.requested_field_text,
            "fallback_reason": "none",
        }

    if len(decision.matched_columns) == 1:
        return str(decision.matched_columns[0]), {
            "candidate_columns": list(decision.candidate_columns),
            "scored_candidates": list(decision.scored_candidates),
            "match_score": decision.match_score,
            "match_strategy": decision.match_strategy,
            "requested_field_text": decision.requested_field_text,
            "fallback_reason": "none",
        }

    fallback_reason = "missing_chart_dimension_column"
    if len(decision.matched_columns) > 1:
        fallback_reason = "ambiguous_chart_dimension_column"
    return None, {
        "candidate_columns": list(decision.candidate_columns) or [str(item) for item in list(table.columns)],
        "scored_candidates": list(decision.scored_candidates),
        "match_score": None,
        "match_strategy": "none",
        "requested_field_text": decision.requested_field_text or requested_field,
        "fallback_reason": fallback_reason,
    }


def _build_chart_sql(
    *,
    query: str,
    table: ResolvedTabularTable,
    decision: TabularIntentDecision,
) -> Tuple[str, Dict[str, Any]]:
    preferred_lang = detect_preferred_response_language(query)
    requested_chart_field = _extract_requested_chart_field(query)
    dimension_column, resolution_debug = _choose_chart_dimension_column(
        query=query,
        table=table,
        decision=decision,
    )
    if not dimension_column:
        raise TabularSQLException(
            code=SQL_ERROR_EXECUTION_FAILED,
            message="Requested chart field could not be matched to table columns",
            details={
                "requested_chart_field": requested_chart_field,
                "requested_field_text": resolution_debug.get("requested_field_text"),
                "candidate_columns": list(resolution_debug.get("candidate_columns") or []),
                "scored_candidates": list(resolution_debug.get("scored_candidates") or []),
                "fallback_reason": str(resolution_debug.get("fallback_reason") or "requested_field_not_matched"),
            },
        )

    dimension_q = _quote_ident(dimension_column)
    table_q = _quote_ident(table.table_name)
    aliases = table.column_aliases if isinstance(table.column_aliases, dict) else {}
    dimension_alias = str(aliases.get(dimension_column) or dimension_column)
    q_norm = _norm(query)
    monthly_requested = any(token in q_norm for token in _MONTH_HINTS)

    if monthly_requested and _is_datetime_like_column(dimension_column, table):
        bucket_expr = f"strftime(TRY_CAST({dimension_q} AS TIMESTAMP), '%m')"
        where_clause = f"WHERE TRY_CAST({dimension_q} AS TIMESTAMP) IS NOT NULL"
        x_title = "месяц" if preferred_lang == "ru" else "month"
        chart_title = (
            f"Распределение по месяцам для {dimension_alias}"
            if preferred_lang == "ru"
            else f"Distribution by month for {dimension_alias}"
        )
    else:
        bucket_expr = f"CAST({dimension_q} AS VARCHAR)"
        where_clause = f"WHERE TRIM(COALESCE(CAST({dimension_q} AS VARCHAR), '')) <> ''"
        x_title = dimension_alias
        chart_title = (
            f"Распределение по {dimension_alias}"
            if preferred_lang == "ru"
            else f"Distribution for {dimension_alias}"
        )

    sql = (
        f"SELECT {bucket_expr} AS bucket, COUNT(*) AS value "
        f"FROM {table_q} {where_clause} "
        "GROUP BY bucket "
        "ORDER BY value DESC LIMIT 24"
    )
    chart_spec = {
        "chart_type": "bar",
        "title": chart_title,
        "x": "bucket",
        "y": "value",
        "x_title": x_title,
        "y_title": "Количество" if preferred_lang == "ru" else "count",
        "requested_chart_field": requested_chart_field,
        "matched_chart_field": dimension_column,
        "matched_chart_field_alias": dimension_alias,
        "requested_dimension_column": dimension_column,
        "aggregation": "count",
        "requested_field_text": resolution_debug.get("requested_field_text"),
        "candidate_columns": list(resolution_debug.get("candidate_columns") or []),
        "scored_candidates": list(resolution_debug.get("scored_candidates") or []),
        "match_score": resolution_debug.get("match_score"),
        "match_strategy": resolution_debug.get("match_strategy"),
    }
    return sql, chart_spec


def _render_chart_artifact(
    *,
    rows: Sequence[Tuple[Any, ...]],
    chart_spec: Dict[str, Any],
) -> Dict[str, Any]:
    return render_chart_artifact(rows=rows, chart_spec=chart_spec)


def _build_chart_response_text(
    *,
    preferred_lang: str,
    chart_spec: Dict[str, Any],
    chart_delivery: Dict[str, Any],
    result_text: str,
) -> str:
    column_label = str(
        chart_spec.get("matched_chart_field_alias")
        or chart_spec.get("matched_chart_field")
        or chart_spec.get("requested_chart_field")
        or chart_spec.get("requested_dimension_column")
        or "field"
    )
    return build_chart_response_text(
        preferred_lang=preferred_lang,
        column_label=column_label,
        chart_rendered=bool(chart_delivery.get("chart_rendered")),
        chart_artifact_available=bool(
            chart_delivery.get("chart_artifact_available", chart_delivery.get("chart_artifact_exists"))
        ),
        chart_fallback_reason=str(chart_delivery.get("chart_fallback_reason") or "none"),
        result_text=result_text,
    )


def _execute_chart_sync(
    *,
    query: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    timeout_seconds: float,
    decision: TabularIntentDecision,
) -> Dict[str, Any]:
    preferred_lang = detect_preferred_response_language(query)
    guardrails = _build_guardrails()
    execution_limits = _build_execution_limits()
    sql, chart_spec = _build_chart_sql(query=query, table=table, decision=decision)
    with TabularExecutionSession(dataset=dataset, table=table, limits=execution_limits) as session:
        rows, guarded_sql, guard_debug = _run_guarded_query(
            session=session,
            guardrails=guardrails,
            sql=sql,
            estimated_scan_rows=int(table.row_count or 0),
            timeout_seconds=timeout_seconds,
        )

    result_text = rows_to_result_text(rows)
    chart_delivery = _render_chart_artifact(rows=rows, chart_spec=chart_spec)
    chart_response_text = _build_chart_response_text(
        preferred_lang=preferred_lang,
        chart_spec=chart_spec,
        chart_delivery=chart_delivery,
        result_text=result_text,
    )

    artifact_item = chart_delivery.get("artifact")
    artifacts = [artifact_item] if isinstance(artifact_item, dict) else []
    chart_artifact_path = chart_delivery.get("chart_artifact_path")
    chart_artifact_id = chart_delivery.get("chart_artifact_id")
    chart_rendered = bool(chart_delivery.get("chart_rendered"))
    chart_artifact_available = bool(
        chart_delivery.get("chart_artifact_available", chart_delivery.get("chart_artifact_exists"))
    )
    chart_artifact_exists = bool(chart_artifact_available)
    chart_fallback_reason = str(chart_delivery.get("chart_fallback_reason") or "none")
    requested_chart_field = chart_spec.get("requested_chart_field")
    matched_chart_field = chart_spec.get("matched_chart_field")
    selected_route = "chart"

    if not chart_artifact_available:
        artifacts = []

    logger.info(
        (
            "tabular_chart_delivery selected_route=%s requested_chart_field=%s matched_chart_field=%s "
            "chart_spec_generated=true chart_rendered=%s chart_artifact_available=%s "
            "chart_artifact_exists=%s chart_fallback_reason=%s"
        ),
        selected_route,
        requested_chart_field,
        matched_chart_field,
        str(chart_rendered).lower(),
        str(chart_artifact_available).lower(),
        str(chart_artifact_exists).lower(),
        chart_fallback_reason,
    )

    prompt_context = "Deterministic chart data and specification (source of truth):\n" + json.dumps(
        {
            "table_name": table.table_name,
            "executed_sql": guarded_sql,
            "chart_spec": chart_spec,
            "chart_delivery": {
                "requested_chart_field": requested_chart_field,
                "matched_chart_field": matched_chart_field,
                "chart_spec_generated": True,
                "chart_rendered": chart_rendered,
                "chart_artifact_path": chart_artifact_path,
                "chart_artifact_id": chart_artifact_id,
                "chart_artifact_available": chart_artifact_available,
                "chart_artifact_exists": chart_artifact_exists,
                "chart_fallback_reason": chart_fallback_reason,
                "response_language": preferred_lang,
            },
            "rows_preview": rows[:24],
        },
        ensure_ascii=False,
        indent=2,
    )
    payload = {
        "status": "ok",
        "prompt_context": prompt_context,
        "chart_response_text": chart_response_text,
        "artifacts": artifacts,
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_chart",
            "deterministic_path": True,
            "tabular_sql": {
                **build_dataset_debug_fields(dataset=dataset, table=table),
                "executed_sql": guarded_sql,
                "sql": guarded_sql,
                "result": result_text,
                "policy_decision": guard_debug.get("policy_decision"),
                "guardrail_flags": guard_debug.get("guardrail_flags", []),
                "sql_guardrails": guard_debug,
                "chart_spec": chart_spec,
            },
        },
        "sources": [
            (
                f"{getattr(target_file, 'original_filename', 'unknown')} "
                f"| table={table.table_name} | dataset_v={dataset.dataset_version} "
                f"| table_v={table.table_version} | sql_chart"
            )
        ],
        "rows_expected_total": int(table.row_count or 0),
        "rows_retrieved_total": int(len(rows)),
        "rows_used_map_total": int(len(rows)),
        "rows_used_reduce_total": int(len(rows)),
        "row_coverage_ratio": (
            float(len(rows) / int(table.row_count or 0))
            if int(table.row_count or 0) > 0
            else 0.0
        ),
    }
    payload = apply_tabular_debug_fields(
        payload,
        fields={
            "requested_field_text": chart_spec.get("requested_field_text"),
            "candidate_columns": list(chart_spec.get("candidate_columns") or []),
            "scored_candidates": list(chart_spec.get("scored_candidates") or []),
            "matched_column": matched_chart_field,
            "match_score": chart_spec.get("match_score"),
            "match_strategy": chart_spec.get("match_strategy"),
            "requested_chart_field": requested_chart_field,
            "matched_chart_field": matched_chart_field,
            "chart_spec_generated": True,
            "chart_rendered": chart_rendered,
            "chart_artifact_path": chart_artifact_path,
            "chart_artifact_id": chart_artifact_id,
            "chart_artifact_available": chart_artifact_available,
            "chart_artifact_exists": chart_artifact_exists,
            "chart_fallback_reason": chart_fallback_reason,
            "response_language": preferred_lang,
            "retrieval_hits_count": int(len(rows)),
        },
    )
    return payload


async def execute_tabular_sql_path(
    *,
    query: str,
    files: List[Any],
) -> Optional[Dict[str, Any]]:
    target_file = None
    dataset = None
    for file_obj in files:
        file_extension = str(
            getattr(file_obj, "extension", getattr(file_obj, "file_type", "")) or ""
        ).lower().lstrip(".")
        if file_extension not in {"xlsx", "xls", "csv", "tsv"}:
            continue
        resolved = resolve_tabular_dataset(file_obj)
        if resolved is not None and resolved.tables:
            target_file = file_obj
            dataset = resolved
            break

    if target_file is None or dataset is None:
        return None

    table = _select_table_for_query(query, dataset)
    if table is None:
        return None

    intent_decision = classify_tabular_query(query=query, table=table)
    intent_kind = intent_decision.legacy_intent
    if intent_kind is None:
        return None

    selected_route = str(intent_decision.selected_route or "")
    if selected_route == "unsupported_missing_column":
        return _build_missing_column_response(
            query=query,
            decision=intent_decision,
            dataset=dataset,
            table=table,
            target_file=target_file,
        )
    if selected_route == "schema_question":
        return _build_schema_question_payload(
            query=query,
            dataset=dataset,
            table=table,
            target_file=target_file,
            decision=intent_decision,
        )

    detected_language = detect_preferred_response_language(query)
    timeout_seconds = float(settings.TABULAR_SQL_TIMEOUT_SECONDS)
    try:
        started = perf_counter()
        if selected_route in {"overview"}:
            payload = await asyncio.wait_for(
                asyncio.to_thread(
                    _execute_profile_sync,
                    dataset=dataset,
                    table=table,
                    target_file=target_file,
                    timeout_seconds=timeout_seconds,
                ),
                timeout=timeout_seconds,
            )
            payload = _apply_route_debug(
                payload=payload,
                decision=intent_decision,
                detected_language=detected_language,
            )
            payload_debug = payload.get("debug", {})
            if isinstance(payload_debug, dict):
                payload_debug["intent"] = "tabular_overview"
                tabular_debug = payload_debug.get("tabular_sql")
                if isinstance(tabular_debug, dict):
                    tabular_debug["intent"] = "tabular_overview"
        elif selected_route in {"filtering"} or intent_kind == "lookup":
            payload = await asyncio.wait_for(
                asyncio.to_thread(
                    _execute_lookup_sync,
                    query=query,
                    dataset=dataset,
                    table=table,
                    target_file=target_file,
                    timeout_seconds=timeout_seconds,
                ),
                timeout=timeout_seconds,
            )
            payload = _apply_route_debug(
                payload=payload,
                decision=intent_decision,
                detected_language=detected_language,
            )
        elif selected_route in {"chart", "trend", "comparison"}:
            payload = await asyncio.wait_for(
                asyncio.to_thread(
                    _execute_chart_sync,
                    query=query,
                    dataset=dataset,
                    table=table,
                    target_file=target_file,
                    timeout_seconds=timeout_seconds,
                    decision=intent_decision,
                ),
                timeout=timeout_seconds,
            )
            payload = _apply_route_debug(
                payload=payload,
                decision=intent_decision,
                detected_language=detected_language,
            )
        else:
            payload = await asyncio.wait_for(
                asyncio.to_thread(
                    _execute_aggregate_sync,
                    query=query,
                    dataset=dataset,
                    table=table,
                    target_file=target_file,
                    timeout_seconds=timeout_seconds,
                ),
                timeout=timeout_seconds,
            )
            payload = _apply_route_debug(
                payload=payload,
                decision=intent_decision,
                detected_language=detected_language,
            )
        observe_ms("tabular_sql_path_ms", (perf_counter() - started) * 1000.0, intent=intent_kind)
        inc_counter("tabular_sql_path_total", intent=intent_kind, engine=dataset.engine)
        return payload
    except TimeoutError:
        inc_counter("tabular_sql_path_timeout_total", intent=intent_kind, engine=dataset.engine)
        inc_counter(
            "tabular_sql_path_error_total",
            intent=intent_kind,
            engine=dataset.engine,
            error_code=SQL_ERROR_TIMEOUT,
        )
        logger.warning("Tabular SQL path timeout: intent=%s timeout=%s", intent_kind, timeout_seconds)
        timeout_exc = TabularSQLException(
            code=SQL_ERROR_TIMEOUT,
            message="Tabular SQL path timeout",
            details={"timeout_seconds": timeout_seconds},
        )
        payload = _build_tabular_error_result(
            query=query,
            intent_kind=intent_kind,
            dataset=dataset,
            table=table,
            target_file=target_file,
            error_payload=to_tabular_error_payload(timeout_exc),
        )
        return _apply_route_debug(
            payload=payload,
            decision=intent_decision,
            detected_language=detected_language,
        )
    except Exception as exc:
        error_payload = to_tabular_error_payload(exc)
        error_code = str(error_payload.get("code") or SQL_ERROR_EXECUTION_FAILED)
        inc_counter(
            "tabular_sql_path_error_total",
            intent=intent_kind,
            engine=getattr(dataset, "engine", "unknown"),
            error_code=error_code,
        )
        if str(error_payload.get("category") or "") == "guardrail":
            inc_counter(
                "tabular_sql_guardrail_violation_total",
                intent=intent_kind,
                engine=dataset.engine,
                code=error_code,
            )
        logger.warning(
            "Tabular SQL path failed: intent=%s code=%s message=%s",
            intent_kind,
            error_code,
            error_payload.get("message"),
            exc_info=True,
        )
        payload = _build_tabular_error_result(
            query=query,
            intent_kind=intent_kind,
            dataset=dataset,
            table=table,
            target_file=target_file,
            error_payload=error_payload,
        )
        return _apply_route_debug(
            payload=payload,
            decision=intent_decision,
            detected_language=detected_language,
        )

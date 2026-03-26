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
)
from app.services.chat.tabular_intent_router import (
    TabularIntentDecision,
    classify_tabular_query,
    detect_legacy_tabular_intent,
)
from app.services.chat.tabular_query_parser import parse_tabular_query
from app.services.chat.tabular_response_composer import (
    build_chart_response_text,
    build_chart_unmatched_field_message,
    build_execution_error_message,
    build_missing_column_message,
    build_timeout_message,
)
from app.services.chat.tabular_scope_selector import select_tabular_scope
from app.services.chat.tabular_schema_resolver import (
    normalize_text,
    resolve_requested_field,
)
from app.services.chat.tabular_sql_query_planner import (
    build_aggregate_sql,
    build_lookup_sql,
)
from app.services.chat.tabular_sql_route_payloads import (
    apply_route_debug as apply_route_debug_payload,
    build_missing_column_response as build_missing_column_route_payload,
    build_route_debug_payload,
    build_schema_question_payload as build_schema_question_route_payload,
    build_scope_clarification_response as build_scope_clarification_route_payload,
)
from app.services.chat.tabular_sql_pipeline import (
    build_profile_payload_pipeline,
    build_tabular_error_result_pipeline,
    execute_aggregate_sync_pipeline,
    execute_lookup_sync_pipeline,
    execute_profile_sync_pipeline,
)
from app.services.chat.tabular_temporal_planner import (
    build_temporal_aggregation_plan,
    build_temporal_bucket_expression,
    resolve_temporal_grouping,
    resolve_temporal_measure_column,
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
_SUM_HINTS = ("sum", "total", "spend", "spending", "expense", "expenses", "revenue", "sales", "сумм", "итого")
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
    return build_aggregate_sql(
        query=query,
        table=table,
        choose_operation_fn=_choose_operation,
        normalize_text_fn=_norm,
        resolve_required_field_fn=_resolve_required_field,
        quote_ident_fn=_quote_ident,
        group_hints=_GROUP_HINTS,
        sql_error_execution_failed=SQL_ERROR_EXECUTION_FAILED,
        tabular_sql_exception_cls=TabularSQLException,
    )


def _build_lookup_sql(
    *,
    query: str,
    table: ResolvedTabularTable,
) -> Tuple[str, Dict[str, Any]]:
    return build_lookup_sql(
        query=query,
        table=table,
        resolve_required_field_fn=_resolve_required_field,
        quote_ident_fn=_quote_ident,
        sql_literal_fn=_sql_literal,
        sql_error_execution_failed=SQL_ERROR_EXECUTION_FAILED,
        tabular_sql_exception_cls=TabularSQLException,
    )


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
    requested_time_grain = str(details.get("requested_time_grain") or "").strip() or None
    source_datetime_field = str(details.get("source_datetime_field") or "").strip() or None
    derived_temporal_dimension = str(details.get("derived_temporal_dimension") or "").strip() or None
    temporal_plan_status = str(details.get("temporal_plan_status") or "not_requested")
    temporal_aggregation_plan = (
        details.get("temporal_aggregation_plan")
        if isinstance(details.get("temporal_aggregation_plan"), dict)
        else {}
    )

    if requested_chart_field:
        payload["clarification_prompt"] = build_chart_unmatched_field_message(
            preferred_lang=preferred_lang,
            requested_field=requested_chart_field,
            alternatives=[str(item) for item in candidate_columns],
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
                "controlled_response_state": "chart_unmatched_field",
                "response_language": preferred_lang,
                "requested_time_grain": requested_time_grain,
                "source_datetime_field": source_datetime_field,
                "derived_temporal_dimension": derived_temporal_dimension,
                "temporal_plan_status": temporal_plan_status,
                "temporal_aggregation_plan": temporal_aggregation_plan,
            },
        )
        return payload

    controlled_state = "tabular_execution_error"
    if error_code == SQL_ERROR_TIMEOUT:
        payload["clarification_prompt"] = build_timeout_message(preferred_lang=preferred_lang)
        controlled_state = "tabular_timeout"
    elif requested_field_text or candidate_columns:
        payload["clarification_prompt"] = build_missing_column_message(
            preferred_lang=preferred_lang,
            requested_fields=[requested_field_text] if requested_field_text else [],
            alternatives=[str(item) for item in candidate_columns],
            ambiguous=False,
        )
        controlled_state = "missing_column"
    else:
        payload["clarification_prompt"] = build_execution_error_message(preferred_lang=preferred_lang)

    payload = apply_tabular_debug_fields(
        payload,
        fields={
            "requested_field_text": requested_field_text or None,
            "candidate_columns": [str(item) for item in candidate_columns],
            "scored_candidates": list(scored_candidates),
            "controlled_response_state": controlled_state,
            "requested_time_grain": requested_time_grain,
            "source_datetime_field": source_datetime_field,
            "derived_temporal_dimension": derived_temporal_dimension,
            "temporal_plan_status": temporal_plan_status,
            "temporal_aggregation_plan": temporal_aggregation_plan,
        },
    )
    return payload


def _route_debug_payload(
    *,
    decision: TabularIntentDecision,
    detected_language: str,
) -> Dict[str, Any]:
    return build_route_debug_payload(
        decision=decision,
        detected_language=detected_language,
    )


def _apply_route_debug(
    *,
    payload: Dict[str, Any],
    decision: TabularIntentDecision,
    detected_language: str,
) -> Dict[str, Any]:
    return apply_route_debug_payload(
        payload=payload,
        decision=decision,
        detected_language=detected_language,
    )


def _build_missing_column_response(
    *,
    query: str,
    decision: TabularIntentDecision,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
) -> Dict[str, Any]:
    return build_missing_column_route_payload(
        query=query,
        decision=decision,
        dataset=dataset,
        table=table,
        target_file=target_file,
    )


def _build_schema_question_payload(
    *,
    query: str,
    dataset: ResolvedTabularDataset,
    table: Optional[ResolvedTabularTable],
    target_file: Any,
    decision: TabularIntentDecision,
) -> Dict[str, Any]:
    return build_schema_question_route_payload(
        query=query,
        dataset=dataset,
        table=table,
        target_file=target_file,
        decision=decision,
    )


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
    parsed = parse_tabular_query(query)
    if parsed.requested_time_grain:
        temporal_resolution = resolve_temporal_grouping(
            query=query,
            table=table,
            requested_time_grain=parsed.requested_time_grain,
            source_datetime_hint=parsed.source_datetime_field_hint,
        )
        if temporal_resolution.temporal_plan_status != "resolved" or not temporal_resolution.source_datetime_field:
            raise TabularSQLException(
                code=SQL_ERROR_EXECUTION_FAILED,
                message="Temporal grouping requested but datetime source field is unresolved",
                details={
                    "requested_chart_field": parsed.requested_time_grain,
                    "requested_time_grain": parsed.requested_time_grain,
                    "requested_field_text": parsed.requested_field_text,
                    "source_datetime_field": None,
                    "derived_temporal_dimension": None,
                    "temporal_plan_status": temporal_resolution.temporal_plan_status,
                    "candidate_columns": list(temporal_resolution.candidate_datetime_fields),
                    "scored_candidates": list(temporal_resolution.scored_datetime_candidates),
                    "fallback_reason": str(temporal_resolution.fallback_reason or "missing_datetime_source"),
                    "temporal_aggregation_plan": build_temporal_aggregation_plan(
                        requested_time_grain=parsed.requested_time_grain,
                        source_datetime_field=None,
                        derived_grouping_dimension=None,
                        operation=parsed.operation,
                        measure_column=None,
                        status=temporal_resolution.temporal_plan_status,
                        fallback_reason=str(temporal_resolution.fallback_reason or "missing_datetime_source"),
                    ),
                },
            )

        source_datetime_field = str(temporal_resolution.source_datetime_field)
        source_datetime_q = _quote_ident(source_datetime_field)
        table_q = _quote_ident(table.table_name)
        bucket_plan = build_temporal_bucket_expression(
            datetime_sql_expr=source_datetime_q,
            requested_time_grain=str(parsed.requested_time_grain),
        )
        operation = parsed.operation or _choose_operation(query)
        if operation not in {"count", "sum", "avg", "min", "max"}:
            operation = "count"

        candidate_columns: List[str] = list(temporal_resolution.candidate_datetime_fields)
        scored_candidates: List[Dict[str, Any]] = list(temporal_resolution.scored_datetime_candidates)
        metric_column: Optional[str] = None
        match_score: Optional[float] = None
        match_strategy = "temporal_derived_dimension"

        where_clause = str(bucket_plan.get("where_clause") or "")
        value_expr = "COUNT(*)"
        if operation in {"sum", "avg", "min", "max"}:
            measure_resolution = resolve_temporal_measure_column(
                query=query,
                table=table,
                requested_metric_text=parsed.requested_field_text,
            )
            if measure_resolution.status != "resolved" or not measure_resolution.measure_column:
                fallback_reason = str(measure_resolution.fallback_reason or "missing_numeric_measure")
                raise TabularSQLException(
                    code=SQL_ERROR_EXECUTION_FAILED,
                    message="Temporal aggregation requested but numeric measure column is unresolved",
                    details={
                        "requested_chart_field": parsed.requested_time_grain,
                        "requested_time_grain": parsed.requested_time_grain,
                        "requested_field_text": parsed.requested_field_text,
                        "source_datetime_field": source_datetime_field,
                        "derived_temporal_dimension": temporal_resolution.derived_grouping_dimension,
                        "temporal_plan_status": "missing_measure",
                        "candidate_columns": list(measure_resolution.candidate_columns),
                        "scored_candidates": list(measure_resolution.scored_candidates),
                        "fallback_reason": fallback_reason,
                        "temporal_aggregation_plan": build_temporal_aggregation_plan(
                            requested_time_grain=parsed.requested_time_grain,
                            source_datetime_field=source_datetime_field,
                            derived_grouping_dimension=temporal_resolution.derived_grouping_dimension,
                            operation=operation,
                            measure_column=None,
                            status="missing_measure",
                            fallback_reason=fallback_reason,
                        ),
                    },
                )

            metric_column = str(measure_resolution.measure_column)
            metric_q = _quote_ident(metric_column)
            numeric_expr = f"CAST(REPLACE(NULLIF(TRIM({metric_q}), ''), ',', '.') AS DOUBLE)"
            sql_op = {"sum": "SUM", "avg": "AVG", "min": "MIN", "max": "MAX"}[operation]
            value_expr = f"ROUND({sql_op}({numeric_expr}), 6)"
            where_clause = (
                f"{where_clause} AND TRIM(COALESCE(CAST({metric_q} AS VARCHAR), '')) <> ''"
                if where_clause
                else f"WHERE TRIM(COALESCE(CAST({metric_q} AS VARCHAR), '')) <> ''"
            )
            candidate_columns = list(measure_resolution.candidate_columns)
            scored_candidates = list(measure_resolution.scored_candidates)
            if scored_candidates:
                try:
                    match_score = float(scored_candidates[0].get("score"))
                except Exception:
                    match_score = None

        sql = (
            f"SELECT {bucket_plan['bucket_expr']} AS bucket, {value_expr} AS value "
            f"FROM {table_q} {where_clause} "
            "GROUP BY bucket "
            f"{bucket_plan['order_by']} LIMIT 120"
        )

        aliases = table.column_aliases if isinstance(table.column_aliases, dict) else {}
        datetime_alias = str(aliases.get(source_datetime_field) or source_datetime_field)
        temporal_aggregation_plan = build_temporal_aggregation_plan(
            requested_time_grain=parsed.requested_time_grain,
            source_datetime_field=source_datetime_field,
            derived_grouping_dimension=temporal_resolution.derived_grouping_dimension,
            operation=operation,
            measure_column=metric_column,
            status="ready",
        )
        chart_spec = {
            "chart_type": "bar",
            "title": f"{operation} by {parsed.requested_time_grain}",
            "x": "bucket",
            "y": "value",
            "x_title": str(parsed.requested_time_grain),
            "y_title": operation,
            "requested_chart_field": parsed.requested_time_grain,
            "matched_chart_field": source_datetime_field,
            "matched_chart_field_alias": datetime_alias,
            "requested_dimension_column": source_datetime_field,
            "aggregation": operation,
            "requested_field_text": parsed.requested_field_text,
            "candidate_columns": candidate_columns,
            "scored_candidates": scored_candidates,
            "match_score": match_score,
            "match_strategy": match_strategy,
            "requested_time_grain": parsed.requested_time_grain,
            "source_datetime_field": source_datetime_field,
            "derived_temporal_dimension": temporal_resolution.derived_grouping_dimension,
            "temporal_plan_status": "resolved",
            "temporal_aggregation_plan": temporal_aggregation_plan,
            "response_language": preferred_lang,
        }
        return sql, chart_spec

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
                "requested_time_grain": None,
                "source_datetime_field": None,
                "derived_temporal_dimension": None,
                "temporal_plan_status": "not_requested",
                "temporal_aggregation_plan": {},
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
        x_title = "month"
        chart_title = f"Distribution by month for {dimension_alias}"
    else:
        bucket_expr = f"CAST({dimension_q} AS VARCHAR)"
        where_clause = f"WHERE TRIM(COALESCE(CAST({dimension_q} AS VARCHAR), '')) <> ''"
        x_title = dimension_alias
        chart_title = f"Distribution for {dimension_alias}"

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
        "y_title": "count",
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
        "requested_time_grain": None,
        "source_datetime_field": None,
        "derived_temporal_dimension": None,
        "temporal_plan_status": "not_requested",
        "temporal_aggregation_plan": {},
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
    source_scope: str,
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
        source_scope=source_scope,
    )


def _build_scope_label(*, target_file: Any, table: ResolvedTabularTable) -> str:
    file_name = str(getattr(target_file, "original_filename", "") or getattr(target_file, "stored_filename", "") or "unknown")
    sheet_name = str(getattr(table, "sheet_name", "") or "").strip()
    table_name = str(getattr(table, "table_name", "") or "table")
    if sheet_name:
        return f"{file_name} | sheet={sheet_name} | table={table_name}"
    return f"{file_name} | table={table_name}"


def _extract_scope_debug_fields(scope_debug: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(scope_debug, dict):
        return {}
    return {
        "scope_selection_status": str(scope_debug.get("scope_selection_status") or "selected"),
        "scope_selected_file_id": scope_debug.get("scope_selected_file_id"),
        "scope_selected_file_name": scope_debug.get("scope_selected_file_name"),
        "scope_selected_table_name": scope_debug.get("scope_selected_table_name"),
        "scope_selected_sheet_name": scope_debug.get("scope_selected_sheet_name"),
        "scope_file_candidates": list(scope_debug.get("scope_file_candidates") or []),
        "table_scope_candidates": list(scope_debug.get("table_scope_candidates") or []),
    }


def _apply_scope_debug_fields(*, payload: Dict[str, Any], scope_debug: Dict[str, Any]) -> Dict[str, Any]:
    fields = _extract_scope_debug_fields(scope_debug)
    if not fields:
        return payload
    return apply_tabular_debug_fields(payload, fields=fields)


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
        source_scope=_build_scope_label(target_file=target_file, table=table),
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
            "requested_time_grain": chart_spec.get("requested_time_grain"),
            "source_datetime_field": chart_spec.get("source_datetime_field"),
            "derived_temporal_dimension": chart_spec.get("derived_temporal_dimension"),
            "temporal_plan_status": chart_spec.get("temporal_plan_status"),
            "temporal_aggregation_plan": chart_spec.get("temporal_aggregation_plan")
            if isinstance(chart_spec.get("temporal_aggregation_plan"), dict)
            else {},
        },
    )
    return payload


async def execute_tabular_sql_path(
    *,
    query: str,
    files: List[Any],
) -> Optional[Dict[str, Any]]:
    eligible_files: List[Any] = []
    for file_obj in list(files or []):
        file_extension = str(
            getattr(file_obj, "extension", getattr(file_obj, "file_type", "")) or ""
        ).lower().lstrip(".")
        if file_extension not in {"xlsx", "xls", "csv", "tsv"}:
            continue
        eligible_files.append(file_obj)

    scope_decision = select_tabular_scope(
        query=query,
        files=eligible_files,
        resolve_dataset_fn=resolve_tabular_dataset,
    )
    scope_debug_fields = dict(scope_decision.debug_fields or {})
    parsed_query = parse_tabular_query(query)
    if scope_decision.status == "no_tabular_dataset":
        return None

    if scope_decision.status == "ambiguous_file":
        scope_kind = "file"
        return build_scope_clarification_route_payload(
            query=query,
            scope_kind=scope_kind,
            scope_options=list(scope_decision.clarification_options or []),
            scope_debug=scope_debug_fields,
        )

    if scope_decision.status == "ambiguous_table" and parsed_query.route != "schema_question":
        scope_kind = "sheet/table"
        return build_scope_clarification_route_payload(
            query=query,
            scope_kind=scope_kind,
            scope_options=list(scope_decision.clarification_options or []),
            scope_debug=scope_debug_fields,
        )

    target_file = scope_decision.target_file
    dataset = scope_decision.dataset
    table = scope_decision.table
    if target_file is None or dataset is None:
        return None

    intent_decision = classify_tabular_query(query=query, table=table)
    intent_kind = intent_decision.legacy_intent
    if intent_kind is None:
        return None

    selected_route = str(intent_decision.selected_route or "")
    if selected_route == "unsupported_missing_column":
        payload = _build_missing_column_response(
            query=query,
            decision=intent_decision,
            dataset=dataset,
            table=table,
            target_file=target_file,
        )
        return _apply_scope_debug_fields(payload=payload, scope_debug=scope_debug_fields)
    if selected_route == "schema_question":
        payload = _build_schema_question_payload(
            query=query,
            dataset=dataset,
            table=table,
            target_file=target_file,
            decision=intent_decision,
        )
        return _apply_scope_debug_fields(payload=payload, scope_debug=scope_debug_fields)

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
        return _apply_scope_debug_fields(payload=payload, scope_debug=scope_debug_fields)
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
        payload = _apply_route_debug(
            payload=payload,
            decision=intent_decision,
            detected_language=detected_language,
        )
        return _apply_scope_debug_fields(payload=payload, scope_debug=scope_debug_fields)
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
        payload = _apply_route_debug(
            payload=payload,
            decision=intent_decision,
            detected_language=detected_language,
        )
        return _apply_scope_debug_fields(payload=payload, scope_debug=scope_debug_fields)


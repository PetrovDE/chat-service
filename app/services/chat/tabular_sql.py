from __future__ import annotations

import asyncio
import json
import logging
import re
from time import perf_counter
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.core.config import settings
from app.domain.chat.query_planner import detect_tabular_intent as plan_detect_tabular_intent
from app.observability.metrics import inc_counter, observe_ms
from app.services.tabular import (
    GuardrailsConfig,
    SQLExecutionLimits,
    SQL_ERROR_EXECUTION_FAILED,
    SQL_ERROR_TIMEOUT,
    SQLGuardrails,
    TabularSQLException,
    TabularExecutionSession,
    resolve_tabular_dataset,
    rows_to_result_text,
    to_tabular_error_payload,
)
from app.services.tabular.sql_execution import ResolvedTabularDataset, ResolvedTabularTable

logger = logging.getLogger(__name__)

_COUNT_HINTS = ("\u0441\u043a\u043e\u043b\u044c\u043a\u043e", "count", "\u043a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e", "\u0447\u0438\u0441\u043b\u043e")
_SUM_HINTS = ("\u0441\u0443\u043c\u043c", "\u0438\u0442\u043e\u0433\u043e", "sum", "total")
_AVG_HINTS = ("\u0441\u0440\u0435\u0434\u043d", "avg", "average", "mean")
_MIN_HINTS = ("\u043c\u0438\u043d\u0438\u043c", "min")
_MAX_HINTS = ("\u043c\u0430\u043a\u0441\u0438\u043c", "max")
_GROUP_HINTS = ("\u0433\u0440\u0443\u043f\u043f", "group by", "\u043f\u043e ")
_AGGREGATE_HINTS = (
    "\u0432\u0441\u0435 \u0441\u0442\u0440\u043e\u043a\u0438",
    "\u043f\u043e \u0432\u0441\u0435\u043c \u0441\u0442\u0440\u043e\u043a\u0430\u043c",
    "all rows",
    "\u0432\u0435\u0441\u044c \u0444\u0430\u0439\u043b",
    "whole file",
    "entire file",
)
_PROFILE_HINTS = (
    "\u043f\u043e \u043a\u0430\u0436\u0434\u043e\u0439 \u043a\u043e\u043b\u043e\u043d",
    "\u043a\u0430\u0436\u0434\u043e\u0439 \u043a\u043e\u043b\u043e\u043d",
    "\u0432\u0441\u0435 \u043a\u043e\u043b\u043e\u043d\u043a\u0438",
    "\u0432\u0441\u0435\u0445 \u043a\u043e\u043b\u043e\u043d",
    "\u043e\u0431\u0449\u0438\u0439 \u0430\u043d\u0430\u043b\u0438\u0437",
    "\u043f\u043e\u043b\u043d\u044b\u0439 \u0430\u043d\u0430\u043b\u0438\u0437",
    "\u043a\u0430\u043a\u0438\u0435 \u0434\u0430\u043d\u043d\u044b\u0435",
    "\u0447\u0442\u043e \u0442\u044b \u043c\u043e\u0436\u0435\u0448\u044c \u0441\u043a\u0430\u0437\u0430\u0442\u044c",
    "\u043f\u043e\u043a\u0430\u0436\u0438 \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0438",
    "\u043f\u043e\u043a\u0430\u0436\u0438 \u043c\u0435\u0442\u0440\u0438\u043a\u0438",
    "column statistics",
    "per column",
    "full analysis",
    "analyze dataset",
)


def is_tabular_aggregate_intent(query: str) -> bool:
    return detect_tabular_intent(query) == "aggregate"


def detect_tabular_intent(query: str) -> Optional[str]:
    return plan_detect_tabular_intent(query)


def _norm(text: str) -> str:
    return re.sub(r"[^a-z\u0430-\u044f\u04510-9]+", " ", (text or "").lower()).strip()


def _quote_ident(name: str) -> str:
    return '"' + str(name or "").replace('"', '""') + '"'


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
    q = (query or "").lower()
    if any(h in q for h in _SUM_HINTS):
        return "sum"
    if any(h in q for h in _AVG_HINTS):
        return "avg"
    if any(h in q for h in _MIN_HINTS):
        return "min"
    if any(h in q for h in _MAX_HINTS):
        return "max"
    return "count"


def _pick_columns_from_query(query: str, table: ResolvedTabularTable) -> List[str]:
    q_norm = _norm(query)
    matches: List[str] = []

    for col in table.columns:
        col_norm = _norm(col)
        alias_norm = _norm(table.column_aliases.get(col, ""))
        if (col_norm and col_norm in q_norm) or (alias_norm and alias_norm in q_norm):
            if col not in matches:
                matches.append(col)

    return matches


def _build_sql(
    *,
    query: str,
    table: ResolvedTabularTable,
) -> Tuple[str, Dict[str, Any]]:
    operation = _choose_operation(query)
    matched_columns = _pick_columns_from_query(query, table)
    q = (query or "").lower()

    group_by_col: Optional[str] = None
    metric_col: Optional[str] = None
    if matched_columns:
        metric_col = matched_columns[0]
        if len(matched_columns) > 1 and any(h in q for h in _GROUP_HINTS):
            group_by_col = matched_columns[0]
            metric_col = matched_columns[1]

    table_q = _quote_ident(table.table_name)
    if operation == "count":
        if group_by_col:
            gq = _quote_ident(group_by_col)
            sql = (
                f"SELECT {gq} AS group_key, COUNT(*) AS value "
                f"FROM {table_q} "
                f"GROUP BY {gq} "
                f"ORDER BY value DESC LIMIT 50"
            )
        else:
            sql = f"SELECT COUNT(*) AS value FROM {table_q}"
    else:
        if not metric_col:
            sql = f"SELECT COUNT(*) AS value FROM {table_q}"
            operation = "count"
        else:
            metric_q = _quote_ident(metric_col)
            numeric_expr = f"CAST(REPLACE(NULLIF(TRIM({metric_q}), ''), ',', '.') AS DOUBLE)"
            sql_op = {"sum": "SUM", "avg": "AVG", "min": "MIN", "max": "MAX"}[operation]
            agg_expr = f"ROUND({sql_op}({numeric_expr}), 6)"
            where_clause = f"WHERE TRIM(COALESCE(CAST({metric_q} AS VARCHAR), '')) <> ''"
            if group_by_col:
                gq = _quote_ident(group_by_col)
                sql = (
                    f"SELECT {gq} AS group_key, {agg_expr} AS value "
                    f"FROM {table_q} "
                    f"{where_clause} "
                    f"GROUP BY {gq} "
                    f"ORDER BY value DESC LIMIT 50"
                )
            else:
                sql = f"SELECT {agg_expr} AS value FROM {table_q} {where_clause}"

    return sql, {
        "operation": operation,
        "group_by_column": group_by_col,
        "metric_column": metric_col,
        "matched_columns": matched_columns,
    }


def _select_table_for_query(query: str, dataset: ResolvedTabularDataset) -> Optional[ResolvedTabularTable]:
    if not dataset.tables:
        return None

    q_norm = _norm(query)
    for table in dataset.tables:
        table_norm = _norm(table.table_name)
        sheet_norm = _norm(table.sheet_name)
        if (table_norm and table_norm in q_norm) or (sheet_norm and sheet_norm in q_norm):
            return table

    return max(dataset.tables, key=lambda t: int(t.row_count or 0))


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


def _execute_aggregate_sync(
    *,
    query: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    timeout_seconds: float,
) -> Dict[str, Any]:
    t0 = perf_counter()
    guardrails = _build_guardrails()
    execution_limits = _build_execution_limits()
    sql, plan = _build_sql(query=query, table=table)
    with TabularExecutionSession(dataset=dataset, table=table, limits=execution_limits) as session:
        rows, guarded_sql, guard_debug = _run_guarded_query(
            session=session,
            guardrails=guardrails,
            sql=sql,
            estimated_scan_rows=int(table.row_count or 0),
            timeout_seconds=timeout_seconds,
        )
        rows_total = int(table.row_count or 0)
        rows_effective = rows_total
        if plan.get("metric_column"):
            metric_q = _quote_ident(str(plan["metric_column"]))
            rows_effective_sql = (
                f"SELECT COUNT(*) AS value FROM {_quote_ident(table.table_name)} "
                f"WHERE TRIM(COALESCE(CAST({metric_q} AS VARCHAR), '')) <> ''"
            )
            rows_effective_rows, _, _ = _run_guarded_query(
                session=session,
                guardrails=guardrails,
                sql=rows_effective_sql,
                estimated_scan_rows=int(table.row_count or 0),
                timeout_seconds=timeout_seconds,
            )
            rows_effective = _first_int(rows_effective_rows, default=rows_total)
        coverage_ratio = float(rows_effective / rows_total) if rows_total > 0 else 0.0

    observe_ms("tabular_sql_execution_ms", (perf_counter() - t0) * 1000.0, intent="aggregate")
    result_text = rows_to_result_text(rows)
    return {
        "status": "ok",
        "prompt_context": (
            "Deterministic tabular SQL result (source of truth):\n"
            f"table={table.table_name}\n"
            f"sql={guarded_sql}\n"
            f"result={result_text}"
        ),
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_aggregate",
            "deterministic_path": True,
            "tabular_sql": {
                "storage_engine": dataset.engine,
                "dataset_id": dataset.dataset_id,
                "dataset_version": dataset.dataset_version,
                "dataset_provenance_id": dataset.dataset_provenance_id,
                "table_name": table.table_name,
                "table_version": table.table_version,
                "table_provenance_id": table.provenance_id,
                "table_row_count": rows_total,
                "executed_sql": guarded_sql,
                "policy_decision": guard_debug.get("policy_decision"),
                "guardrail_flags": guard_debug.get("guardrail_flags", []),
                "sql": guarded_sql,
                "result": result_text,
                "sql_guardrails": guard_debug,
                **plan,
            },
        },
        "sources": [
            (
                f"{getattr(target_file, 'original_filename', 'unknown')} "
                f"| table={table.table_name} | dataset_v={dataset.dataset_version} "
                f"| table_v={table.table_version} | sql"
            )
        ],
        "rows_expected_total": rows_total,
        "rows_retrieved_total": rows_effective,
        "rows_used_map_total": rows_effective,
        "rows_used_reduce_total": rows_effective,
        "row_coverage_ratio": coverage_ratio,
    }


def _build_profile_payload(
    *,
    session: TabularExecutionSession,
    guardrails: SQLGuardrails,
    table: ResolvedTabularTable,
    max_columns: int,
    timeout_seconds: float,
) -> Tuple[str, Dict[str, Any], int]:
    table_q = _quote_ident(table.table_name)
    row_count_sql = f"SELECT COUNT(*) AS value FROM {table_q}"
    row_count_rows, row_count_sql_final, row_guard = _run_guarded_query(
        session=session,
        guardrails=guardrails,
        sql=row_count_sql,
        estimated_scan_rows=int(table.row_count or 0),
        timeout_seconds=timeout_seconds,
    )
    row_count = _first_int(row_count_rows, default=int(table.row_count or 0))

    sample_rows_sql = f"SELECT * FROM {table_q} LIMIT 5"
    sample_rows_raw, sample_rows_sql_final, sample_guard = _run_guarded_query(
        session=session,
        guardrails=guardrails,
        sql=sample_rows_sql,
        estimated_scan_rows=int(table.row_count or 0),
        timeout_seconds=timeout_seconds,
    )
    sample_rows = _extract_row_tuples(sample_rows_raw)

    column_stats: List[Dict[str, Any]] = []
    profiled_columns = min(len(table.columns), int(max_columns))
    executed_sql_list: List[str] = [row_count_sql_final, sample_rows_sql_final]
    policy_decisions: List[Dict[str, Any]] = []
    guardrail_flags: List[str] = []
    for debug_payload in (row_guard, sample_guard):
        policy_payload = debug_payload.get("policy_decision")
        if isinstance(policy_payload, dict):
            policy_decisions.append(dict(policy_payload))
        flags_payload = debug_payload.get("guardrail_flags")
        if isinstance(flags_payload, list):
            for flag in flags_payload:
                flag_str = str(flag)
                if flag_str and flag_str not in guardrail_flags:
                    guardrail_flags.append(flag_str)

    for col in table.columns[:profiled_columns]:
        cq = _quote_ident(col)
        non_empty_sql = f"SELECT COUNT(*) AS value FROM {table_q} WHERE TRIM(COALESCE(CAST({cq} AS VARCHAR), '')) <> ''"
        distinct_sql = (
            f"SELECT COUNT(DISTINCT {cq}) AS value "
            f"FROM {table_q} "
            f"WHERE TRIM(COALESCE(CAST({cq} AS VARCHAR), '')) <> ''"
        )
        top_values_sql = (
            f"SELECT {cq} AS value, COUNT(*) AS cnt "
            f"FROM {table_q} "
            f"WHERE TRIM(COALESCE(CAST({cq} AS VARCHAR), '')) <> '' "
            f"GROUP BY {cq} "
            f"ORDER BY cnt DESC LIMIT 3"
        )
        non_empty_rows, non_empty_sql_final, non_empty_guard = _run_guarded_query(
            session=session,
            guardrails=guardrails,
            sql=non_empty_sql,
            estimated_scan_rows=int(table.row_count or 0),
            timeout_seconds=timeout_seconds,
        )
        distinct_rows, distinct_sql_final, distinct_guard = _run_guarded_query(
            session=session,
            guardrails=guardrails,
            sql=distinct_sql,
            estimated_scan_rows=int(table.row_count or 0),
            timeout_seconds=timeout_seconds,
        )
        top_values_rows, top_values_sql_final, top_values_guard = _run_guarded_query(
            session=session,
            guardrails=guardrails,
            sql=top_values_sql,
            estimated_scan_rows=int(table.row_count or 0),
            timeout_seconds=timeout_seconds,
        )
        executed_sql_list.extend([non_empty_sql_final, distinct_sql_final, top_values_sql_final])
        for debug_payload in (non_empty_guard, distinct_guard, top_values_guard):
            policy_payload = debug_payload.get("policy_decision")
            if isinstance(policy_payload, dict):
                policy_decisions.append(dict(policy_payload))
            flags_payload = debug_payload.get("guardrail_flags")
            if isinstance(flags_payload, list):
                for flag in flags_payload:
                    flag_str = str(flag)
                    if flag_str and flag_str not in guardrail_flags:
                        guardrail_flags.append(flag_str)
        column_stats.append(
            {
                "column": col,
                "column_original_name": table.column_aliases.get(col),
                "non_empty_count": _first_int(non_empty_rows, default=0),
                "distinct_non_empty_count": _first_int(distinct_rows, default=0),
                "top_values": _extract_row_tuples(top_values_rows),
            }
        )

    profile_payload = {
        "table_name": table.table_name,
        "table_version": table.table_version,
        "table_provenance_id": table.provenance_id,
        "row_count": row_count,
        "columns_total": len(table.columns),
        "profiled_columns": profiled_columns,
        "column_stats": column_stats,
        "sample_rows": sample_rows,
    }
    prompt_context = "Deterministic tabular profile (source of truth):\n" + json.dumps(
        profile_payload,
        ensure_ascii=False,
        indent=2,
    )
    debug = {
        "profile_kind": "per_column",
        "row_count_sql": row_count_sql_final,
        "sample_rows_sql": sample_rows_sql_final,
        "executed_sql": executed_sql_list,
        "policy_decision": {
            "allowed": True,
            "reason": "allowed",
            "queries_executed": len(executed_sql_list),
            "checks": policy_decisions,
        },
        "guardrail_flags": guardrail_flags,
        "sql_guardrails_checks": [row_guard, sample_guard],
        "profiled_columns": profiled_columns,
    }
    return prompt_context, debug, row_count


def _execute_profile_sync(
    *,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    timeout_seconds: float,
) -> Dict[str, Any]:
    guardrails = _build_guardrails()
    execution_limits = _build_execution_limits()
    row_count_sql = f"SELECT COUNT(*) AS value FROM {_quote_ident(table.table_name)}"
    _, guard_debug = guardrails.enforce(row_count_sql, estimated_scan_rows=int(table.row_count or 0))

    with TabularExecutionSession(dataset=dataset, table=table, limits=execution_limits) as session:
        prompt_context, profile_debug, rows_total = _build_profile_payload(
            session=session,
            guardrails=guardrails,
            table=table,
            max_columns=int(settings.TABULAR_PROFILE_MAX_COLUMNS),
            timeout_seconds=timeout_seconds,
        )

    return {
        "status": "ok",
        "prompt_context": prompt_context,
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_profile",
            "deterministic_path": True,
            "tabular_sql": {
                "storage_engine": dataset.engine,
                "dataset_id": dataset.dataset_id,
                "dataset_version": dataset.dataset_version,
                "dataset_provenance_id": dataset.dataset_provenance_id,
                "table_name": table.table_name,
                "table_version": table.table_version,
                "table_provenance_id": table.provenance_id,
                "table_row_count": rows_total,
                "executed_sql": profile_debug.get("executed_sql", []),
                "policy_decision": profile_debug.get("policy_decision"),
                "guardrail_flags": profile_debug.get("guardrail_flags", []),
                "sql_guardrails": guard_debug,
                **profile_debug,
            },
        },
        "sources": [
            (
                f"{getattr(target_file, 'original_filename', 'unknown')} "
                f"| table={table.table_name} | dataset_v={dataset.dataset_version} "
                f"| table_v={table.table_version} | sql_profile"
            )
        ],
        "rows_expected_total": rows_total,
        "rows_retrieved_total": rows_total,
        "rows_used_map_total": rows_total,
        "rows_used_reduce_total": rows_total,
        "row_coverage_ratio": 1.0 if rows_total > 0 else 0.0,
    }


def _build_tabular_error_result(
    *,
    intent_kind: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    error_payload: Dict[str, Any],
) -> Dict[str, Any]:
    error_code = str(error_payload.get("code") or SQL_ERROR_EXECUTION_FAILED)
    error_message = str(error_payload.get("message") or "Deterministic SQL execution failed")
    executed_sql = str(error_payload.get("executed_sql") or "")
    policy_decision = error_payload.get("policy_decision")
    if not isinstance(policy_decision, dict):
        policy_decision = {"allowed": False, "reason": error_code}
    guardrail_flags = error_payload.get("guardrail_flags")
    if not isinstance(guardrail_flags, list):
        guardrail_flags = []

    clarification_prompt = (
        "Deterministic SQL execution was blocked by safety policy. "
        "Please narrow the metric/filter scope and retry."
    )
    if error_code == SQL_ERROR_TIMEOUT:
        clarification_prompt = (
            "Deterministic SQL execution timed out. "
            "Please narrow the filter or reduce the analysis scope and retry."
        )

    return {
        "status": "error",
        "clarification_prompt": clarification_prompt,
        "prompt_context": (
            "Deterministic tabular SQL execution failed.\n"
            f"error_code={error_code}\n"
            f"error_message={error_message}\n"
            f"executed_sql={executed_sql or 'n/a'}"
        ),
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": f"tabular_{intent_kind}",
            "deterministic_path": True,
            "deterministic_error": error_payload,
            "tabular_sql": {
                "storage_engine": dataset.engine,
                "dataset_id": dataset.dataset_id,
                "dataset_version": dataset.dataset_version,
                "dataset_provenance_id": dataset.dataset_provenance_id,
                "table_name": table.table_name,
                "table_version": table.table_version,
                "table_provenance_id": table.provenance_id,
                "table_row_count": int(table.row_count or 0),
                "executed_sql": executed_sql or None,
                "policy_decision": policy_decision,
                "guardrail_flags": guardrail_flags,
                "sql": executed_sql or None,
                "result": None,
                "sql_guardrails": {
                    "valid": False,
                    "reason": policy_decision.get("reason"),
                    "policy_decision": policy_decision,
                    "guardrail_flags": guardrail_flags,
                },
            },
        },
        "sources": [
            (
                f"{getattr(target_file, 'original_filename', 'unknown')} "
                f"| table={table.table_name} | dataset_v={dataset.dataset_version} "
                f"| table_v={table.table_version} | sql_error={error_code}"
            )
        ],
        "rows_expected_total": int(table.row_count or 0),
        "rows_retrieved_total": 0,
        "rows_used_map_total": 0,
        "rows_used_reduce_total": 0,
        "row_coverage_ratio": 0.0,
    }


async def execute_tabular_sql_path(
    *,
    query: str,
    files: List[Any],
) -> Optional[Dict[str, Any]]:
    intent_kind = detect_tabular_intent(query)
    if intent_kind is None:
        return None

    target_file = None
    dataset = None
    for file_obj in files:
        file_type = str(getattr(file_obj, "file_type", "") or "").lower()
        if file_type not in {"xlsx", "xls", "csv"}:
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

    timeout_seconds = float(settings.TABULAR_SQL_TIMEOUT_SECONDS)
    try:
        started = perf_counter()
        if intent_kind == "profile":
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
        return _build_tabular_error_result(
            intent_kind=intent_kind,
            dataset=dataset,
            table=table,
            target_file=target_file,
            error_payload=to_tabular_error_payload(timeout_exc),
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
            inc_counter("tabular_sql_guardrail_violation_total", intent=intent_kind, engine=dataset.engine, code=error_code)
        logger.warning(
            "Tabular SQL path failed: intent=%s code=%s message=%s",
            intent_kind,
            error_code,
            error_payload.get("message"),
            exc_info=True,
        )
        return _build_tabular_error_result(
            intent_kind=intent_kind,
            dataset=dataset,
            table=table,
            target_file=target_file,
            error_payload=error_payload,
        )

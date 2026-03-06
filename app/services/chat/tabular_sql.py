from __future__ import annotations

import asyncio
import logging
import re
from time import perf_counter
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.core.config import settings
from app.domain.chat.query_planner import detect_tabular_intent as plan_detect_tabular_intent
from app.observability.metrics import inc_counter, observe_ms
from app.services.chat.tabular_sql_pipeline import (
    build_profile_payload_pipeline,
    build_tabular_error_result_pipeline,
    execute_aggregate_sync_pipeline,
    execute_profile_sync_pipeline,
)
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
    intent_kind: str,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    target_file: Any,
    error_payload: Dict[str, Any],
) -> Dict[str, Any]:
    return build_tabular_error_result_pipeline(
        intent_kind=intent_kind,
        dataset=dataset,
        table=table,
        target_file=target_file,
        error_payload=error_payload,
        sql_error_execution_failed=SQL_ERROR_EXECUTION_FAILED,
        sql_error_timeout=SQL_ERROR_TIMEOUT,
    )


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

from __future__ import annotations

import ast
import asyncio
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from langchain_community.tools.sql_database.tool import QuerySQLDatabaseTool
from langchain_community.utilities import SQLDatabase

logger = logging.getLogger(__name__)

_COUNT_HINTS = ("сколько", "count", "количество", "число")
_SUM_HINTS = ("сумм", "итого", "sum", "total")
_AVG_HINTS = ("средн", "avg", "average", "mean")
_MIN_HINTS = ("миним", "min")
_MAX_HINTS = ("максим", "max")
_GROUP_HINTS = ("групп", "group by", "по ")
_AGGREGATE_HINTS = (
    "все строки",
    "по всем строкам",
    "all rows",
    "весь файл",
    "whole file",
    "entire file",
)
_PROFILE_HINTS = (
    "по каждой колон",
    "каждой колон",
    "все колонки",
    "всех колон",
    "общий анализ",
    "полный анализ",
    "какие данные",
    "что ты можешь сказать",
    "покажи статистики",
    "покажи метрики",
    "column statistics",
    "per column",
    "full analysis",
    "analyze dataset",
)


def is_tabular_aggregate_intent(query: str) -> bool:
    return detect_tabular_intent(query) == "aggregate"


def detect_tabular_intent(query: str) -> Optional[str]:
    q = (query or "").strip().lower()
    if not q:
        return None
    if any(h in q for h in _PROFILE_HINTS):
        return "profile"
    if any(h in q for h in (_COUNT_HINTS + _SUM_HINTS + _AVG_HINTS + _MIN_HINTS + _MAX_HINTS + _AGGREGATE_HINTS)):
        return "aggregate"
    return None


def _norm(text: str) -> str:
    return re.sub(r"[^a-zа-яё0-9]+", " ", (text or "").lower()).strip()


def _quote_ident(name: str) -> str:
    return '"' + str(name or "").replace('"', '""') + '"'


def _extract_sidecar(file_obj: Any) -> Optional[Dict[str, Any]]:
    meta = getattr(file_obj, "custom_metadata", None)
    if not isinstance(meta, dict):
        return None
    sidecar = meta.get("tabular_sidecar")
    if not isinstance(sidecar, dict):
        return None
    path = Path(str(sidecar.get("path") or "")).expanduser()
    if not path.exists():
        return None
    tables = sidecar.get("tables")
    if not isinstance(tables, list) or not tables:
        return None
    return sidecar


def _select_table_for_query(query: str, sidecar: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    tables = sidecar.get("tables")
    if not isinstance(tables, list):
        return None
    candidates = [t for t in tables if isinstance(t, dict)]
    if not candidates:
        return None
    q_norm = _norm(query)
    for table in candidates:
        name = _norm(str(table.get("table_name") or ""))
        sheet = _norm(str(table.get("sheet_name") or ""))
        if (name and name in q_norm) or (sheet and sheet in q_norm):
            return table
    return max(candidates, key=lambda t: int(t.get("row_count", 0) or 0))


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


def _pick_columns_from_query(query: str, columns: List[str]) -> List[str]:
    q_norm = _norm(query)
    matches: List[str] = []
    for col in columns:
        col_norm = _norm(col)
        if not col_norm:
            continue
        if col_norm in q_norm and col not in matches:
            matches.append(col)
    return matches


def _build_sql(
    *,
    query: str,
    table_name: str,
    columns: List[str],
) -> Tuple[str, Dict[str, Any]]:
    operation = _choose_operation(query)
    matched_columns = _pick_columns_from_query(query, columns)
    q = (query or "").lower()

    group_by_col: Optional[str] = None
    metric_col: Optional[str] = None
    if matched_columns:
        metric_col = matched_columns[0]
        if len(matched_columns) > 1 and any(h in q for h in _GROUP_HINTS):
            group_by_col = matched_columns[0]
            metric_col = matched_columns[1]

    table_q = _quote_ident(table_name)
    sql = ""
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
            numeric_expr = f"CAST(REPLACE(NULLIF(TRIM({metric_q}), ''), ',', '.') AS REAL)"
            sql_op = {"sum": "SUM", "avg": "AVG", "min": "MIN", "max": "MAX"}[operation]
            agg_expr = f"ROUND({sql_op}({numeric_expr}), 6)"
            where_clause = f"WHERE TRIM({metric_q}) <> ''"
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


async def _run_sql(sql_tool: QuerySQLDatabaseTool, sql: str) -> str:
    try:
        result = await asyncio.to_thread(sql_tool.invoke, sql)
        return str(result or "").strip()
    except Exception as exc:
        logger.warning("Tabular SQL tool error for query: %s", sql, exc_info=True)
        return f"ERROR: {type(exc).__name__}: {exc}"


def _parse_sql_rows(raw: str) -> List[Tuple[Any, ...]]:
    text = str(raw or "").strip()
    if not text or text.startswith("ERROR:"):
        return []
    try:
        parsed = ast.literal_eval(text)
    except Exception:
        return []
    if isinstance(parsed, list):
        if not parsed:
            return []
        if isinstance(parsed[0], tuple):
            return parsed
        if isinstance(parsed[0], list):
            return [tuple(x) for x in parsed]
        return [(parsed[0],)]
    if isinstance(parsed, tuple):
        return [parsed]
    return [(parsed,)]


def _first_int(rows_text: str, default: int = 0) -> int:
    rows = _parse_sql_rows(rows_text)
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


async def _build_profile_context(
    *,
    sql_tool: QuerySQLDatabaseTool,
    table_name: str,
    columns: List[str],
    table_row_count: int,
) -> Tuple[str, Dict[str, Any], int]:
    table_q = _quote_ident(table_name)
    row_count_sql = f"SELECT COUNT(*) AS value FROM {table_q}"
    row_count_text = await _run_sql(sql_tool, row_count_sql)
    row_count = _first_int(row_count_text, default=table_row_count)
    sample_rows_sql = f"SELECT * FROM {table_q} LIMIT 5"
    sample_rows_text = await _run_sql(sql_tool, sample_rows_sql)

    column_stats: List[Dict[str, Any]] = []
    max_profiled_columns = min(len(columns), 160)
    for col in columns[:max_profiled_columns]:
        cq = _quote_ident(col)
        non_empty_sql = f"SELECT COUNT(*) AS value FROM {table_q} WHERE TRIM(COALESCE({cq}, '')) <> ''"
        distinct_sql = f"SELECT COUNT(DISTINCT {cq}) AS value FROM {table_q} WHERE TRIM(COALESCE({cq}, '')) <> ''"
        top_values_sql = (
            f"SELECT {cq} AS value, COUNT(*) AS cnt "
            f"FROM {table_q} "
            f"WHERE TRIM(COALESCE({cq}, '')) <> '' "
            f"GROUP BY {cq} "
            f"ORDER BY cnt DESC LIMIT 3"
        )
        non_empty_text = await _run_sql(sql_tool, non_empty_sql)
        distinct_text = await _run_sql(sql_tool, distinct_sql)
        top_values_text = await _run_sql(sql_tool, top_values_sql)
        column_stats.append(
            {
                "column": col,
                "non_empty_count": _first_int(non_empty_text, default=0),
                "distinct_non_empty_count": _first_int(distinct_text, default=0),
                "top_values": _parse_sql_rows(top_values_text),
            }
        )

    profile_payload = {
        "table_name": table_name,
        "row_count": row_count,
        "columns_total": len(columns),
        "profiled_columns": max_profiled_columns,
        "column_stats": column_stats,
        "sample_rows": _parse_sql_rows(sample_rows_text),
    }
    prompt_context = (
        "Deterministic tabular profile (source of truth):\n"
        + json.dumps(profile_payload, ensure_ascii=False, indent=2)
    )
    debug = {
        "profile_kind": "per_column",
        "row_count_sql": row_count_sql,
        "row_count_result": row_count_text,
        "sample_rows_sql": sample_rows_sql,
        "sample_rows_result": sample_rows_text,
        "profiled_columns": max_profiled_columns,
    }
    return prompt_context, debug, row_count


async def execute_tabular_sql_path(
    *,
    query: str,
    files: List[Any],
) -> Optional[Dict[str, Any]]:
    intent_kind = detect_tabular_intent(query)
    if intent_kind is None:
        return None

    target_file = None
    sidecar = None
    for file_obj in files:
        file_type = str(getattr(file_obj, "file_type", "") or "").lower()
        if file_type not in {"xlsx", "xls", "csv"}:
            continue
        data = _extract_sidecar(file_obj)
        if data:
            target_file = file_obj
            sidecar = data
            break
    if target_file is None or sidecar is None:
        return None

    table = _select_table_for_query(query, sidecar)
    if not isinstance(table, dict):
        return None

    table_name = str(table.get("table_name") or "").strip()
    columns = table.get("columns") if isinstance(table.get("columns"), list) else []
    sidecar_path = Path(str(sidecar.get("path"))).resolve()
    if not table_name or not columns or not sidecar_path.exists():
        return None

    columns_norm = [str(c) for c in columns]
    db = SQLDatabase.from_uri(f"sqlite:///{sidecar_path}")
    sql_tool = QuerySQLDatabaseTool(db=db)
    rows_total = int(table.get("row_count", 0) or 0)
    if intent_kind == "profile":
        prompt_context, profile_debug, profile_rows_total = await _build_profile_context(
            sql_tool=sql_tool,
            table_name=table_name,
            columns=columns_norm,
            table_row_count=rows_total,
        )
        rows_total = max(rows_total, profile_rows_total)
        return {
            "prompt_context": prompt_context,
            "debug": {
                "retrieval_mode": "tabular_sql",
                "intent": "tabular_profile",
                "deterministic_path": True,
                "tabular_sql": {
                    "sidecar_path": str(sidecar_path),
                    "table_name": table_name,
                    "table_row_count": rows_total,
                    **profile_debug,
                },
            },
            "sources": [f"{getattr(target_file, 'original_filename', 'unknown')} | table={table_name} | sql_profile"],
            "rows_expected_total": rows_total,
            "rows_retrieved_total": rows_total,
            "rows_used_map_total": rows_total,
            "rows_used_reduce_total": rows_total,
            "row_coverage_ratio": 1.0 if rows_total > 0 else 0.0,
        }

    sql, plan = _build_sql(query=query, table_name=table_name, columns=columns_norm)
    result_text = await _run_sql(sql_tool, sql)
    return {
        "prompt_context": (
            "Deterministic tabular SQL result (source of truth):\n"
            f"table={table_name}\n"
            f"sql={sql}\n"
            f"result={result_text}"
        ),
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_aggregate",
            "deterministic_path": True,
            "tabular_sql": {
                "sidecar_path": str(sidecar_path),
                "table_name": table_name,
                "sql": sql,
                "result": result_text,
                **plan,
            },
        },
        "sources": [f"{getattr(target_file, 'original_filename', 'unknown')} | table={table_name} | sql"],
        "rows_expected_total": rows_total,
        "rows_retrieved_total": rows_total,
        "rows_used_map_total": rows_total,
        "rows_used_reduce_total": rows_total,
        "row_coverage_ratio": 1.0 if rows_total > 0 else 0.0,
    }

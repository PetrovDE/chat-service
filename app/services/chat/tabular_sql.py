from __future__ import annotations

import asyncio
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


def is_tabular_aggregate_intent(query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return False
    return any(h in q for h in (_COUNT_HINTS + _SUM_HINTS + _AVG_HINTS + _MIN_HINTS + _MAX_HINTS + _AGGREGATE_HINTS))


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


async def execute_tabular_sql_path(
    *,
    query: str,
    files: List[Any],
) -> Optional[Dict[str, Any]]:
    if not is_tabular_aggregate_intent(query):
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

    tables = sidecar.get("tables") or []
    table = tables[0] if isinstance(tables[0], dict) else None
    if not isinstance(table, dict):
        return None

    table_name = str(table.get("table_name") or "").strip()
    columns = table.get("columns") if isinstance(table.get("columns"), list) else []
    sidecar_path = Path(str(sidecar.get("path"))).resolve()
    if not table_name or not columns or not sidecar_path.exists():
        return None

    sql, plan = _build_sql(query=query, table_name=table_name, columns=[str(c) for c in columns])
    db = SQLDatabase.from_uri(f"sqlite:///{sidecar_path}")
    sql_tool = QuerySQLDatabaseTool(db=db)
    result_text = await asyncio.to_thread(sql_tool.invoke, sql)

    rows_total = int(table.get("row_count", 0) or 0)
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

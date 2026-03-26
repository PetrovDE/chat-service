from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Type

from app.services.chat.tabular_query_parser import parse_tabular_query
from app.services.chat.tabular_schema_resolver import find_direct_column_mentions
from app.services.tabular.sql_execution import ResolvedTabularTable


def build_aggregate_sql(
    *,
    query: str,
    table: ResolvedTabularTable,
    choose_operation_fn: Callable[[str], str],
    normalize_text_fn: Callable[[str], str],
    resolve_required_field_fn: Callable[..., Dict[str, Any]],
    quote_ident_fn: Callable[[str], str],
    group_hints: Sequence[str],
    sql_error_execution_failed: str,
    tabular_sql_exception_cls: Type[Exception],
) -> Tuple[str, Dict[str, Any]]:
    parsed = parse_tabular_query(query)
    operation = parsed.operation or choose_operation_fn(query)
    direct_mentions = find_direct_column_mentions(query, table)
    q_norm = normalize_text_fn(query)

    metric_column: Optional[str] = None
    group_by_column: Optional[str] = None
    requested_field_text = parsed.requested_field_text
    candidate_columns: List[str] = []
    scored_candidates: List[Dict[str, Any]] = []
    match_score: Optional[float] = None
    match_strategy: Optional[str] = None

    if operation in {"sum", "avg", "min", "max"}:
        if requested_field_text:
            metric_resolution = resolve_required_field_fn(
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
                raise tabular_sql_exception_cls(
                    code=sql_error_execution_failed,
                    message="Metric operation matched multiple candidate columns",
                    details={
                        "requested_field_text": None,
                        "fallback_reason": "ambiguous_metric_column",
                        "candidate_columns": [str(item) for item in direct_mentions],
                    },
                )
        else:
            raise tabular_sql_exception_cls(
                code=sql_error_execution_failed,
                message="Metric operation requires a matched column",
                details={
                    "requested_field_text": None,
                    "fallback_reason": "missing_metric_column",
                    "candidate_columns": [str(col) for col in list(table.columns)],
                },
            )

    if parsed.group_by_field_text:
        group_resolution = resolve_required_field_fn(
            field_text=parsed.group_by_field_text,
            table=table,
            detail_reason="missing_group_by_column",
            expected_dtype_family="categorical",
        )
        group_by_column = group_resolution["column"]
    elif operation == "count" and direct_mentions and any(h in q_norm for h in group_hints):
        if len(direct_mentions) == 1:
            group_by_column = str(direct_mentions[0])
        else:
            raise tabular_sql_exception_cls(
                code=sql_error_execution_failed,
                message="Count group-by matched multiple candidate columns",
                details={
                    "requested_field_text": None,
                    "fallback_reason": "ambiguous_group_by_column",
                    "candidate_columns": [str(item) for item in direct_mentions],
                },
            )

    table_q = quote_ident_fn(table.table_name)
    if operation == "count":
        if group_by_column:
            group_q = quote_ident_fn(group_by_column)
            sql = (
                f"SELECT {group_q} AS group_key, COUNT(*) AS value "
                f"FROM {table_q} "
                f"GROUP BY {group_q} "
                f"ORDER BY value DESC LIMIT 50"
            )
        else:
            sql = f"SELECT COUNT(*) AS value FROM {table_q}"
    else:
        assert metric_column is not None
        metric_q = quote_ident_fn(metric_column)
        numeric_expr = f"CAST(REPLACE(NULLIF(TRIM({metric_q}), ''), ',', '.') AS DOUBLE)"
        sql_op = {"sum": "SUM", "avg": "AVG", "min": "MIN", "max": "MAX"}[operation]
        agg_expr = f"ROUND({sql_op}({numeric_expr}), 6)"
        where_clause = f"WHERE TRIM(COALESCE(CAST({metric_q} AS VARCHAR), '')) <> ''"
        if group_by_column:
            group_q = quote_ident_fn(group_by_column)
            sql = (
                f"SELECT {group_q} AS group_key, {agg_expr} AS value "
                f"FROM {table_q} "
                f"{where_clause} "
                f"GROUP BY {group_q} "
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


def build_lookup_sql(
    *,
    query: str,
    table: ResolvedTabularTable,
    resolve_required_field_fn: Callable[..., Dict[str, Any]],
    quote_ident_fn: Callable[[str], str],
    sql_literal_fn: Callable[[str], str],
    sql_error_execution_failed: str,
    tabular_sql_exception_cls: Type[Exception],
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
        lookup_resolution = resolve_required_field_fn(
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
            raise tabular_sql_exception_cls(
                code=sql_error_execution_failed,
                message="Lookup filter value matched multiple candidate columns",
                details={
                    "requested_field_text": None,
                    "fallback_reason": "ambiguous_lookup_filter_column",
                    "candidate_columns": [str(item) for item in direct_mentions],
                },
            )
    elif lookup_value:
        raise tabular_sql_exception_cls(
            code=sql_error_execution_failed,
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
        value = str(lookup_value).strip().lower()
        where_clause = (
            f"WHERE LOWER(TRIM(COALESCE(CAST({quote_ident_fn(filter_column)} AS VARCHAR), ''))) "
            f"LIKE {sql_literal_fn('%' + value + '%')}"
        )
        retrieval_filters = {"where": {str(filter_column): {"like": f"%{value}%"}}}

    order_column = result_columns[0] if result_columns else None
    if result_columns == ["*"]:
        select_cols = "*"
    else:
        select_cols = ", ".join([quote_ident_fn(col) for col in result_columns if col != "*"])
    sql = f"SELECT {select_cols} FROM {quote_ident_fn(table.table_name)} {where_clause}".strip()
    if order_column and order_column != "*":
        sql += f" ORDER BY {quote_ident_fn(order_column)}"
    sql += " LIMIT 30"

    matched_columns: List[str] = []
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

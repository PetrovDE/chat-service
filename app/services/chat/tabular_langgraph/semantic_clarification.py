from __future__ import annotations

from typing import Any, List


_NUMERIC_NAME_TOKENS = ("amount", "total", "sum", "cost", "price", "revenue", "expense", "value", "score")


def _normalize_identifier(value: str) -> str:
    return str(value or "").strip().lower()


def candidate_numeric_columns(*, table: Any) -> List[str]:
    columns = [str(item) for item in list(getattr(table, "columns", []) or [])]
    metadata = getattr(table, "column_metadata", None)
    normalized_columns: List[str] = []
    for column in columns:
        column_meta = metadata.get(column) if isinstance(metadata, dict) else {}
        dtype = str((column_meta or {}).get("dtype") or "").lower()
        if any(token in dtype for token in ("int", "float", "double", "decimal", "numeric", "number")):
            normalized_columns.append(column)
            continue
        lowered = column.lower()
        if any(token in lowered for token in _NUMERIC_NAME_TOKENS):
            normalized_columns.append(column)
    return list(dict.fromkeys(normalized_columns))


def classify_retry_reason(*, retry_reason: str, table: Any) -> str:
    reason = _normalize_identifier(retry_reason)
    if "no_numeric_measure" in reason:
        return "no_numeric_columns_for_aggregation"
    if reason.startswith("measure_field_invalid") or "missing_measure" in reason or "missing_value_output_column" in reason:
        numeric_columns = candidate_numeric_columns(table=table)
        if not numeric_columns:
            return "no_numeric_columns_for_aggregation"
        return "missing_measure"
    if "dimension" in reason or "group_by" in reason or "task_requires_dimension" in reason or "chart_requires_dimension" in reason:
        return "missing_grouping_dimension"
    if "datetime" in reason or "time_grain" in reason or "source_datetime" in reason:
        return "ambiguous_date_grain_or_source"
    return "planner_validation_failed"


def build_retry_clarification(*, reason_code: str) -> str:
    if reason_code == "no_numeric_columns_for_aggregation":
        return (
            "I cannot run this aggregation because the dataset has no numeric columns. "
            "Please ask for count-based analysis or provide a numeric metric."
        )
    if reason_code == "missing_measure":
        return (
            "Please specify which metric to aggregate. "
            "Example: `sum of amount by city`."
        )
    if reason_code == "missing_grouping_dimension":
        return (
            "Please specify how to group the result. "
            "Example: `by city` or `by month using created_at`."
        )
    if reason_code == "ambiguous_date_grain_or_source":
        return (
            "Please clarify the datetime column and time grain. "
            "Example: `by month using created_at`."
        )
    return (
        "Please clarify the metric column and grouping scope. "
        "Example: `sum of amount by month using created_at`."
    )

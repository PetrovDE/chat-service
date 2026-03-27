from __future__ import annotations

from typing import Any, List

from app.services.chat.language import localized_text


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


def build_retry_clarification(*, reason_code: str, preferred_lang: str) -> str:
    if reason_code == "no_numeric_columns_for_aggregation":
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "\u042d\u0442\u0443 \u0430\u0433\u0440\u0435\u0433\u0430\u0446\u0438\u044e "
                "\u043d\u0435\u043b\u044c\u0437\u044f \u0432\u044b\u043f\u043e\u043b\u043d\u0438\u0442\u044c, "
                "\u043f\u043e\u0442\u043e\u043c\u0443 \u0447\u0442\u043e \u0432 \u043d\u0430\u0431\u043e\u0440\u0435 "
                "\u043d\u0435\u0442 \u0447\u0438\u0441\u043b\u043e\u0432\u044b\u0445 "
                "\u043a\u043e\u043b\u043e\u043d\u043e\u043a. \u0417\u0430\u043f\u0440\u043e\u0441\u0438\u0442\u0435 "
                "\u0430\u043d\u0430\u043b\u0438\u0437 \u043f\u043e `count` \u0438\u043b\u0438 "
                "\u0443\u043a\u0430\u0436\u0438\u0442\u0435 \u0447\u0438\u0441\u043b\u043e\u0432\u0443\u044e "
                "\u043c\u0435\u0442\u0440\u0438\u043a\u0443."
            ),
            en=(
                "I cannot run this aggregation because the dataset has no numeric columns. "
                "Please ask for count-based analysis or provide a numeric metric."
            ),
        )
    if reason_code == "missing_measure":
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "\u0423\u0442\u043e\u0447\u043d\u0438\u0442\u0435, \u043a\u0430\u043a\u0443\u044e "
                "\u043c\u0435\u0442\u0440\u0438\u043a\u0443 \u0430\u0433\u0440\u0435\u0433\u0438\u0440\u043e\u0432\u0430\u0442\u044c. "
                "\u041f\u0440\u0438\u043c\u0435\u0440: `sum of amount by city`."
            ),
            en=(
                "Please specify which metric to aggregate. "
                "Example: `sum of amount by city`."
            ),
        )
    if reason_code == "missing_grouping_dimension":
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "\u0423\u0442\u043e\u0447\u043d\u0438\u0442\u0435, \u043a\u0430\u043a "
                "\u0433\u0440\u0443\u043f\u043f\u0438\u0440\u043e\u0432\u0430\u0442\u044c "
                "\u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442. "
                "\u041f\u0440\u0438\u043c\u0435\u0440: `by city` \u0438\u043b\u0438 "
                "`by month using created_at`."
            ),
            en=(
                "Please specify how to group the result. "
                "Example: `by city` or `by month using created_at`."
            ),
        )
    if reason_code == "ambiguous_date_grain_or_source":
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "\u0423\u0442\u043e\u0447\u043d\u0438\u0442\u0435 "
                "\u043a\u043e\u043b\u043e\u043d\u043a\u0443 \u0434\u0430\u0442\u044b/"
                "\u0432\u0440\u0435\u043c\u0435\u043d\u0438 \u0438 "
                "\u0432\u0440\u0435\u043c\u0435\u043d\u043d\u0443\u044e "
                "\u0433\u0440\u0430\u043d\u0443\u043b\u044f\u0446\u0438\u044e. "
                "\u041f\u0440\u0438\u043c\u0435\u0440: `by month using created_at`."
            ),
            en=(
                "Please clarify the datetime column and time grain. "
                "Example: `by month using created_at`."
            ),
        )
    return localized_text(
        preferred_lang=preferred_lang,
        ru=(
            "\u0423\u0442\u043e\u0447\u043d\u0438\u0442\u0435 "
            "\u043c\u0435\u0442\u0440\u0438\u043a\u0443 \u0438 "
            "\u043e\u0431\u043b\u0430\u0441\u0442\u044c "
            "\u0433\u0440\u0443\u043f\u043f\u0438\u0440\u043e\u0432\u043a\u0438. "
            "\u041f\u0440\u0438\u043c\u0435\u0440: `sum of amount by month using created_at`."
        ),
        en=(
            "Please clarify the metric column and grouping scope. "
            "Example: `sum of amount by month using created_at`."
        ),
    )

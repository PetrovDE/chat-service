from __future__ import annotations

import json
import re
from typing import Any, List, Sequence

from app.services.chat.language import localized_text


_NUMERIC_RE = re.compile(r"^-?\d+(?:[.,]\d+)?$")


def _parse_tabular_rows(result_text: str) -> List[List[Any]]:
    raw = str(result_text or "").strip()
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    rows: List[List[Any]] = []
    for item in payload:
        if isinstance(item, list):
            rows.append(item)
    return rows


def _coerce_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text or not _NUMERIC_RE.match(text):
        return None
    normalized = text.replace(",", ".")
    try:
        return float(normalized)
    except Exception:
        return None


def _format_value(value: Any) -> str:
    numeric = _coerce_number(value)
    if numeric is not None:
        if float(numeric).is_integer():
            return str(int(numeric))
        return f"{numeric:.6f}".rstrip("0").rstrip(".")
    if value is None:
        return "-"
    text = str(value).strip()
    return text or "-"


def _sanitize_table_cell(value: Any) -> str:
    text = _format_value(value)
    text = text.replace("\n", " ").replace("\r", " ")
    return text.replace("|", "/")


def _metric_expression(*, operation: str, metric_column: str | None) -> str:
    op = str(operation or "value").strip().lower() or "value"
    metric = str(metric_column or "").strip()
    if metric:
        return f"{op}({metric})"
    if op == "count":
        return "count(*)"
    return op


def _build_grouped_table(
    *,
    rows: Sequence[Sequence[Any]],
    group_by_column: str | None,
    metric_expr: str,
    max_rows: int,
) -> List[str]:
    header_left = str(group_by_column or "group").strip() or "group"
    visible_rows = list(rows[: max(1, int(max_rows))])
    lines: List[str] = [
        f"| {header_left} | {metric_expr} |",
        "|---|---:|",
    ]
    for row in visible_rows:
        bucket = _sanitize_table_cell(row[0] if len(row) > 0 else "")
        value = _sanitize_table_cell(row[1] if len(row) > 1 else "")
        lines.append(f"| {bucket} | {value} |")
    return lines


def build_aggregation_response_text(
    *,
    preferred_lang: str,
    result_text: str,
    operation: str,
    metric_column: str | None = None,
    group_by_column: str | None = None,
    source_scope: str | None = None,
    max_rows: int = 8,
) -> str:
    rows = _parse_tabular_rows(result_text)
    source_scope_value = str(source_scope or "").strip()
    metric_expr = _metric_expression(operation=operation, metric_column=metric_column)

    if not rows:
        base = localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "\u0417\u0430\u043f\u0440\u043e\u0441 \u0432\u044b\u043f\u043e\u043b\u043d\u0435\u043d, "
                "\u043d\u043e \u0440\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442 \u043f\u0443\u0441\u0442."
            ),
            en="The query executed successfully, but returned no rows.",
        )
        if not source_scope_value:
            return base
        source_line = localized_text(
            preferred_lang=preferred_lang,
            ru=f"\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a: {source_scope_value}.",
            en=f"Data source: {source_scope_value}.",
        )
        return f"{base} {source_line}".strip()

    first_row = rows[0]
    if len(first_row) <= 1:
        value = _format_value(first_row[0] if first_row else None)
        base = localized_text(
            preferred_lang=preferred_lang,
            ru=(
                f"\u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442 `{metric_expr}`: {value}."
            ),
            en=f"Result for `{metric_expr}`: {value}.",
        )
        if not source_scope_value:
            return base
        source_line = localized_text(
            preferred_lang=preferred_lang,
            ru=f"\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a: {source_scope_value}.",
            en=f"Data source: {source_scope_value}.",
        )
        return f"{base} {source_line}".strip()

    group_label = str(group_by_column or "").strip() or "group"
    top_bucket = _format_value(first_row[0])
    top_value = _format_value(first_row[1] if len(first_row) > 1 else None)
    intro = localized_text(
        preferred_lang=preferred_lang,
        ru=(
            f"\u0421\u0433\u0440\u0443\u043f\u043f\u0438\u0440\u043e\u0432\u0430\u043b "
            f"\u043f\u043e `{group_label}` \u0438 \u043f\u043e\u0441\u0447\u0438\u0442\u0430\u043b `{metric_expr}`. "
            f"\u041b\u0438\u0434\u0435\u0440: `{top_bucket}` \u2014 {top_value}."
        ),
        en=(
            f"Grouped by `{group_label}` and computed `{metric_expr}`. "
            f"Top bucket is `{top_bucket}` with {top_value}."
        ),
    )

    shown = min(len(rows), max(1, int(max_rows)))
    if shown < len(rows):
        coverage_line = localized_text(
            preferred_lang=preferred_lang,
            ru=f"\u041f\u043e\u043a\u0430\u0437\u0430\u043d\u044b \u043f\u0435\u0440\u0432\u044b\u0435 {shown} \u0438\u0437 {len(rows)} \u0441\u0442\u0440\u043e\u043a.",
            en=f"Showing first {shown} of {len(rows)} rows.",
        )
    else:
        coverage_line = localized_text(
            preferred_lang=preferred_lang,
            ru=f"\u041f\u043e\u043a\u0430\u0437\u0430\u043d\u044b \u0432\u0441\u0435 {len(rows)} \u0441\u0442\u0440\u043e\u043a.",
            en=f"Showing all {len(rows)} rows.",
        )

    table_lines = _build_grouped_table(
        rows=rows,
        group_by_column=group_by_column,
        metric_expr=metric_expr,
        max_rows=max_rows,
    )
    parts: List[str] = [intro, coverage_line, "\n".join(table_lines)]
    if source_scope_value:
        parts.append(
            localized_text(
                preferred_lang=preferred_lang,
                ru=f"\u0418\u0441\u0442\u043e\u0447\u043d\u0438\u043a: {source_scope_value}.",
                en=f"Data source: {source_scope_value}.",
            )
        )
    return "\n\n".join([part for part in parts if str(part or "").strip()]).strip()

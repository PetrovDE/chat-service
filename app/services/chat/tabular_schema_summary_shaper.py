from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.services.tabular.sql_execution import ResolvedTabularDataset, ResolvedTabularTable

_IDENTIFIER_RE = re.compile(r"(^id$|_id$|(^|_)id_|\buuid\b|\bkey\b)")
_NUMERIC_DTYPE_TOKENS = ("int", "float", "double", "decimal", "number", "numeric")
_DATETIME_DTYPE_TOKENS = ("date", "time", "timestamp", "datetime")
_TEXT_DTYPE_TOKENS = ("char", "str", "text", "object")


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _is_identifier_like(column_name: str) -> bool:
    normalized = _normalize_text(column_name).replace(" ", "_")
    if not normalized:
        return False
    return bool(_IDENTIFIER_RE.search(normalized))


def _dtype_family(dtype_value: Any) -> str:
    dtype = _normalize_text(str(dtype_value or ""))
    if not dtype:
        return "unknown"
    if any(token in dtype for token in _DATETIME_DTYPE_TOKENS):
        return "datetime"
    if any(token in dtype for token in _NUMERIC_DTYPE_TOKENS):
        return "numeric"
    if any(token in dtype for token in _TEXT_DTYPE_TOKENS):
        return "text"
    if "bool" in dtype:
        return "boolean"
    return "other"


def _column_metadata(table: ResolvedTabularTable, column_name: str) -> Dict[str, Any]:
    raw = table.column_metadata.get(column_name) if isinstance(table.column_metadata, dict) else None
    return raw if isinstance(raw, dict) else {}


def rank_relevant_fields(
    *,
    table: ResolvedTabularTable,
    limit: int = 6,
) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    aliases = table.column_aliases if isinstance(table.column_aliases, dict) else {}
    for index, raw_column in enumerate(list(table.columns or [])):
        column_name = str(raw_column or "").strip()
        if not column_name:
            continue
        metadata = _column_metadata(table, column_name)
        dtype_family = _dtype_family(metadata.get("dtype"))
        cardinality_hint = str(metadata.get("cardinality_hint") or "").strip().lower()
        sample_values = metadata.get("sample_values")
        alias = str(aliases.get(column_name) or "").strip()
        score = 0.0
        reasons: List[str] = []

        if dtype_family == "numeric":
            score += 4.0
            reasons.append("numeric values")
        elif dtype_family == "datetime":
            score += 4.0
            reasons.append("date/time values")
        elif dtype_family == "text":
            score += 1.0

        if cardinality_hint in {"single", "low", "medium"}:
            score += 1.5
            reasons.append(f"{cardinality_hint} cardinality")
        elif cardinality_hint == "high":
            score += 0.4

        if isinstance(sample_values, list) and sample_values:
            score += 0.8
            reasons.append("sample values available")

        if alias and _normalize_text(alias) != _normalize_text(column_name):
            score += 0.4
            reasons.append("display alias available")

        identifier_like = _is_identifier_like(column_name)
        if identifier_like:
            score -= 2.5
            reasons.append("identifier-like field")

        # Preserve deterministic ordering when scores are close.
        score += max(0.0, 0.2 - (float(index) * 0.01))

        if not reasons:
            reasons.append("schema column")

        ranked.append(
            {
                "name": column_name,
                "alias": alias or None,
                "score": round(float(score), 4),
                "reasons": reasons[:3],
                "dtype_family": dtype_family,
                "identifier_like": identifier_like,
                "order_index": index,
            }
        )

    ranked.sort(
        key=lambda item: (
            -float(item.get("score", 0.0) or 0.0),
            bool(item.get("identifier_like", False)),
            int(item.get("order_index", 0) or 0),
            str(item.get("name") or "").lower(),
        )
    )
    max_items = max(1, int(limit))
    return ranked[:max_items]


def _table_scope_label(table: ResolvedTabularTable) -> str:
    sheet_name = str(table.sheet_name or "").strip()
    table_name = str(table.table_name or "").strip()
    if sheet_name:
        return f"sheet {sheet_name} (table {table_name})"
    return f"table {table_name or 'unknown'}"


def _table_data_characteristics(table: ResolvedTabularTable) -> str:
    relevant = rank_relevant_fields(table=table, limit=max(1, min(len(table.columns), 12)))
    numeric_count = sum(1 for item in relevant if item.get("dtype_family") == "numeric")
    datetime_count = sum(1 for item in relevant if item.get("dtype_family") == "datetime")
    text_count = sum(1 for item in relevant if item.get("dtype_family") == "text")

    characteristics: List[str] = []
    if numeric_count > 0:
        characteristics.append("numeric measures")
    if datetime_count > 0:
        characteristics.append("date/time dimensions")
    if text_count > 0:
        characteristics.append("categorical/text fields")
    if not characteristics:
        return "schema metadata is limited; core fields are still available for analysis"
    return "includes " + ", ".join(characteristics[:3])


def _summary_focus(query: str) -> str:
    q = _normalize_text(query)
    if not q:
        return "general"
    if "sheet" in q or "table" in q:
        return "sheets_tables"
    if "column" in q or "field" in q:
        return "fields"
    if "analy" in q or "analysis" in q:
        return "analysis"
    return "general"


def _compose_next_questions(
    *,
    tables_summary: Sequence[Dict[str, Any]],
    focus: str,
) -> List[str]:
    suggestions: List[str] = []
    first_table = tables_summary[0] if tables_summary else None
    second_table = tables_summary[1] if len(tables_summary) > 1 else None

    if focus == "sheets_tables":
        if first_table:
            scope = str(first_table.get("scope_label") or "the selected sheet")
            suggestions.append(f"Use {scope} and list its key columns.")
        if first_table and second_table:
            left = str(first_table.get("scope_label") or "sheet 1")
            right = str(second_table.get("scope_label") or "sheet 2")
            suggestions.append(f"Compare row counts between {left} and {right}.")
    else:
        if first_table:
            relevant_fields = list(first_table.get("relevant_fields") or [])
            numeric = next((f for f in relevant_fields if f.get("dtype_family") == "numeric"), None)
            dimension = next(
                (
                    f
                    for f in relevant_fields
                    if f.get("dtype_family") in {"datetime", "text", "other"}
                    and str(f.get("name") or "").strip()
                    != str((numeric or {}).get("name") or "").strip()
                ),
                None,
            )
            scope = str(first_table.get("scope_label") or "the selected table")
            if numeric and dimension:
                suggestions.append(
                    f"In {scope}, show {numeric.get('name')} by {dimension.get('name')}."
                )
            elif numeric:
                suggestions.append(
                    f"In {scope}, summarize {numeric.get('name')} (count, min, max, average)."
                )
            else:
                suggestions.append(f"In {scope}, show top values for the most important categorical field.")

        if first_table:
            suggestions.append("List all columns if you want a complete schema dump.")

    deduped: List[str] = []
    seen = set()
    for item in suggestions:
        value = str(item or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
        if len(deduped) >= 3:
            break
    return deduped


def build_schema_summary_context(
    *,
    query: str,
    dataset: ResolvedTabularDataset,
    target_file: Any,
    selected_table: Optional[ResolvedTabularTable],
) -> Dict[str, Any]:
    file_name = str(
        getattr(target_file, "original_filename", "")
        or getattr(target_file, "stored_filename", "")
        or "unknown"
    )
    tables = list(dataset.tables or [])
    tables_summary: List[Dict[str, Any]] = []
    total_rows = 0
    for table in tables:
        rows = int(getattr(table, "row_count", 0) or 0)
        total_rows += rows
        relevant_fields = rank_relevant_fields(table=table, limit=6)
        tables_summary.append(
            {
                "table_name": str(table.table_name or ""),
                "sheet_name": str(table.sheet_name or ""),
                "scope_label": _table_scope_label(table),
                "row_count": rows,
                "columns_total": int(len(list(table.columns or []))),
                "data_characteristics": _table_data_characteristics(table),
                "relevant_fields": relevant_fields,
            }
        )

    selected_scope = None
    if selected_table is not None:
        selected_scope = {
            "table_name": str(selected_table.table_name or ""),
            "sheet_name": str(selected_table.sheet_name or ""),
            "scope_label": _table_scope_label(selected_table),
        }

    focus = _summary_focus(query)
    next_questions = _compose_next_questions(tables_summary=tables_summary, focus=focus)
    summary_statement = (
        f"Workbook with {len(tables_summary)} table(s)/sheet(s), total rows={int(total_rows)}."
        if len(tables_summary) > 1
        else (
            f"Single table file with rows={int(total_rows)} and columns={tables_summary[0].get('columns_total', 0)}."
            if tables_summary
            else "No tabular tables were resolved for schema summary."
        )
    )

    return {
        "schema_summary_version": "tabular_schema_summary_v2",
        "summary_focus": focus,
        "summary_statement": summary_statement,
        "file_name": file_name,
        "dataset_id": str(dataset.dataset_id or ""),
        "dataset_version": int(dataset.dataset_version or 0),
        "tables_total": len(tables_summary),
        "rows_total": int(total_rows),
        "selected_scope": selected_scope,
        "tables": tables_summary,
        "next_question_suggestions": next_questions,
    }


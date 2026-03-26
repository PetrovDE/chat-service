from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence


def _to_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _format_number(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _parse_tabular_rows(result_text: str) -> List[List[Any]]:
    raw = str(result_text or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    rows: List[List[Any]] = []
    for item in parsed:
        if isinstance(item, list):
            rows.append(item)
    return rows


def extract_chart_highlights(*, result_text: str, max_items: int = 3) -> List[str]:
    rows = _parse_tabular_rows(result_text)
    if not rows:
        return ["No non-empty buckets were returned by the deterministic chart query."]

    ranked: List[Dict[str, Any]] = []
    for row in rows:
        if len(row) < 2:
            continue
        bucket = str(row[0]).strip()
        value = _to_number(row[1])
        if not bucket or value is None:
            continue
        ranked.append({"bucket": bucket, "value": value})

    if not ranked:
        return ["No numeric chart buckets were returned by the deterministic chart query."]

    max_items = max(1, int(max_items))
    top = ranked[:max_items]
    highlights: List[str] = []
    first = top[0]
    highlights.append(f"Top bucket: `{first['bucket']}` ({_format_number(float(first['value']))}).")
    if len(top) > 1:
        second = top[1]
        highlights.append(f"Second bucket: `{second['bucket']}` ({_format_number(float(second['value']))}).")
    total = sum(float(item["value"]) for item in ranked)
    top_total = sum(float(item["value"]) for item in top)
    if total > 0 and len(top) > 1:
        coverage = (top_total / total) * 100.0
        highlights.append(f"Top {len(top)} buckets cover {coverage:.1f}% of counted rows.")
    return highlights


def build_column_followup_suggestion(
    *,
    alternatives: Sequence[str],
    requested_fields: Sequence[str],
) -> str:
    preferred = str(next((item for item in list(alternatives or []) if str(item or "").strip()), "")).strip()
    requested = str(next((item for item in list(requested_fields or []) if str(item or "").strip()), "")).strip()
    if preferred and requested:
        return f"Best next question: `Use {preferred} instead of {requested}, then run the same analysis.`"
    if preferred:
        return f"Best next question: `Run the analysis using {preferred}.`"
    return "Best next question: `Tell me the exact column name you want to analyze.`"


def build_scope_followup_suggestion(*, scope_options: Sequence[str]) -> str:
    preferred = str(next((item for item in list(scope_options or []) if str(item or "").strip()), "")).strip()
    if preferred:
        return f"Best next question: `Use {preferred}.`"
    return "Best next question: `Name the file or sheet/table to use.`"


def _source_scope_hint(rag_sources: Sequence[str]) -> str:
    sources = [str(item).strip() for item in list(rag_sources or []) if str(item).strip()]
    if not sources:
        return "Primary source scope is available in deterministic sources metadata."
    return f"Primary source scope: {sources[0]}."


def _schema_relevant_fields(columns: Sequence[str]) -> List[str]:
    if not columns:
        return []

    time_tokens = ("date", "time", "month", "year", "day", "timestamp")
    measure_tokens = ("amount", "total", "sum", "avg", "price", "cost", "revenue", "sales", "qty", "quantity", "score", "rate", "value")
    identifier_tokens = ("id", "uuid", "key")

    time_fields: List[str] = []
    measure_fields: List[str] = []
    category_fields: List[str] = []
    id_fields: List[str] = []
    seen = set()
    for raw in columns:
        field = str(raw or "").strip()
        if not field:
            continue
        key = field.lower()
        if key in seen:
            continue
        seen.add(key)
        lowered = key
        if any(token in lowered for token in time_tokens):
            time_fields.append(field)
            continue
        if any(token in lowered for token in measure_tokens):
            measure_fields.append(field)
            continue
        if any(token in lowered for token in identifier_tokens):
            id_fields.append(field)
            continue
        category_fields.append(field)

    ranked = [*time_fields, *measure_fields, *category_fields, *id_fields]
    if not ranked:
        return []
    return ranked[:6]


def _schema_hint_block(tabular_sql_result: Dict[str, Any]) -> str:
    debug = tabular_sql_result.get("debug") if isinstance(tabular_sql_result.get("debug"), dict) else {}
    tabular_debug = debug.get("tabular_sql") if isinstance(debug.get("tabular_sql"), dict) else {}
    schema_payload = tabular_debug.get("schema_payload") if isinstance(tabular_debug.get("schema_payload"), dict) else {}
    if not schema_payload:
        return ""

    table_name = str(schema_payload.get("table_name") or "").strip()
    row_count = schema_payload.get("row_count")
    columns = schema_payload.get("columns") if isinstance(schema_payload.get("columns"), list) else []
    selected_fields = _schema_relevant_fields(columns)

    lines: List[str] = []
    if table_name:
        lines.append(f"- Table: {table_name}")
    if row_count is not None:
        lines.append(f"- Row count: {row_count}")
    if columns:
        lines.append(f"- Columns total: {len(columns)}")
    if selected_fields:
        lines.append(f"- Most relevant analysis fields (max 6): {', '.join(selected_fields)}")
    if not lines:
        return ""
    return "Schema summary hint:\n" + "\n".join(lines)


def _aggregation_hint_block(tabular_sql_result: Dict[str, Any]) -> str:
    debug = tabular_sql_result.get("debug") if isinstance(tabular_sql_result.get("debug"), dict) else {}
    tabular_debug = debug.get("tabular_sql") if isinstance(debug.get("tabular_sql"), dict) else {}
    result_text = str(tabular_debug.get("result") or "").strip()
    rows = _parse_tabular_rows(result_text)
    if not rows:
        return ""

    first = rows[0]
    if len(first) == 1:
        return f"Deterministic direct answer candidate: {first[0]}."
    if len(first) >= 2:
        preview_rows = rows[:3]
        return f"Deterministic grouped result preview (first rows): {json.dumps(preview_rows, ensure_ascii=False)}"
    return ""


def build_tabular_answer_quality_guidance(
    *,
    selected_route: str,
    tabular_sql_result: Dict[str, Any],
    rag_sources: Sequence[str],
) -> str:
    route = str(selected_route or "").strip().lower()
    lines: List[str] = [
        "Answer quality requirements:",
        "- Start with the direct answer in the first sentence and keep exact deterministic values.",
        "- Keep wording concise and natural (avoid robotic templates and repetitive phrasing).",
        "- Mention what data was used (file/sheet/table and key field) when available.",
        "- Add a short \"so what\" interpretation only if it is directly supported by deterministic output.",
        "- Optionally add one concrete follow-up question when it helps the user continue analysis.",
        "- Do not include headings named Answer/Limitations/Sources; these are appended by the response pipeline.",
        "- Do not dump raw JSON unless the user explicitly asks for raw output.",
        f"- {_source_scope_hint(rag_sources)}",
    ]

    if route == "schema_question":
        lines.extend(
            [
                "- For schema/file summary requests, describe available data first, then list only the most relevant fields.",
                "- Keep schema summaries concise; avoid full column dumps unless the user explicitly asks for all columns.",
            ]
        )
        schema_hint = _schema_hint_block(tabular_sql_result)
        if schema_hint:
            lines.append(schema_hint)
    else:
        if route in {"aggregation", "filtering", "overview"}:
            lines.append("- For aggregation/table results, provide the direct value first, then a brief interpretation.")
        aggregation_hint = _aggregation_hint_block(tabular_sql_result)
        if aggregation_hint:
            lines.append(f"- {aggregation_hint}")

    return "\n".join(lines).strip()

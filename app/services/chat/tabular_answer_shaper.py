from __future__ import annotations

import json
from typing import Any, Dict, List, Sequence

from app.services.chat.chart_insight_shaper import extract_chart_highlights


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


def _schema_tables(schema_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    tables_raw = schema_payload.get("tables")
    if not isinstance(tables_raw, list):
        return []
    return [item for item in tables_raw if isinstance(item, dict)]


def _preferred_schema_table(schema_payload: Dict[str, Any]) -> Dict[str, Any]:
    tables = _schema_tables(schema_payload)
    selected_scope = schema_payload.get("selected_scope")
    selected_table_name = None
    if isinstance(selected_scope, dict):
        selected_table_name = str(selected_scope.get("table_name") or "").strip().lower()
    if selected_table_name:
        for table in tables:
            table_name = str(table.get("table_name") or "").strip().lower()
            if table_name == selected_table_name:
                return table
    return tables[0] if tables else {}


def _render_relevant_fields(preferred_table: Dict[str, Any]) -> str:
    relevant = preferred_table.get("relevant_fields")
    if not isinstance(relevant, list):
        return ""
    rendered: List[str] = []
    for item in relevant[:6]:
        if not isinstance(item, dict):
            continue
        field_name = str(item.get("name") or "").strip()
        if not field_name:
            continue
        reasons = item.get("reasons")
        reasons_text = ""
        if isinstance(reasons, list) and reasons:
            reasons_text = "; ".join([str(reason).strip() for reason in reasons if str(reason).strip()][:2])
        if reasons_text:
            rendered.append(f"{field_name} ({reasons_text})")
        else:
            rendered.append(field_name)
    return ", ".join(rendered)


def _schema_hint_block(tabular_sql_result: Dict[str, Any]) -> str:
    debug = tabular_sql_result.get("debug") if isinstance(tabular_sql_result.get("debug"), dict) else {}
    tabular_debug = debug.get("tabular_sql") if isinstance(debug.get("tabular_sql"), dict) else {}
    schema_payload = tabular_debug.get("schema_payload") if isinstance(tabular_debug.get("schema_payload"), dict) else {}
    if not schema_payload:
        return ""

    file_name = str(schema_payload.get("file_name") or "").strip()
    summary_statement = str(schema_payload.get("summary_statement") or "").strip()
    tables_total = int(schema_payload.get("tables_total", 0) or 0)
    rows_total = int(schema_payload.get("rows_total", 0) or 0)
    selected_scope = schema_payload.get("selected_scope") if isinstance(schema_payload.get("selected_scope"), dict) else {}
    selected_scope_label = str(selected_scope.get("scope_label") or "").strip()
    preferred_table = _preferred_schema_table(schema_payload)
    table_scope = str(preferred_table.get("scope_label") or "").strip()
    relevant_fields_preview = _render_relevant_fields(preferred_table)
    next_questions = schema_payload.get("next_question_suggestions")
    next_questions_preview = (
        " | ".join([str(item).strip() for item in list(next_questions or []) if str(item).strip()][:3])
        if isinstance(next_questions, list)
        else ""
    )
    tables_preview = []
    for table in _schema_tables(schema_payload)[:4]:
        scope = str(table.get("scope_label") or "").strip()
        row_count = int(table.get("row_count", 0) or 0)
        if scope:
            tables_preview.append(f"{scope}, rows={row_count}")

    lines: List[str] = []
    if file_name:
        lines.append(f"- File: {file_name}")
    if selected_scope_label:
        lines.append(f"- Selected scope: {selected_scope_label}")
    elif table_scope:
        lines.append(f"- Preferred scope for examples: {table_scope}")
    if summary_statement:
        lines.append(f"- First-impression summary: {summary_statement}")
    if tables_total > 1:
        lines.append(f"- Tables/sheets total: {tables_total} (rows total across tables: {rows_total})")
    if tables_preview:
        lines.append(f"- Available scopes preview: {' | '.join(tables_preview)}")
    if relevant_fields_preview:
        lines.append(f"- Most relevant analysis fields (max 6): {relevant_fields_preview}")
    if next_questions_preview:
        lines.append(f"- Suggested next questions: {next_questions_preview}")
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
                "- For schema/file summary requests, open with a one-sentence first impression of the file and current scope.",
                "- Explain what kind of data is present before listing fields.",
                "- Prioritize only the most analysis-relevant fields with brief evidence; do not dump every column by default.",
                "- If multiple sheets/tables exist, name them clearly and do not guess one silently.",
                "- End with one or two concrete next-step questions tailored to available fields/scope.",
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

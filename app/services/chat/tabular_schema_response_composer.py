from __future__ import annotations

from typing import Any, Dict, List

from app.services.chat.language import localized_text


def _schema_payload_from_debug(rag_debug: Dict[str, Any]) -> Dict[str, Any]:
    tabular_debug = rag_debug.get("tabular_sql") if isinstance(rag_debug.get("tabular_sql"), dict) else {}
    schema_payload = tabular_debug.get("schema_payload") if isinstance(tabular_debug.get("schema_payload"), dict) else {}
    return dict(schema_payload)


def _compact_columns(columns: List[str], *, max_items: int = 12) -> str:
    visible = [str(item).strip() for item in columns if str(item).strip()][: max(1, int(max_items))]
    if not visible:
        return ""
    return ", ".join(visible)


def build_schema_summary_response(
    *,
    preferred_lang: str,
    rag_debug: Dict[str, Any],
) -> str:
    schema_payload = _schema_payload_from_debug(rag_debug)
    if not schema_payload:
        return localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "\u0421\u0445\u0435\u043c\u0430 \u0444\u0430\u0439\u043b\u0430 \u043d\u0435 \u043e\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0430. "
                "\u0423\u0442\u043e\u0447\u043d\u0438\u0442\u0435, \u043a\u0430\u043a\u043e\u0439 \u0444\u0430\u0439\u043b \u0438\u043b\u0438 \u043b\u0438\u0441\u0442 \u043d\u0443\u0436\u043d\u043e \u043e\u043f\u0438\u0441\u0430\u0442\u044c."
            ),
            en="The file schema is not available. Please specify which file or sheet should be described.",
        )

    file_name = str(schema_payload.get("file_name") or "unknown").strip()
    tables_total = int(schema_payload.get("tables_total", 0) or 0)
    rows_total = int(schema_payload.get("rows_total", 0) or 0)
    selected_scope = schema_payload.get("selected_scope") if isinstance(schema_payload.get("selected_scope"), dict) else {}
    scope_label = str(selected_scope.get("scope_label") or "").strip()
    columns = [str(item).strip() for item in list(schema_payload.get("columns") or []) if str(item).strip()]
    columns_total = len(columns)
    columns_preview = _compact_columns(columns)

    if tables_total <= 1:
        ru_table_phrase = (
            "\u043e\u0434\u043d\u0430 \u0442\u0430\u0431\u043b\u0438\u0446\u0430"
            if tables_total == 1
            else "\u043d\u0435 \u043d\u0430\u0439\u0434\u0435\u043d\u043e \u0442\u0430\u0431\u043b\u0438\u0446"
        )
        intro_ru = (
            f"\u0412 \u0444\u0430\u0439\u043b\u0435 {file_name} "
            f"{ru_table_phrase}"
        )
        if scope_label:
            intro_ru += f" ({scope_label})"
        intro_ru += f" \u0441 {columns_total} \u0441\u0442\u043e\u043b\u0431\u0446\u0430\u043c\u0438 \u0438 {rows_total} \u0441\u0442\u0440\u043e\u043a\u0430\u043c\u0438."

        intro_en = (
            f"File {file_name} has "
            f"{'one table' if tables_total == 1 else 'no resolved tables'}"
        )
        if scope_label:
            intro_en += f" ({scope_label})"
        intro_en += f" with {columns_total} columns and {rows_total} rows."
    else:
        intro_ru = (
            f"\u0412 \u0444\u0430\u0439\u043b\u0435 {file_name} {tables_total} \u0442\u0430\u0431\u043b\u0438\u0446/\u043b\u0438\u0441\u0442\u043e\u0432 "
            f"\u0438 \u0432\u0441\u0435\u0433\u043e {rows_total} \u0441\u0442\u0440\u043e\u043a."
        )
        intro_en = f"File {file_name} has {tables_total} tables/sheets and {rows_total} rows in total."

    if columns_preview:
        columns_ru = f"\u0421\u0442\u043e\u043b\u0431\u0446\u044b: {columns_preview}."
        columns_en = f"Columns: {columns_preview}."
    else:
        columns_ru = "\u0421\u043f\u0438\u0441\u043e\u043a \u0441\u0442\u043e\u043b\u0431\u0446\u043e\u0432 \u043f\u0443\u0441\u0442."
        columns_en = "The column list is empty."

    next_step_ru = (
        "\u041c\u043e\u0433\u0443 \u043f\u043e\u043a\u0430\u0437\u0430\u0442\u044c \u043f\u043e\u043b\u043d\u043e\u0435 \u043e\u043f\u0438\u0441\u0430\u043d\u0438\u0435 "
        "\u043f\u043e \u043a\u0430\u0436\u0434\u043e\u043c\u0443 \u0441\u0442\u043e\u043b\u0431\u0446\u0443 \u0438\u043b\u0438 \u0441\u0440\u0430\u0437\u0443 \u043f\u043e\u0441\u0442\u0440\u043e\u0438\u0442\u044c \u0430\u043d\u0430\u043b\u0438\u0442\u0438\u043a\u0443."
    )
    next_step_en = "I can list detailed field descriptions or immediately run analytics on these columns."

    return localized_text(
        preferred_lang=preferred_lang,
        ru=f"{intro_ru} {columns_ru} {next_step_ru}",
        en=f"{intro_en} {columns_en} {next_step_en}",
    )

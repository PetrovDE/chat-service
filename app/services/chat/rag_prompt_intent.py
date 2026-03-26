from __future__ import annotations

import re
from typing import Any, Dict

from app.domain.chat.query_planner import detect_tabular_intent
from app.services.chat.complex_analytics import is_complex_analytics_query
from app.services.chat.tabular_intent_router import classify_tabular_query

_EXPLICIT_FILE_REQUEST_RE = re.compile(
    r"(?:\bfile\b|\bfiles\b|\bdocument\b|\bdocuments\b|\bdataset\b|\bspreadsheet\b|\bcsv\b|\bxlsx\b|\bexcel\b|\u0444\u0430\u0439\u043b|\u0444\u0430\u0439\u043b\u0430|\u0434\u043e\u043a\u0443\u043c\u0435\u043d\u0442|\u0434\u0430\u0442\u0430\u0441\u0435\u0442)"
)
_EXPLICIT_DATA_CONTEXT_RE = re.compile(
    r"(?:\b(?:from|in|by)\s+(?:the\s+)?(?:table|data|sheet)\b|\b(?:table|data)\b\s+(?:summary|analysis)|\b(?:\u043f\u043e|\u0438\u0437|\u0432)\s+(?:\u0442\u0430\u0431\u043b\u0438\u0446|\u0434\u0430\u043d\u043d|\u043b\u0438\u0441\u0442))"
)
_TABULAR_ACTION_SIGNAL_RE = re.compile(
    r"(?:\b(?:chart|graph|plot|histogram|trend|comparison|schema|column|columns|row|rows|record|records|table|sheet|spreadsheet|dataset)\b|(?:\u0433\u0440\u0430\u0444\u0438\u043a|\u0434\u0438\u0430\u0433\u0440\u0430\u043c|\u0441\u0442\u043e\u043b\u0431\u0435\u0446|\u0441\u0442\u043e\u043b\u0431\u0446\u044b|\u043a\u043e\u043b\u043e\u043d\u043a|\u0441\u0442\u0440\u043e\u043a|\u0442\u0430\u0431\u043b\u0438\u0446|\u043b\u0438\u0441\u0442))"
)


def classify_top_level_intent(
    *,
    query: str,
    resolution_meta: Dict[str, Any],
) -> str:
    text = str(query or "").strip()
    if not text:
        return "general_chat"

    requested_file_names = list(resolution_meta.get("requested_file_names") or [])
    if requested_file_names:
        return "file_lookup"

    lowered = text.lower()
    explicit_file_or_data_signal = bool(
        _EXPLICIT_FILE_REQUEST_RE.search(lowered)
        or _EXPLICIT_DATA_CONTEXT_RE.search(lowered)
    )
    tabular_action_signal = bool(_TABULAR_ACTION_SIGNAL_RE.search(lowered))
    strong_file_data_signal = bool(explicit_file_or_data_signal or tabular_action_signal)

    if is_complex_analytics_query(text):
        if strong_file_data_signal:
            return "tabular_analytics"
        return "general_chat"

    try:
        tabular_decision = classify_tabular_query(query=text, table=None)
        selected_route = str(getattr(tabular_decision, "selected_route", "") or "")
        requested_fields = list(getattr(tabular_decision, "requested_fields", []) or [])
        lookup_field_text = str(getattr(tabular_decision, "lookup_field_text", "") or "").strip()
    except Exception:
        selected_route = ""
        requested_fields = []
        lookup_field_text = ""

    if selected_route == "schema_question":
        if strong_file_data_signal:
            return "schema_question"
        return "general_chat"
    if selected_route == "filtering":
        if strong_file_data_signal or lookup_field_text:
            return "file_lookup"
        return "general_chat"
    if selected_route in {"overview", "aggregation", "chart", "comparison", "trend", "unsupported_missing_column"}:
        if strong_file_data_signal:
            return "tabular_analytics"
        if selected_route in {"aggregation", "unsupported_missing_column"} and requested_fields:
            return "tabular_analytics"
        return "general_chat"

    legacy_tabular_intent = detect_tabular_intent(text)
    if strong_file_data_signal or requested_fields:
        if legacy_tabular_intent == "lookup":
            return "file_lookup"
        if legacy_tabular_intent in {"aggregate", "profile"}:
            return "tabular_analytics"

    if _EXPLICIT_FILE_REQUEST_RE.search(lowered):
        return "file_question"
    if _EXPLICIT_DATA_CONTEXT_RE.search(lowered):
        return "file_question"

    return "general_chat"

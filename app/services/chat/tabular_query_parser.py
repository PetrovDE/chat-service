from __future__ import annotations

from dataclasses import dataclass
import re
from typing import List, Optional, Sequence, Tuple

from app.services.chat.tabular_temporal_planner import (
    detect_requested_time_grain,
    extract_datetime_source_hint,
    has_temporal_grouping_signal,
)


_CYR_COLS = "\u043a\u0430\u043a\u0438\u0435 \u043a\u043e\u043b\u043e\u043d\u043a\u0438"
_CYR_COLUMNS_2 = "\u043a\u0430\u043a\u0438\u0435 \u0441\u0442\u043e\u043b\u0431\u0446\u044b"
_CYR_FIELDS = "\u043a\u0430\u043a\u0438\u0435 \u043f\u043e\u043b\u044f"
_CYR_COL_LIST = "\u0441\u043f\u0438\u0441\u043e\u043a \u043a\u043e\u043b\u043e\u043d\u043e\u043a"
_CYR_FIELD_LIST = "\u0441\u043f\u0438\u0441\u043e\u043a \u043f\u043e\u043b\u0435\u0439"
_CYR_OVERVIEW = "\u043e\u0431\u0437\u043e\u0440"
_CYR_FULL_ANALYSIS = "\u043f\u043e\u043b\u043d\u044b\u0439 \u0430\u043d\u0430\u043b\u0438\u0437"
_CYR_GENERAL_ANALYSIS = "\u043e\u0431\u0449\u0438\u0439 \u0430\u043d\u0430\u043b\u0438\u0437"
_CYR_SHOW_STATS = "\u043f\u043e\u043a\u0430\u0436\u0438 \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0438"
_CYR_SHOW_METRICS = "\u043f\u043e\u043a\u0430\u0436\u0438 \u043c\u0435\u0442\u0440\u0438\u043a\u0438"
_CYR_CHART = "\u0433\u0440\u0430\u0444\u0438\u043a"
_CYR_DIAGRAM = "\u0434\u0438\u0430\u0433\u0440\u0430\u043c"
_CYR_VISUAL = "\u0432\u0438\u0437\u0443\u0430\u043b"
_CYR_DISTR = "\u0440\u0430\u0441\u043f\u0440\u0435\u0434\u0435\u043b"
_CYR_TREND = "\u0442\u0440\u0435\u043d\u0434"
_CYR_DYNAMICS = "\u0434\u0438\u043d\u0430\u043c"
_CYR_BY_TIME = "\u043f\u043e \u0432\u0440\u0435\u043c\u0435\u043d\u0438"
_CYR_MONTHLY = "\u043f\u043e\u043c\u0435\u0441\u044f\u0447"
_CYR_BY_MONTH = "\u043f\u043e \u043c\u0435\u0441\u044f\u0446"
_CYR_COMPARE = "\u0441\u0440\u0430\u0432\u043d\u0438"
_CYR_COMPARISON = "\u0441\u0440\u0430\u0432\u043d\u0435\u043d\u0438\u0435"
_CYR_MATCH = "\u0441\u043e\u043f\u043e\u0441\u0442\u0430\u0432"
_CYR_WHERE = "\u0433\u0434\u0435"
_CYR_FILTER = "\u0444\u0438\u043b\u044c\u0442\u0440"
_CYR_FIND_ROWS = "\u043d\u0430\u0439\u0434\u0438 \u0441\u0442\u0440\u043e\u043a\u0438"
_CYR_SHOW_ROWS = "\u043f\u043e\u043a\u0430\u0436\u0438 \u0441\u0442\u0440\u043e\u043a\u0438"
_CYR_SHOW_RECORDS = "\u043f\u043e\u043a\u0430\u0436\u0438 \u0437\u0430\u043f\u0438\u0441\u0438"
_CYR_COUNT = "\u0441\u043a\u043e\u043b\u044c\u043a\u043e"
_CYR_COUNT_2 = "\u043a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e"
_CYR_COUNT_3 = "\u0447\u0438\u0441\u043b\u043e"
_CYR_SUM = "\u0441\u0443\u043c\u043c"
_CYR_TOTAL = "\u0438\u0442\u043e\u0433\u043e"
_CYR_AVG = "\u0441\u0440\u0435\u0434\u043d"
_CYR_MIN = "\u043c\u0438\u043d\u0438\u043c"
_CYR_MAX = "\u043c\u0430\u043a\u0441\u0438\u043c"
_CYR_BY = "\u043f\u043e"
_CYR_DATE = "\u0434\u0430\u0442\u0430"
_CYR_TIME = "\u0432\u0440\u0435\u043c\u044f"
_CYR_MONTH = "\u043c\u0435\u0441\u044f\u0446"
_CYR_MONTHS = "\u043c\u0435\u0441\u044f\u0446\u044b"
_CYR_DAY = "\u0434\u0435\u043d\u044c"
_CYR_DAYS = "\u0434\u043d\u0438"
_CYR_YEAR = "\u0433\u043e\u0434"
_CYR_YEARS = "\u0433\u043e\u0434\u044b"

_TEXT_PATTERN = r"[a-z\u0430-\u044f\u04510-9_ ]"
_VALUE_PATTERN = r"[a-z\u0430-\u044f\u04510-9_./:\\-]"
_NORM_RE = re.compile(r"[^a-z\u0430-\u044f\u04510-9]+")


SCHEMA_HINTS: Tuple[str, ...] = (
    "schema",
    "columns",
    "column names",
    "list columns",
    "what columns",
    "which fields are important",
    "what is in this file",
    "tell me about this file",
    "what can i analyze here",
    "which sheets",
    "what sheets",
    "sheets available",
    "which tables",
    "what tables",
    "tables available",
    _CYR_COLS,
    _CYR_COLUMNS_2,
    _CYR_FIELDS,
    _CYR_COL_LIST,
    _CYR_FIELD_LIST,
)

OVERVIEW_HINTS: Tuple[str, ...] = (
    "overview",
    "summary",
    "summarize table",
    "full analysis",
    "analyze dataset",
    "column statistics",
    "per column",
    "dataset profile",
    "table summary",
    "dataset summary",
    _CYR_OVERVIEW,
    _CYR_GENERAL_ANALYSIS,
    _CYR_FULL_ANALYSIS,
    _CYR_SHOW_STATS,
    _CYR_SHOW_METRICS,
)

CHART_HINTS: Tuple[str, ...] = (
    "chart",
    "graph",
    "plot",
    "histogram",
    "distribution",
    _CYR_DIAGRAM,
    _CYR_CHART,
    _CYR_VISUAL,
    _CYR_DISTR,
)

TREND_HINTS: Tuple[str, ...] = (
    "trend",
    "time series",
    "timeseries",
    "over time",
    _CYR_DYNAMICS,
    _CYR_TREND,
    _CYR_BY_TIME,
    _CYR_MONTHLY,
    _CYR_BY_MONTH,
)

COMPARISON_HINTS: Tuple[str, ...] = (
    "compare",
    "comparison",
    _CYR_COMPARE,
    _CYR_COMPARISON,
    _CYR_MATCH,
)

FILTERING_HINTS: Tuple[str, ...] = (
    "where",
    "filter",
    "lookup",
    "find rows",
    "show rows",
    "show records",
    f"{_CYR_WHERE} ",
    _CYR_FILTER,
    _CYR_FIND_ROWS,
    _CYR_SHOW_ROWS,
    _CYR_SHOW_RECORDS,
)

COUNT_HINTS: Tuple[str, ...] = ("count", "how many", _CYR_COUNT, _CYR_COUNT_2, _CYR_COUNT_3)
SUM_HINTS: Tuple[str, ...] = (
    "sum",
    "total",
    "volume",
    "spend",
    "spending",
    "expense",
    "expenses",
    "revenue",
    "sales",
    "\u043e\u0431\u044a\u0435\u043c",
    "\u043e\u0431\u044a\u0451\u043c",
    "\u0437\u0430\u0442\u0440\u0430\u0442",
    "\u0440\u0430\u0441\u0445\u043e\u0434",
    "\u0442\u0440\u0430\u0442",
    "\u0441\u0442\u043e\u0438\u043c",
    _CYR_SUM,
    _CYR_TOTAL,
)
AVG_HINTS: Tuple[str, ...] = ("avg", "average", "mean", _CYR_AVG)
MIN_HINTS: Tuple[str, ...] = ("min", _CYR_MIN)
MAX_HINTS: Tuple[str, ...] = ("max", _CYR_MAX)

GROUP_BY_HINTS: Tuple[str, ...] = ("group by", "by ", f"{_CYR_BY} ")

GENERIC_FIELD_STOP_WORDS = {
    "month",
    "months",
    "date",
    "time",
    "day",
    "days",
    "year",
    "years",
    _CYR_MONTH,
    _CYR_MONTHS,
    _CYR_DATE,
    _CYR_TIME,
    _CYR_DAY,
    _CYR_DAYS,
    _CYR_YEAR,
    _CYR_YEARS,
}


@dataclass(frozen=True)
class ParsedTabularQuery:
    route: str
    legacy_intent: Optional[str]
    operation: Optional[str]
    requested_field_text: Optional[str]
    group_by_field_text: Optional[str]
    lookup_field_text: Optional[str]
    lookup_value_text: Optional[str]
    requested_fields: List[str]
    requested_time_grain: Optional[str]
    source_datetime_field_hint: Optional[str]


def normalize_text(text: str) -> str:
    return _NORM_RE.sub(" ", (text or "").lower()).strip()


def _dedupe(items: Sequence[Optional[str]]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in items:
        value = str(raw or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def detect_tabular_route(query: str) -> str:
    q = normalize_text(query)
    if not q:
        return "unknown"
    if any(h in q for h in SCHEMA_HINTS):
        return "schema_question"
    if any(h in q for h in OVERVIEW_HINTS):
        return "overview"
    if any(h in q for h in CHART_HINTS):
        return "chart"
    if has_temporal_grouping_signal(q):
        return "trend"
    if any(h in q for h in TREND_HINTS):
        return "trend"
    if any(h in q for h in COMPARISON_HINTS):
        return "comparison"
    if any(h in q for h in FILTERING_HINTS):
        return "filtering"
    if any(h in q for h in (COUNT_HINTS + SUM_HINTS + AVG_HINTS + MIN_HINTS + MAX_HINTS)):
        return "aggregation"
    return "unknown"


def detect_legacy_tabular_intent(query: str) -> Optional[str]:
    route = detect_tabular_route(query)
    if route in {"schema_question", "overview"}:
        return "profile"
    if route == "filtering":
        return "lookup"
    if route in {"aggregation", "chart", "trend", "comparison"}:
        return "aggregate"
    return None


def detect_operation(query: str) -> Optional[str]:
    q = normalize_text(query)
    if not q:
        return None
    if any(h in q for h in SUM_HINTS):
        return "sum"
    if any(h in q for h in AVG_HINTS):
        return "avg"
    if any(h in q for h in MIN_HINTS):
        return "min"
    if any(h in q for h in MAX_HINTS):
        return "max"
    if any(h in q for h in COUNT_HINTS):
        return "count"
    return None


def _truncate_clause(candidate: str) -> str:
    text = normalize_text(candidate)
    if not text:
        return ""
    text = re.split(
        r"\b(and|with|where|when|for|from|in|on|\u0438|\u0441|\u0434\u043b\u044f|\u0433\u0434\u0435|\u043a\u043e\u0433\u0434\u0430|\u043f\u043e)\b",
        text,
        maxsplit=1,
    )[0]
    return normalize_text(text)


def _extract_field_after_preposition(query: str) -> Optional[str]:
    q = normalize_text(query)
    if not q:
        return None
    patterns = (
        rf"\bby\s+({_TEXT_PATTERN}{{2,120}})",
        rf"\b{_CYR_BY}\s+({_TEXT_PATTERN}{{2,120}})",
        rf"\bfor\s+({_TEXT_PATTERN}{{2,120}})",
    )
    for pattern in patterns:
        match = re.search(pattern, q)
        if not match:
            continue
        candidate = _truncate_clause(str(match.group(1) or ""))
        if not candidate or candidate in GENERIC_FIELD_STOP_WORDS:
            continue
        return candidate
    return None


def _extract_operation_field(query: str, operation: Optional[str]) -> Optional[str]:
    if operation not in {"sum", "avg", "min", "max"}:
        return None
    q = normalize_text(query)
    if not q:
        return None
    operation_patterns = {
        "sum": rf"\b(sum|total|{_CYR_SUM}{_TEXT_PATTERN}*)\s+(of\s+)?({_TEXT_PATTERN}{{2,120}})",
        "avg": rf"\b(avg|average|mean|{_CYR_AVG}{_TEXT_PATTERN}*)\s+(of\s+)?({_TEXT_PATTERN}{{2,120}})",
        "min": rf"\b(min|{_CYR_MIN}{_TEXT_PATTERN}*)\s+(of\s+)?({_TEXT_PATTERN}{{2,120}})",
        "max": rf"\b(max|{_CYR_MAX}{_TEXT_PATTERN}*)\s+(of\s+)?({_TEXT_PATTERN}{{2,120}})",
    }
    pattern = operation_patterns[operation]
    match = re.search(pattern, q)
    if not match:
        return None
    candidate = _truncate_clause(str(match.group(3) or ""))
    if not candidate:
        return None
    return candidate


def _extract_group_by_field(query: str) -> Optional[str]:
    q = normalize_text(query)
    if not q:
        return None
    patterns = (
        rf"\bgroup by\s+({_TEXT_PATTERN}{{2,120}})",
        rf"\bby\s+({_TEXT_PATTERN}{{2,120}})",
        rf"\b{_CYR_BY}\s+({_TEXT_PATTERN}{{2,120}})",
    )
    for pattern in patterns:
        match = re.search(pattern, q)
        if not match:
            continue
        candidate = _truncate_clause(str(match.group(1) or ""))
        if not candidate or candidate in GENERIC_FIELD_STOP_WORDS:
            continue
        return candidate
    return None


def _extract_metric_before_grouping(query: str) -> Optional[str]:
    q = normalize_text(query)
    if not q:
        return None
    patterns = (
        rf"\bof\s+({_TEXT_PATTERN}{{2,120}})\s+\b(group by|by|per)\b",
        rf"\bshow\s+({_TEXT_PATTERN}{{2,120}})\s+\b(group by|by|per)\b",
        rf"\bbuild\s+({_TEXT_PATTERN}{{2,120}})\s+\b(group by|by|per)\b",
        rf"\bplot\s+({_TEXT_PATTERN}{{2,120}})\s+\b(group by|by|per)\b",
        rf"\bgraph\s+({_TEXT_PATTERN}{{2,120}})\s+\b(group by|by|per)\b",
        rf"\bchart\s+({_TEXT_PATTERN}{{2,120}})\s+\b(group by|by|per)\b",
        rf"\b({_TEXT_PATTERN}{{2,120}})\s+\b(group by|by|per)\b",
    )
    for pattern in patterns:
        match = re.search(pattern, q)
        if not match:
            continue
        candidate = _truncate_clause(str(match.group(1) or ""))
        candidate = re.sub(r"\b(show|build|plot|graph|chart|a|an|the|distribution|trend|group)\b", " ", candidate)
        candidate = normalize_text(candidate)
        if not candidate or candidate in GENERIC_FIELD_STOP_WORDS:
            continue
        return candidate
    return None


def _extract_lookup_components(query: str) -> Tuple[Optional[str], Optional[str]]:
    q = str(query or "").strip()
    if not q:
        return None, None

    where_patterns = (
        rf"(?:where|{_CYR_WHERE})\s+({_TEXT_PATTERN}{{1,120}})\s*(?:=|\u0440\u0430\u0432\u043d\u043e|is|equals)\s*(\"[^\"]{{1,200}}\"|'[^']{{1,200}}'|{_VALUE_PATTERN}{{1,120}})",
    )
    for pattern in where_patterns:
        match = re.search(pattern, q, flags=re.IGNORECASE)
        if not match:
            continue
        field = normalize_text(str(match.group(1) or ""))
        value = str(match.group(2) or "").strip().strip("\"'")
        return (field or None), (value or None)

    for regex in (r'"([^"]{1,200})"', r"'([^']{1,200})'"):
        match = re.search(regex, q)
        if match:
            return None, str(match.group(1) or "").strip()
    return None, None


def parse_tabular_query(query: str) -> ParsedTabularQuery:
    route = detect_tabular_route(query)
    legacy_intent = detect_legacy_tabular_intent(query)
    operation = detect_operation(query)
    requested_time_grain = detect_requested_time_grain(query)
    source_datetime_field_hint = extract_datetime_source_hint(query) if requested_time_grain else None
    group_by_field_text = _extract_group_by_field(query)
    lookup_field_text, lookup_value_text = _extract_lookup_components(query)

    requested_field_text: Optional[str] = None
    if route in {"chart", "trend", "comparison"}:
        if requested_time_grain:
            requested_field_text = _extract_metric_before_grouping(query) or _extract_operation_field(query, operation)
        else:
            requested_field_text = _extract_field_after_preposition(query)
    elif route == "aggregation":
        requested_field_text = _extract_operation_field(query, operation)
    elif route == "filtering":
        requested_field_text = lookup_field_text

    requested_fields = _dedupe([requested_field_text, group_by_field_text, lookup_field_text])
    return ParsedTabularQuery(
        route=route,
        legacy_intent=legacy_intent,
        operation=operation,
        requested_field_text=requested_field_text,
        group_by_field_text=group_by_field_text,
        lookup_field_text=lookup_field_text,
        lookup_value_text=lookup_value_text,
        requested_fields=requested_fields,
        requested_time_grain=requested_time_grain,
        source_datetime_field_hint=source_datetime_field_hint,
    )

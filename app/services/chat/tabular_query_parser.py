from __future__ import annotations

from dataclasses import dataclass
import re
from typing import List, Optional, Sequence, Tuple


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
    "какие колонки",
    "какие столбцы",
    "какие поля",
    "список колонок",
    "список полей",
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
    "обзор",
    "общий анализ",
    "полный анализ",
    "покажи статистики",
    "покажи метрики",
)

CHART_HINTS: Tuple[str, ...] = (
    "chart",
    "graph",
    "plot",
    "histogram",
    "distribution",
    "диаграм",
    "график",
    "график",
    "визуал",
    "распредел",
)

TREND_HINTS: Tuple[str, ...] = (
    "trend",
    "time series",
    "timeseries",
    "over time",
    "динам",
    "тренд",
    "по времени",
    "помесяч",
    "по месяц",
)

COMPARISON_HINTS: Tuple[str, ...] = (
    "compare",
    "comparison",
    "сравни",
    "сравнение",
    "сопостав",
)

FILTERING_HINTS: Tuple[str, ...] = (
    "where",
    "filter",
    "lookup",
    "find rows",
    "show rows",
    "show records",
    "где ",
    "фильтр",
    "найди строки",
    "покажи строки",
    "покажи записи",
)

COUNT_HINTS: Tuple[str, ...] = ("count", "how many", "сколько", "количество", "число")
SUM_HINTS: Tuple[str, ...] = ("sum", "total", "сумм", "итого")
AVG_HINTS: Tuple[str, ...] = ("avg", "average", "mean", "средн")
MIN_HINTS: Tuple[str, ...] = ("min", "миним")
MAX_HINTS: Tuple[str, ...] = ("max", "максим")

GROUP_BY_HINTS: Tuple[str, ...] = ("group by", "by ", "по ")

GENERIC_FIELD_STOP_WORDS = {
    "month",
    "months",
    "date",
    "time",
    "day",
    "days",
    "year",
    "years",
    "месяц",
    "месяцы",
    "дата",
    "время",
    "день",
    "дни",
    "год",
    "годы",
}

_NORM_RE = re.compile(r"[^a-zа-яё0-9]+")


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
        r"\b(and|with|where|when|for|from|in|on|и|с|для|где|когда|по)\b",
        text,
        maxsplit=1,
    )[0]
    return normalize_text(text)


def _extract_field_after_preposition(query: str) -> Optional[str]:
    q = normalize_text(query)
    if not q:
        return None
    patterns = (
        r"\bby\s+([a-zа-яё0-9_ ]{2,120})",
        r"\bпо\s+([a-zа-яё0-9_ ]{2,120})",
        r"\bfor\s+([a-zа-яё0-9_ ]{2,120})",
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
        "sum": r"\b(sum|total|сумм[a-zа-яё]*)\s+(of\s+)?([a-zа-яё0-9_ ]{2,120})",
        "avg": r"\b(avg|average|mean|средн[a-zа-яё]*)\s+(of\s+)?([a-zа-яё0-9_ ]{2,120})",
        "min": r"\b(min|миним[a-zа-яё]*)\s+(of\s+)?([a-zа-яё0-9_ ]{2,120})",
        "max": r"\b(max|максим[a-zа-яё]*)\s+(of\s+)?([a-zа-яё0-9_ ]{2,120})",
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
        r"\bgroup by\s+([a-zа-яё0-9_ ]{2,120})",
        r"\bby\s+([a-zа-яё0-9_ ]{2,120})",
        r"\bпо\s+([a-zа-яё0-9_ ]{2,120})",
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


def _extract_lookup_components(query: str) -> Tuple[Optional[str], Optional[str]]:
    q = str(query or "").strip()
    if not q:
        return None, None

    where_patterns = (
        r"(?:where|где)\s+([a-zа-яё0-9_ ]{1,120})\s*(?:=|равно|is|equals)\s*(\"[^\"]{1,200}\"|'[^']{1,200}'|[a-zа-яё0-9_./:\-]{1,120})",
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
    group_by_field_text = _extract_group_by_field(query)
    lookup_field_text, lookup_value_text = _extract_lookup_components(query)

    requested_field_text: Optional[str] = None
    if route in {"chart", "trend", "comparison"}:
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
    )

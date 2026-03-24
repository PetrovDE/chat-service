from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


SCHEMA_HINTS = (
    "какие колонки",
    "какие столбцы",
    "какие поля",
    "список колонок",
    "список полей",
    "schema",
    "columns",
    "column names",
    "list columns",
    "what columns",
)

OVERVIEW_HINTS = (
    "обзор",
    "overview",
    "summary",
    "summarize table",
    "общий анализ",
    "полный анализ",
    "full analysis",
    "analyze dataset",
    "column statistics",
    "per column",
    "dataset profile",
    "table summary",
    "dataset summary",
)

CHART_HINTS = (
    "график",
    "диаграм",
    "визуал",
    "chart",
    "plot",
    "histogram",
    "distribution",
    "распредел",
)

TREND_HINTS = (
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

COMPARISON_HINTS = (
    "compare",
    "comparison",
    "сравни",
    "сравнение",
    "сопостав",
)

FILTERING_HINTS = (
    "where",
    "filter",
    "lookup",
    "show rows",
    "show records",
    "find rows",
    "где ",
    "фильтр",
    "найди строки",
    "покажи строки",
    "покажи записи",
)

AGGREGATION_HINTS = (
    "how many",
    "сколько",
    "count",
    "количество",
    "число",
    "rows",
    "sum",
    "total",
    "avg",
    "average",
    "mean",
    "min",
    "max",
    "сумм",
    "итого",
    "средн",
    "миним",
    "максим",
)

SEMANTIC_FIELD_ALIASES: Dict[str, Tuple[str, ...]] = {
    "birth_date": (
        "birth_date",
        "date_of_birth",
        "dob",
        "birthday",
        "month_of_birth",
        "birth month",
        "month birth",
        "дата рождения",
        "месяц рождения",
        "месяцам рождения",
        "день рождения",
    ),
}

DATETIME_COLUMN_HINTS = (
    "date",
    "time",
    "created",
    "updated",
    "month",
    "year",
    "дата",
    "время",
    "месяц",
    "год",
)

CATEGORICAL_COLUMN_HINTS = (
    "status",
    "city",
    "product",
    "client",
    "priority",
    "category",
    "segment",
    "статус",
    "город",
    "продукт",
    "клиент",
    "приоритет",
    "катег",
    "сегмент",
)


@dataclass(frozen=True)
class TabularIntentDecision:
    detected_intent: str
    selected_route: str
    legacy_intent: Optional[str]
    requested_fields: List[str]
    matched_columns: List[str]
    unmatched_requested_fields: List[str]
    fallback_reason: str = "none"

    @property
    def is_supported(self) -> bool:
        return self.selected_route != "unsupported_missing_column"


def _norm(text: str) -> str:
    return re.sub(r"[^a-zа-яё0-9]+", " ", (text or "").lower()).strip()


def _dedupe(items: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        value = str(item or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _resolve_route(query: str) -> str:
    q = _norm(query)
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
    if any(h in q for h in AGGREGATION_HINTS):
        return "aggregation"
    return "unknown"


def _column_aliases(table: Any) -> Dict[str, str]:
    aliases = getattr(table, "column_aliases", None)
    if isinstance(aliases, dict):
        return {str(k): str(v) for k, v in aliases.items()}
    return {}


def _extract_direct_column_mentions(query: str, table: Any) -> List[str]:
    q_norm = _norm(query)
    if not q_norm:
        return []
    matches: List[str] = []
    aliases = _column_aliases(table)
    for column in list(getattr(table, "columns", []) or []):
        col_norm = _norm(str(column))
        alias_norm = _norm(str(aliases.get(column) or ""))
        if (col_norm and col_norm in q_norm) or (alias_norm and alias_norm in q_norm):
            matches.append(str(column))
    return _dedupe(matches)


def _extract_requested_semantic_fields(query: str) -> List[str]:
    q = _norm(query)
    requested: List[str] = []
    if not q:
        return requested
    for semantic_field, aliases in SEMANTIC_FIELD_ALIASES.items():
        if any(_norm(alias) in q for alias in aliases):
            requested.append(semantic_field)
    return _dedupe(requested)


def _semantic_field_matches(field_name: str, table: Any) -> List[str]:
    aliases = _column_aliases(table)
    all_candidates: List[Tuple[str, str]] = []
    for column in list(getattr(table, "columns", []) or []):
        column_name = str(column)
        all_candidates.append((column_name, _norm(column_name)))
        alias_name = str(aliases.get(column_name) or "")
        if alias_name:
            all_candidates.append((column_name, _norm(alias_name)))

    matched: List[str] = []
    alias_list = SEMANTIC_FIELD_ALIASES.get(field_name, ())
    normalized_aliases = {_norm(alias) for alias in alias_list if _norm(alias)}
    if _norm(field_name):
        normalized_aliases.add(_norm(field_name))

    for column_name, candidate_norm in all_candidates:
        if not candidate_norm:
            continue
        if candidate_norm in normalized_aliases:
            matched.append(column_name)
            continue
        if field_name == "birth_date":
            if (
                ("birth" in candidate_norm and ("date" in candidate_norm or "day" in candidate_norm or "dob" in candidate_norm))
                or ("рожд" in candidate_norm and ("дат" in candidate_norm or "день" in candidate_norm or "месяц" in candidate_norm))
            ):
                matched.append(column_name)
    return _dedupe(matched)


def _legacy_intent_for_route(route: str) -> Optional[str]:
    if route in {"schema_question", "overview"}:
        return "profile"
    if route in {"aggregation", "chart", "comparison", "trend", "unsupported_missing_column"}:
        return "aggregate"
    if route == "filtering":
        return "lookup"
    return None


def classify_tabular_query(
    *,
    query: str,
    table: Optional[Any],
) -> TabularIntentDecision:
    route = _resolve_route(query)
    requested_fields = _extract_requested_semantic_fields(query)
    direct_columns: List[str] = []
    matched_columns: List[str] = []
    unmatched: List[str] = []
    fallback_reason = "none"

    if table is not None:
        direct_columns = _extract_direct_column_mentions(query, table)
        matched_columns.extend(direct_columns)
        for field_name in requested_fields:
            semantic_matches = _semantic_field_matches(field_name, table)
            if semantic_matches:
                matched_columns.extend(semantic_matches)
            else:
                unmatched.append(field_name)
        matched_columns = _dedupe(matched_columns)
        unmatched = _dedupe(unmatched)

    if route in {"chart", "comparison", "trend"} and unmatched:
        fallback_reason = "missing_required_columns"
        route = "unsupported_missing_column"

    return TabularIntentDecision(
        detected_intent=route,
        selected_route=route,
        legacy_intent=_legacy_intent_for_route(route),
        requested_fields=requested_fields,
        matched_columns=matched_columns,
        unmatched_requested_fields=unmatched,
        fallback_reason=fallback_reason,
    )


def detect_legacy_tabular_intent(query: str) -> Optional[str]:
    decision = classify_tabular_query(query=query, table=None)
    return decision.legacy_intent


def suggest_relevant_alternative_columns(table: Any, *, limit: int = 6) -> List[str]:
    columns = [str(col) for col in (list(getattr(table, "columns", []) or []))]
    if not columns:
        return []
    aliases = _column_aliases(table)

    def score(column: str) -> Tuple[int, int]:
        joined = " ".join([_norm(column), _norm(aliases.get(column, ""))]).strip()
        s = 0
        if any(token in joined for token in DATETIME_COLUMN_HINTS):
            s += 30
        if any(token in joined for token in CATEGORICAL_COLUMN_HINTS):
            s += 20
        if "id" in joined:
            s -= 5
        return (-s, len(column))

    ranked = sorted(columns, key=score)
    return _dedupe(ranked[: max(1, int(limit))])

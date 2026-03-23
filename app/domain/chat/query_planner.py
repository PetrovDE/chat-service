from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Dict, List, Optional, Sequence

from app.observability.metrics import inc_counter
from app.observability.slo_metrics import observe_planner_decision
from app.services.chat.complex_analytics import is_complex_analytics_query
from app.services.tabular.sql_execution import resolve_tabular_dataset

ROUTE_DETERMINISTIC_ANALYTICS = "deterministic_analytics"
ROUTE_COMPLEX_ANALYTICS = "complex_analytics"
ROUTE_NARRATIVE_RETRIEVAL = "narrative_retrieval"

INTENT_TABULAR_AGGREGATE = "tabular_aggregate"
INTENT_TABULAR_PROFILE = "tabular_profile"
INTENT_TABULAR_LOOKUP = "tabular_lookup"
INTENT_TABULAR_COMBINED = "tabular_combined"
INTENT_COMPLEX_ANALYTICS = "complex_analytics"
INTENT_NARRATIVE_RETRIEVAL = "narrative_retrieval"
INTENT_METRIC_CLARIFICATION = "metric_clarification"

_COUNT_HINTS = ("\u0441\u043a\u043e\u043b\u044c\u043a\u043e", "count", "\u043a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u043e", "\u0447\u0438\u0441\u043b\u043e")
_SUM_HINTS = ("\u0441\u0443\u043c\u043c", "\u0438\u0442\u043e\u0433\u043e", "sum", "total")
_AVG_HINTS = ("\u0441\u0440\u0435\u0434\u043d", "avg", "average", "mean")
_MIN_HINTS = ("\u043c\u0438\u043d\u0438\u043c", "min")
_MAX_HINTS = ("\u043c\u0430\u043a\u0441\u0438\u043c", "max")
_AGGREGATE_SCOPE_HINTS = (
    "\u0432\u0441\u0435\u0433\u043e \u0441\u0442\u0440\u043e\u043a",
    "\u0432\u0441\u0435\u0433\u043e \u0437\u0430\u043f\u0438\u0441\u0435\u0439",
    "\u0432\u0441\u0435 \u0441\u0442\u0440\u043e\u043a\u0438",
    "\u043f\u043e \u0432\u0441\u0435\u043c \u0441\u0442\u0440\u043e\u043a\u0430\u043c",
    "\u0432\u0441\u0435 \u0437\u0430\u043f\u0438\u0441\u0438",
    "\u0432\u0435\u0441\u044c \u0444\u0430\u0439\u043b",
    "\u0432\u0441\u044f \u0442\u0430\u0431\u043b\u0438\u0446\u0430",
    "all rows",
    "entire file",
    "whole file",
)
_PROFILE_HINTS = (
    "\u043f\u043e \u043a\u0430\u0436\u0434\u043e\u0439 \u043a\u043e\u043b\u043e\u043d",
    "\u043a\u0430\u0436\u0434\u043e\u0439 \u043a\u043e\u043b\u043e\u043d",
    "\u0432\u0441\u0435 \u043a\u043e\u043b\u043e\u043d\u043a\u0438",
    "\u0432\u0441\u0435\u0445 \u043a\u043e\u043b\u043e\u043d",
    "\u043e\u0431\u0449\u0438\u0439 \u0430\u043d\u0430\u043b\u0438\u0437",
    "\u043f\u043e\u043b\u043d\u044b\u0439 \u0430\u043d\u0430\u043b\u0438\u0437",
    "\u043a\u0430\u043a\u0438\u0435 \u0434\u0430\u043d\u043d\u044b\u0435",
    "\u0447\u0442\u043e \u0442\u044b \u043c\u043e\u0436\u0435\u0448\u044c \u0441\u043a\u0430\u0437\u0430\u0442\u044c",
    "\u043f\u043e\u043a\u0430\u0436\u0438 \u0441\u0442\u0430\u0442\u0438\u0441\u0442\u0438\u043a\u0438",
    "\u043f\u043e\u043a\u0430\u0436\u0438 \u043c\u0435\u0442\u0440\u0438\u043a\u0438",
    "column statistics",
    "per column",
    "full analysis",
    "analyze dataset",
)
_LOOKUP_HINTS = (
    "find rows",
    "show rows",
    "show records",
    "lookup",
    "filter",
    "where",
    "найди строки",
    "покажи строки",
    "покажи записи",
    "выведи строки",
    "фильтр",
    "где ",
)
_SEMANTIC_RETRIEVAL_HINTS = (
    "where is",
    "which sheet",
    "на каком листе",
    "в каком листе",
    "где находится",
    "покажи релевантные строки",
    "show relevant rows",
    "find relevant rows",
    "есть ли колонка",
    "which column",
)
_METRIC_KEYWORDS = (
    "\u043c\u0435\u0442\u0440\u0438\u043a",
    "\u043f\u0440\u043e\u0446\u0435\u043d\u0442",
    "\u0434\u043e\u043b\u044f",
    "\u0440\u043e\u0441\u0442",
    "metric",
    "percentage",
    "ratio",
    "growth",
    "trend",
)


@dataclass(frozen=True)
class QueryPlanDecision:
    route: str
    intent: str
    strategy_mode: str
    confidence: float
    requires_clarification: bool
    reason_codes: List[str]
    metric_critical: bool = False
    clarification_prompt: Optional[str] = None

    def as_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "route": self.route,
            "intent": self.intent,
            "strategy_mode": self.strategy_mode,
            "confidence": float(self.confidence),
            "requires_clarification": bool(self.requires_clarification),
            "reason_codes": list(self.reason_codes),
        }
        if self.metric_critical:
            payload["metric_critical"] = True
        if self.clarification_prompt:
            payload["clarification_prompt"] = self.clarification_prompt
        return payload


def _norm(text: str) -> str:
    return re.sub(r"[^a-z\u0430-\u044f\u04510-9]+", " ", (text or "").lower()).strip()


def _is_tabular_file(file_obj: Any) -> bool:
    extension = str(
        getattr(file_obj, "extension", "")
        or getattr(file_obj, "file_type", "")
        or ""
    ).lower().lstrip(".")
    if extension:
        return extension in {"xlsx", "xls", "csv", "tsv"}
    original_filename = str(getattr(file_obj, "original_filename", "") or "").lower().strip()
    if "." in original_filename:
        return original_filename.rsplit(".", 1)[-1] in {"xlsx", "xls", "csv", "tsv"}
    return False


def detect_tabular_intent(query: str) -> Optional[str]:
    q = (query or "").strip().lower()
    if not q:
        return None
    if any(hint in q for hint in _PROFILE_HINTS):
        return "profile"
    if any(hint in q for hint in (_COUNT_HINTS + _SUM_HINTS + _AVG_HINTS + _MIN_HINTS + _MAX_HINTS + _AGGREGATE_SCOPE_HINTS)):
        return "aggregate"
    if any(hint in q for hint in _LOOKUP_HINTS):
        return "lookup"
    return None


def is_metric_critical_query(query: str) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return False
    if any(hint in q for hint in (_COUNT_HINTS + _SUM_HINTS + _AVG_HINTS + _MIN_HINTS + _MAX_HINTS)):
        return True
    return any(hint in q for hint in _METRIC_KEYWORDS)


def _detect_operation(query: str) -> Optional[str]:
    q = (query or "").strip().lower()
    if not q:
        return None
    if any(hint in q for hint in _SUM_HINTS):
        return "sum"
    if any(hint in q for hint in _AVG_HINTS):
        return "avg"
    if any(hint in q for hint in _MIN_HINTS):
        return "min"
    if any(hint in q for hint in _MAX_HINTS):
        return "max"
    if any(hint in q for hint in _COUNT_HINTS):
        return "count"
    return None


def _collect_column_matches(query: str, files: Sequence[Any]) -> List[str]:
    q_norm = _norm(query)
    if not q_norm:
        return []
    matches: List[str] = []
    seen = set()
    for file_obj in files:
        dataset = resolve_tabular_dataset(file_obj)
        if dataset is None:
            continue
        for table in dataset.tables:
            for column in table.columns:
                column_norm = _norm(column)
                alias_norm = _norm(table.column_aliases.get(column, ""))
                if (column_norm and column_norm in q_norm) or (alias_norm and alias_norm in q_norm):
                    if column not in seen:
                        seen.add(column)
                        matches.append(column)
    return matches


def _has_semantic_tabular_hints(query: str, files: Sequence[Any]) -> bool:
    q = (query or "").strip().lower()
    if not q:
        return False
    if any(hint in q for hint in _SEMANTIC_RETRIEVAL_HINTS):
        return True
    for file_obj in files:
        dataset = resolve_tabular_dataset(file_obj)
        if dataset is None:
            continue
        for table in dataset.tables:
            sheet_name = str(table.sheet_name or "").strip().lower()
            if sheet_name and sheet_name in q:
                return True
    return False


def _build_clarification_prompt(query: str) -> str:
    if re.search(r"[\u0400-\u04FF]", query or ""):
        return (
            "Уточните, пожалуйста, метрику и срез. "
            "Напишите, какую колонку/показатель считать и нужен ли фильтр или группировка."
        )
    return (
        "Please clarify the metric and scope. "
        "Specify which column/measure to calculate and whether a filter or grouping is required."
    )


def _observe_decision(decision: QueryPlanDecision) -> None:
    inc_counter(
        "query_planner_decision_total",
        route=decision.route,
        intent=decision.intent,
        requires_clarification=str(bool(decision.requires_clarification)).lower(),
    )
    observe_planner_decision(
        route=decision.route,
        intent=decision.intent,
        requires_clarification=bool(decision.requires_clarification),
        metric_critical=bool(decision.metric_critical),
    )
    for reason_code in decision.reason_codes:
        inc_counter("query_planner_reason_total", reason=reason_code)


def plan_query(
    *,
    query: str,
    files: Optional[Sequence[Any]],
) -> QueryPlanDecision:
    reason_codes: List[str] = []
    trimmed_query = (query or "").strip()
    if not trimmed_query:
        decision = QueryPlanDecision(
            route=ROUTE_NARRATIVE_RETRIEVAL,
            intent=INTENT_NARRATIVE_RETRIEVAL,
            strategy_mode="semantic",
            confidence=0.0,
            requires_clarification=False,
            reason_codes=["empty_query"],
        )
        _observe_decision(decision)
        return decision

    files_list = list(files or [])
    tabular_files = [file_obj for file_obj in files_list if _is_tabular_file(file_obj)]
    metric_critical = is_metric_critical_query(trimmed_query)
    intent_kind = detect_tabular_intent(trimmed_query)

    if not tabular_files:
        reason_codes.extend(["no_tabular_files", "narrative_default"])
        if metric_critical:
            reason_codes.append("metric_critical_without_tabular_dataset")
        decision = QueryPlanDecision(
            route=ROUTE_NARRATIVE_RETRIEVAL,
            intent=INTENT_NARRATIVE_RETRIEVAL,
            strategy_mode="semantic",
            confidence=0.8,
            requires_clarification=False,
            reason_codes=reason_codes,
            metric_critical=metric_critical,
        )
        _observe_decision(decision)
        return decision

    tabular_ready = [file_obj for file_obj in tabular_files if resolve_tabular_dataset(file_obj) is not None]
    if not tabular_ready:
        reason_codes.extend(["tabular_files_present", "tabular_dataset_unavailable", "narrative_default"])
        decision = QueryPlanDecision(
            route=ROUTE_NARRATIVE_RETRIEVAL,
            intent=INTENT_NARRATIVE_RETRIEVAL,
            strategy_mode="semantic",
            confidence=0.65,
            requires_clarification=False,
            reason_codes=reason_codes,
            metric_critical=metric_critical,
        )
        _observe_decision(decision)
        return decision

    reason_codes.extend(["tabular_dataset_available"])
    if is_complex_analytics_query(trimmed_query):
        reason_codes.append("complex_analytics_intent")
        decision = QueryPlanDecision(
            route=ROUTE_COMPLEX_ANALYTICS,
            intent=INTENT_COMPLEX_ANALYTICS,
            strategy_mode="analytical",
            confidence=0.93,
            requires_clarification=False,
            reason_codes=reason_codes,
            metric_critical=metric_critical,
        )
        _observe_decision(decision)
        return decision

    column_matches = _collect_column_matches(trimmed_query, tabular_ready)
    operation = _detect_operation(trimmed_query)
    semantic_tabular_hint = _has_semantic_tabular_hints(trimmed_query, tabular_ready)
    missing_scope_for_count = (
        operation == "count"
        and not semantic_tabular_hint
        and not column_matches
        and not any(hint in trimmed_query.lower() for hint in _AGGREGATE_SCOPE_HINTS)
    )
    missing_metric_column = operation in {"sum", "avg", "min", "max"} and not column_matches
    metric_ambiguous = bool(
        metric_critical
        and (
            intent_kind is None
            or operation is None
            or missing_scope_for_count
            or missing_metric_column
        )
    )

    if metric_ambiguous:
        if intent_kind == "profile":
            route = ROUTE_DETERMINISTIC_ANALYTICS
            intent = INTENT_TABULAR_PROFILE
            confidence = 0.75
            reason_codes.append("tabular_profile_intent")
            decision = QueryPlanDecision(
                route=route,
                intent=intent,
                strategy_mode="analytical",
                confidence=confidence,
                requires_clarification=False,
                reason_codes=reason_codes,
                metric_critical=metric_critical,
            )
            _observe_decision(decision)
            return decision

        reason_codes.extend(["metric_critical_ambiguous", "clarification_required"])
        if operation is None:
            reason_codes.append("missing_operation")
        if missing_metric_column:
            reason_codes.append("missing_metric_column")
        if missing_scope_for_count:
            reason_codes.append("missing_count_scope")
        decision = QueryPlanDecision(
            route=ROUTE_DETERMINISTIC_ANALYTICS,
            intent=INTENT_METRIC_CLARIFICATION,
            strategy_mode="analytical",
            confidence=0.35,
            requires_clarification=True,
            reason_codes=reason_codes,
            metric_critical=True,
            clarification_prompt=_build_clarification_prompt(trimmed_query),
        )
        _observe_decision(decision)
        return decision

    combined_hint = semantic_tabular_hint
    if intent_kind in {"aggregate", "lookup"} and combined_hint:
        reason_codes.extend(["combined_tabular_route", "semantic_tabular_hints"])
        if column_matches:
            reason_codes.append("column_match_found")
        decision = QueryPlanDecision(
            route=ROUTE_DETERMINISTIC_ANALYTICS,
            intent=INTENT_TABULAR_COMBINED,
            strategy_mode="combined",
            confidence=0.88 if column_matches else 0.8,
            requires_clarification=False,
            reason_codes=reason_codes,
            metric_critical=metric_critical,
        )
        _observe_decision(decision)
        return decision

    if intent_kind == "profile":
        reason_codes.append("tabular_profile_intent")
        decision = QueryPlanDecision(
            route=ROUTE_DETERMINISTIC_ANALYTICS,
            intent=INTENT_TABULAR_PROFILE,
            strategy_mode="analytical",
            confidence=0.95,
            requires_clarification=False,
            reason_codes=reason_codes,
            metric_critical=metric_critical,
        )
        _observe_decision(decision)
        return decision

    if intent_kind == "aggregate":
        reason_codes.append("tabular_aggregate_intent")
        if column_matches:
            reason_codes.append("column_match_found")
        decision = QueryPlanDecision(
            route=ROUTE_DETERMINISTIC_ANALYTICS,
            intent=INTENT_TABULAR_AGGREGATE,
            strategy_mode="analytical",
            confidence=0.92 if column_matches else 0.84,
            requires_clarification=False,
            reason_codes=reason_codes,
            metric_critical=metric_critical,
        )
        _observe_decision(decision)
        return decision

    if intent_kind == "lookup":
        reason_codes.append("tabular_lookup_intent")
        if column_matches:
            reason_codes.append("column_match_found")
        decision = QueryPlanDecision(
            route=ROUTE_DETERMINISTIC_ANALYTICS,
            intent=INTENT_TABULAR_LOOKUP,
            strategy_mode="analytical",
            confidence=0.86 if column_matches else 0.8,
            requires_clarification=False,
            reason_codes=reason_codes,
            metric_critical=metric_critical,
        )
        _observe_decision(decision)
        return decision

    reason_codes.extend(["no_deterministic_intent", "narrative_default"])
    decision = QueryPlanDecision(
        route=ROUTE_NARRATIVE_RETRIEVAL,
        intent=INTENT_NARRATIVE_RETRIEVAL,
        strategy_mode="semantic",
        confidence=0.72,
        requires_clarification=False,
        reason_codes=reason_codes,
        metric_critical=metric_critical,
    )
    _observe_decision(decision)
    return decision

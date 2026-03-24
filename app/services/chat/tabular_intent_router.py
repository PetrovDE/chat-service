from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.services.chat.tabular_query_parser import (
    ParsedTabularQuery,
    detect_legacy_tabular_intent as _detect_legacy_tabular_intent_parser,
    parse_tabular_query,
)
from app.services.chat.tabular_schema_resolver import (
    find_direct_column_mentions,
    normalize_text,
    resolve_requested_field,
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
    requested_field_text: Optional[str] = None
    candidate_columns: List[str] = field(default_factory=list)
    scored_candidates: List[Dict[str, Any]] = field(default_factory=list)
    matched_column: Optional[str] = None
    match_score: Optional[float] = None
    match_strategy: Optional[str] = None
    operation: Optional[str] = None
    lookup_value_text: Optional[str] = None
    lookup_field_text: Optional[str] = None
    group_by_field_text: Optional[str] = None

    @property
    def is_supported(self) -> bool:
        return self.selected_route != "unsupported_missing_column"


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


def _legacy_intent_for_route(route: str) -> Optional[str]:
    if route in {"schema_question", "overview"}:
        return "profile"
    if route in {"aggregation", "chart", "trend", "comparison", "unsupported_missing_column"}:
        return "aggregate"
    if route == "filtering":
        return "lookup"
    return None


def _field_resolution_for_query(
    *,
    parsed: ParsedTabularQuery,
    table: Any,
) -> Tuple[List[str], List[str], Optional[Dict[str, Any]]]:
    matched_columns: List[str] = []
    unmatched_requested_fields: List[str] = []

    direct_mentions = find_direct_column_mentions(parsed.requested_field_text or "", table)
    if direct_mentions:
        matched_columns.extend(direct_mentions)

    primary_resolution_payload: Optional[Dict[str, Any]] = None
    requested_fields = list(parsed.requested_fields)
    for index, requested_field in enumerate(requested_fields):
        resolution = resolve_requested_field(requested_field_text=requested_field, table=table)
        if resolution.status == "matched" and resolution.matched_column:
            matched_columns.append(str(resolution.matched_column))
        elif resolution.status in {"ambiguous", "no_match"}:
            unmatched_requested_fields.append(str(requested_field))
        if index == 0:
            primary_resolution_payload = {
                "requested_field_text": resolution.requested_field_text,
                "candidate_columns": list(resolution.candidate_columns),
                "scored_candidates": list(resolution.scored_candidates),
                "matched_column": resolution.matched_column,
                "match_score": resolution.match_score,
                "match_strategy": resolution.match_strategy,
                "status": resolution.status,
            }

    return _dedupe(matched_columns), _dedupe(unmatched_requested_fields), primary_resolution_payload


def classify_tabular_query(
    *,
    query: str,
    table: Optional[Any],
) -> TabularIntentDecision:
    parsed = parse_tabular_query(query)
    selected_route = str(parsed.route or "unknown")
    detected_intent = str(parsed.route or "unknown")

    matched_columns: List[str] = []
    unmatched_requested_fields: List[str] = []
    candidate_columns: List[str] = []
    scored_candidates: List[Dict[str, Any]] = []
    matched_column: Optional[str] = None
    match_score: Optional[float] = None
    match_strategy: Optional[str] = None
    fallback_reason = "none"

    if table is not None:
        direct_mentions = find_direct_column_mentions(query, table)
        matched_columns.extend(direct_mentions)

        resolved_matches, unresolved_requested, primary_resolution = _field_resolution_for_query(
            parsed=parsed,
            table=table,
        )
        matched_columns.extend(resolved_matches)
        unmatched_requested_fields.extend(unresolved_requested)
        matched_columns = _dedupe(matched_columns)
        unmatched_requested_fields = _dedupe(unmatched_requested_fields)

        if primary_resolution:
            candidate_columns = list(primary_resolution.get("candidate_columns") or [])
            scored_candidates = list(primary_resolution.get("scored_candidates") or [])
            matched_column = primary_resolution.get("matched_column")
            match_score = primary_resolution.get("match_score")
            match_strategy = primary_resolution.get("match_strategy")

    if selected_route in {"chart", "trend", "comparison"}:
        if unmatched_requested_fields or not matched_columns:
            selected_route = "unsupported_missing_column"
            fallback_reason = "missing_required_columns"

    if selected_route == "aggregation":
        if parsed.operation in {"sum", "avg", "min", "max"} and not matched_columns:
            if parsed.requested_field_text and parsed.requested_field_text not in unmatched_requested_fields:
                unmatched_requested_fields.append(parsed.requested_field_text)
            if not unmatched_requested_fields:
                unmatched_requested_fields.append("metric")
            selected_route = "unsupported_missing_column"
            fallback_reason = "missing_required_columns"

    if selected_route == "filtering":
        if parsed.lookup_value_text and parsed.lookup_field_text and not matched_columns:
            if parsed.lookup_field_text not in unmatched_requested_fields:
                unmatched_requested_fields.append(parsed.lookup_field_text)
            selected_route = "unsupported_missing_column"
            fallback_reason = "missing_required_columns"

    if selected_route == "unsupported_missing_column":
        fallback_reason = fallback_reason or "missing_required_columns"

    return TabularIntentDecision(
        detected_intent=detected_intent,
        selected_route=selected_route,
        legacy_intent=_legacy_intent_for_route(selected_route or detected_intent),
        requested_fields=_dedupe(parsed.requested_fields),
        matched_columns=matched_columns,
        unmatched_requested_fields=unmatched_requested_fields,
        fallback_reason=fallback_reason,
        requested_field_text=parsed.requested_field_text,
        candidate_columns=candidate_columns,
        scored_candidates=scored_candidates,
        matched_column=matched_column,
        match_score=match_score,
        match_strategy=match_strategy,
        operation=parsed.operation,
        lookup_value_text=parsed.lookup_value_text,
        lookup_field_text=parsed.lookup_field_text,
        group_by_field_text=parsed.group_by_field_text,
    )


def detect_legacy_tabular_intent(query: str) -> Optional[str]:
    return _detect_legacy_tabular_intent_parser(query)


def suggest_relevant_alternative_columns(table: Any, *, limit: int = 6) -> List[str]:
    columns = [str(col) for col in (list(getattr(table, "columns", []) or []))]
    aliases = getattr(table, "column_aliases", None)
    if not columns:
        return []
    if not isinstance(aliases, dict):
        aliases = {}

    def score(column_name: str) -> Tuple[int, int]:
        normalized = normalize_text(column_name)
        alias_norm = normalize_text(str(aliases.get(column_name, "")))
        identifier_penalty = 0
        if normalized == "id" or normalized.endswith(" id") or normalized.endswith("_id") or "uuid" in normalized:
            identifier_penalty = 20
        alias_bonus = 5 if alias_norm and alias_norm != normalized else 0
        token_richness = len(set([token for token in normalized.split() if token]))
        return (identifier_penalty * -1 + alias_bonus + token_richness, -len(column_name))

    ranked = sorted(columns, key=score, reverse=True)
    return _dedupe(ranked[: max(1, int(limit))])

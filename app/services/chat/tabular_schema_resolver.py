from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple


_NORM_RE = re.compile(r"[^a-zа-яё0-9]+")
_ID_TOKENS = {"id", "uuid", "guid"}
_TIME_TOKENS = {"date", "time", "month", "year", "day", "дата", "время", "месяц", "год", "день"}


def normalize_text(text: str) -> str:
    return _NORM_RE.sub(" ", (text or "").lower()).strip()


def tokenize(text: str) -> List[str]:
    normalized = normalize_text(text)
    return [token for token in normalized.split() if token]


def _clamp(value: float, min_value: float = 0.0, max_value: float = 1.0) -> float:
    return max(min_value, min(max_value, value))


def _contains_phrase(haystack: str, needle: str) -> bool:
    if not haystack or not needle:
        return False
    return f" {needle} " in f" {haystack} "


def _is_identifier_like(text: str) -> bool:
    tokens = set(tokenize(text))
    if not tokens:
        return False
    if tokens.intersection(_ID_TOKENS):
        return True
    normalized = normalize_text(text)
    return bool(
        normalized == "id"
        or normalized.endswith(" id")
        or normalized.endswith("_id")
        or "_id_" in normalized
    )


def _has_time_tokens(text: str) -> bool:
    tokens = set(tokenize(text))
    return bool(tokens.intersection(_TIME_TOKENS))


def _column_aliases(table: Any) -> Dict[str, str]:
    aliases = getattr(table, "column_aliases", None)
    if not isinstance(aliases, dict):
        return {}
    return {str(key): str(value) for key, value in aliases.items()}


@dataclass(frozen=True)
class ColumnScore:
    column: str
    score: float
    strategy: str
    reasons: List[str]

    def as_debug(self) -> Dict[str, Any]:
        return {
            "column": self.column,
            "score": round(float(self.score), 6),
            "strategy": self.strategy,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class FieldResolution:
    requested_field_text: Optional[str]
    status: str
    matched_column: Optional[str]
    match_score: Optional[float]
    match_strategy: Optional[str]
    candidate_columns: List[str]
    scored_candidates: List[Dict[str, Any]]


def find_direct_column_mentions(query: str, table: Any) -> List[str]:
    q_norm = normalize_text(query)
    if not q_norm:
        return []
    aliases = _column_aliases(table)
    out: List[str] = []
    seen = set()
    for raw_column in list(getattr(table, "columns", []) or []):
        column = str(raw_column)
        column_norm = normalize_text(column)
        alias_norm = normalize_text(str(aliases.get(column, "")))
        if (column_norm and _contains_phrase(q_norm, column_norm)) or (
            alias_norm and _contains_phrase(q_norm, alias_norm)
        ):
            key = column.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(column)
    return out


def _score_candidate_for_requested_text(
    *,
    requested_text: str,
    requested_norm: str,
    requested_tokens: Sequence[str],
    candidate_text: str,
) -> Tuple[float, str, List[str]]:
    candidate_norm = normalize_text(candidate_text)
    candidate_tokens = tokenize(candidate_text)
    if not candidate_norm:
        return 0.0, "none", []

    score = 0.0
    strategy = "fuzzy_similarity"
    reasons: List[str] = []

    if requested_norm == candidate_norm:
        return 1.0, "exact_normalized_match", ["exact_normalized_match"]

    if requested_norm and _contains_phrase(candidate_norm, requested_norm):
        score = max(score, 0.9)
        strategy = "contains_match"
        reasons.append("candidate_contains_requested")
    if candidate_norm and _contains_phrase(requested_norm, candidate_norm):
        score = max(score, 0.84)
        strategy = "contains_match"
        reasons.append("requested_contains_candidate")

    requested_token_set = set(requested_tokens)
    candidate_token_set = set(candidate_tokens)
    if requested_token_set and candidate_token_set:
        intersection = requested_token_set.intersection(candidate_token_set)
        if intersection:
            overlap_ratio = float(len(intersection) / max(1, len(requested_token_set)))
            token_score = 0.46 + (0.42 * overlap_ratio)
            score = max(score, token_score)
            if token_score >= score:
                strategy = "token_overlap"
            reasons.append(f"token_overlap={round(overlap_ratio, 3)}")

    fuzzy_ratio = SequenceMatcher(a=requested_norm, b=candidate_norm).ratio()
    fuzzy_score = 0.2 + (0.7 * fuzzy_ratio)
    if fuzzy_score > score:
        strategy = "fuzzy_similarity"
    score = max(score, fuzzy_score)
    reasons.append(f"fuzzy_similarity={round(fuzzy_ratio, 3)}")

    if _has_time_tokens(requested_text) and _has_time_tokens(candidate_text):
        score += 0.06
        reasons.append("time_semantics_compatible")

    if not _is_identifier_like(requested_text) and _is_identifier_like(candidate_text):
        score -= 0.08
        reasons.append("identifier_penalty")

    return _clamp(score), strategy, reasons


def resolve_requested_field(
    *,
    requested_field_text: Optional[str],
    table: Any,
    min_confidence: float = 0.68,
    ambiguity_gap: float = 0.08,
    max_debug_candidates: int = 12,
) -> FieldResolution:
    requested = str(requested_field_text or "").strip()
    if not requested:
        return FieldResolution(
            requested_field_text=None,
            status="not_requested",
            matched_column=None,
            match_score=None,
            match_strategy=None,
            candidate_columns=[],
            scored_candidates=[],
        )

    aliases = _column_aliases(table)
    requested_norm = normalize_text(requested)
    requested_tokens = tokenize(requested)

    best_by_column: Dict[str, ColumnScore] = {}
    for raw_column in list(getattr(table, "columns", []) or []):
        column = str(raw_column)
        candidate_variants = [column]
        alias_value = str(aliases.get(column, "")).strip()
        if alias_value:
            candidate_variants.append(alias_value)

        best_variant_score = 0.0
        best_variant_strategy = "fuzzy_similarity"
        best_variant_reasons: List[str] = []

        for variant in candidate_variants:
            score, strategy, reasons = _score_candidate_for_requested_text(
                requested_text=requested,
                requested_norm=requested_norm,
                requested_tokens=requested_tokens,
                candidate_text=variant,
            )
            if score > best_variant_score:
                best_variant_score = score
                best_variant_strategy = strategy
                best_variant_reasons = reasons

        best_by_column[column] = ColumnScore(
            column=column,
            score=best_variant_score,
            strategy=best_variant_strategy,
            reasons=best_variant_reasons,
        )

    scored = sorted(
        best_by_column.values(),
        key=lambda item: (-float(item.score), len(item.column), item.column.lower()),
    )
    candidate_columns = [item.column for item in scored]
    scored_candidates = [item.as_debug() for item in scored[: max(1, int(max_debug_candidates))]]
    if not scored:
        return FieldResolution(
            requested_field_text=requested,
            status="no_match",
            matched_column=None,
            match_score=None,
            match_strategy=None,
            candidate_columns=[],
            scored_candidates=[],
        )

    top = scored[0]
    second = scored[1] if len(scored) > 1 else None
    top_score = float(top.score)
    second_score = float(second.score) if second is not None else None

    if top_score < float(min_confidence):
        return FieldResolution(
            requested_field_text=requested,
            status="no_match",
            matched_column=None,
            match_score=top_score,
            match_strategy=top.strategy,
            candidate_columns=candidate_columns,
            scored_candidates=scored_candidates,
        )

    if second is not None and second_score is not None:
        gap = top_score - second_score
        if second_score >= float(min_confidence) and gap < float(ambiguity_gap):
            return FieldResolution(
                requested_field_text=requested,
                status="ambiguous",
                matched_column=None,
                match_score=top_score,
                match_strategy=top.strategy,
                candidate_columns=candidate_columns,
                scored_candidates=scored_candidates,
            )

    return FieldResolution(
        requested_field_text=requested,
        status="matched",
        matched_column=top.column,
        match_score=top_score,
        match_strategy=top.strategy,
        candidate_columns=candidate_columns,
        scored_candidates=scored_candidates,
    )

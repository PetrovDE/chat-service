from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


_NORM_RE = re.compile(r"[_\W]+", flags=re.UNICODE)
_ID_TOKENS = {"id", "uuid", "guid", "identifier"}
_TIME_TOKENS = {"date", "time", "month", "year", "day", "week", "quarter", "timestamp"}
_NUMERIC_HINT_TOKENS = {
    "sum",
    "avg",
    "average",
    "mean",
    "count",
    "total",
    "amount",
    "price",
    "cost",
    "value",
    "score",
    "ratio",
    "percent",
    "quantity",
}


def normalize_text(text: str) -> str:
    return _NORM_RE.sub(" ", str(text or "").lower()).strip()


def tokenize(text: str) -> List[str]:
    normalized = normalize_text(text)
    return [token for token in normalized.split() if token]


def _clamp(value: float, min_value: float = 0.0, max_value: float = 1.0) -> float:
    return max(min_value, min(max_value, value))


def _contains_phrase(haystack: str, needle: str) -> bool:
    if not haystack or not needle:
        return False
    return f" {needle} " in f" {haystack} "


def _column_aliases(table: Any) -> Dict[str, str]:
    aliases = getattr(table, "column_aliases", None)
    if not isinstance(aliases, dict):
        return {}
    return {str(key): str(value) for key, value in aliases.items()}


def _column_metadata(table: Any) -> Dict[str, Dict[str, Any]]:
    raw = getattr(table, "column_metadata", None)
    if not isinstance(raw, dict):
        return {}
    metadata: Dict[str, Dict[str, Any]] = {}
    for key, payload in raw.items():
        if not isinstance(payload, dict):
            continue
        parsed: Dict[str, Any] = {}
        display_name = str(payload.get("display_name") or "").strip()
        if display_name:
            parsed["display_name"] = display_name
        dtype = str(payload.get("dtype") or "").strip().lower()
        if dtype:
            parsed["dtype"] = dtype
        aliases_raw = payload.get("aliases")
        if isinstance(aliases_raw, list):
            aliases = [str(item).strip() for item in aliases_raw if str(item).strip()]
            if aliases:
                parsed["aliases"] = aliases
        sample_values_raw = payload.get("sample_values")
        if isinstance(sample_values_raw, list):
            sample_values = [str(item).strip() for item in sample_values_raw if str(item).strip()]
            if sample_values:
                parsed["sample_values"] = sample_values[:12]
        metadata[str(key)] = parsed
    return metadata


def _iter_candidate_variants(*, column: str, alias: str, metadata: Dict[str, Any]) -> Iterable[Tuple[str, str]]:
    seen = set()

    def _yield(source: str, value: str) -> Iterable[Tuple[str, str]]:
        raw = str(value or "").strip()
        if not raw:
            return []
        key = (source, normalize_text(raw))
        if key in seen or not key[1]:
            return []
        seen.add(key)
        return [(source, raw)]

    for item in _yield("raw_column_name", column):
        yield item
    for item in _yield("normalized_column_name", column.replace("_", " ")):
        yield item
    for item in _yield("display_name", alias):
        yield item
    for item in _yield("metadata_display_name", str(metadata.get("display_name") or "")):
        yield item
    aliases = metadata.get("aliases")
    if isinstance(aliases, list):
        for alias_value in aliases:
            for item in _yield("metadata_alias", str(alias_value)):
                yield item


def _is_identifier_like(text: str) -> bool:
    normalized = normalize_text(text)
    tokens = set(tokenize(text))
    return bool(
        tokens.intersection(_ID_TOKENS)
        or normalized == "id"
        or normalized.endswith(" id")
        or normalized.endswith("_id")
        or " id " in normalized
    )


def _expected_dtype_from_requested_tokens(requested_tokens: Sequence[str]) -> Optional[str]:
    token_set = set(requested_tokens)
    if not token_set:
        return None
    if token_set.intersection(_TIME_TOKENS):
        return "datetime"
    if token_set.intersection(_NUMERIC_HINT_TOKENS):
        return "numeric"
    if token_set.intersection(_ID_TOKENS):
        return "identifier"
    return None


def _dtype_family_from_text(dtype_text: str) -> str:
    text = normalize_text(dtype_text)
    if not text:
        return "unknown"
    if any(token in text for token in ("int", "float", "double", "decimal", "numeric")):
        return "numeric"
    if any(token in text for token in ("date", "time", "timestamp")):
        return "datetime"
    if any(token in text for token in ("bool", "boolean")):
        return "boolean"
    if any(token in text for token in ("string", "str", "text", "object", "category")):
        return "categorical"
    return "unknown"


def _dtype_family_for_column(*, column: str, alias: str, metadata: Dict[str, Any]) -> str:
    metadata_dtype = _dtype_family_from_text(str(metadata.get("dtype") or ""))
    if metadata_dtype != "unknown":
        return metadata_dtype

    descriptor = normalize_text(" ".join([column, alias, str(metadata.get("display_name") or "")]))
    if not descriptor:
        return "unknown"
    if _is_identifier_like(descriptor):
        return "identifier"
    if any(token in descriptor for token in _TIME_TOKENS):
        return "datetime"
    if any(token in descriptor for token in _NUMERIC_HINT_TOKENS):
        return "numeric"
    return "categorical"


def _score_variant(
    *,
    requested_norm: str,
    requested_tokens: Sequence[str],
    variant_norm: str,
    source: str,
) -> Tuple[float, str, List[str]]:
    if not variant_norm:
        return 0.0, "none", []

    exact_strategy = {
        "raw_column_name": "exact_normalized_match",
        "normalized_column_name": "exact_normalized_match",
        "display_name": "display_name_match",
        "metadata_display_name": "display_name_match",
        "metadata_alias": "metadata_alias_match",
    }
    exact_score = {
        "raw_column_name": 1.0,
        "normalized_column_name": 0.995,
        "display_name": 0.985,
        "metadata_display_name": 0.98,
        "metadata_alias": 0.975,
    }

    if requested_norm == variant_norm:
        strategy = exact_strategy.get(source, "exact_normalized_match")
        return exact_score.get(source, 0.97), strategy, [f"exact_match:{source}"]

    score = 0.0
    strategy = "fuzzy_similarity"
    reasons: List[str] = [f"source={source}"]

    if requested_norm and _contains_phrase(variant_norm, requested_norm):
        score = max(score, 0.88)
        strategy = "contains_match"
        reasons.append("candidate_contains_requested")
    if variant_norm and _contains_phrase(requested_norm, variant_norm):
        score = max(score, 0.84)
        strategy = "contains_match"
        reasons.append("requested_contains_candidate")

    requested_token_set = set(requested_tokens)
    candidate_token_set = set(tokenize(variant_norm))
    if requested_token_set and candidate_token_set:
        overlap = requested_token_set.intersection(candidate_token_set)
        if overlap:
            overlap_ratio = float(len(overlap) / max(1, len(requested_token_set)))
            token_score = 0.42 + (0.42 * overlap_ratio)
            if token_score > score:
                strategy = "token_overlap"
            score = max(score, token_score)
            reasons.append(f"token_overlap={round(overlap_ratio, 3)}")

    fuzzy_ratio = SequenceMatcher(a=requested_norm, b=variant_norm).ratio()
    fuzzy_score = 0.18 + (0.68 * fuzzy_ratio)
    if fuzzy_score > score:
        strategy = "fuzzy_similarity"
    score = max(score, fuzzy_score)
    reasons.append(f"fuzzy_similarity={round(fuzzy_ratio, 3)}")

    source_bonus = {
        "display_name": 0.03,
        "metadata_display_name": 0.025,
        "metadata_alias": 0.025,
        "normalized_column_name": 0.015,
    }.get(source, 0.0)
    if source_bonus > 0.0:
        score += source_bonus
        reasons.append(f"source_bonus={round(source_bonus, 3)}")

    return _clamp(score), strategy, reasons


@dataclass(frozen=True)
class ColumnScore:
    column: str
    score: float
    strategy: str
    reasons: List[str]
    dtype_family: str

    def as_debug(self) -> Dict[str, Any]:
        return {
            "column": self.column,
            "score": round(float(self.score), 6),
            "strategy": self.strategy,
            "dtype_family": self.dtype_family,
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
    query_norm = normalize_text(query)
    if not query_norm:
        return []
    aliases = _column_aliases(table)
    metadata = _column_metadata(table)
    out: List[str] = []
    seen = set()
    for raw_column in list(getattr(table, "columns", []) or []):
        column = str(raw_column)
        alias = str(aliases.get(column, ""))
        column_meta = metadata.get(column, {})
        for _, variant in _iter_candidate_variants(column=column, alias=alias, metadata=column_meta):
            variant_norm = normalize_text(variant)
            if variant_norm and _contains_phrase(query_norm, variant_norm):
                key = column.lower()
                if key not in seen:
                    seen.add(key)
                    out.append(column)
                break
    return out


def resolve_requested_field(
    *,
    requested_field_text: Optional[str],
    table: Any,
    min_confidence: float = 0.72,
    ambiguity_gap: float = 0.08,
    max_debug_candidates: int = 12,
    expected_dtype_family: Optional[str] = None,
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
    metadata = _column_metadata(table)
    requested_norm = normalize_text(requested)
    requested_tokens = tokenize(requested)
    effective_expected_dtype = expected_dtype_family or _expected_dtype_from_requested_tokens(requested_tokens)

    best_by_column: Dict[str, ColumnScore] = {}
    for raw_column in list(getattr(table, "columns", []) or []):
        column = str(raw_column)
        alias = str(aliases.get(column, ""))
        column_meta = metadata.get(column, {})
        dtype_family = _dtype_family_for_column(column=column, alias=alias, metadata=column_meta)

        best_variant_score = 0.0
        best_variant_strategy = "fuzzy_similarity"
        best_variant_reasons: List[str] = []

        for source, variant in _iter_candidate_variants(column=column, alias=alias, metadata=column_meta):
            variant_norm = normalize_text(variant)
            score, strategy, reasons = _score_variant(
                requested_norm=requested_norm,
                requested_tokens=requested_tokens,
                variant_norm=variant_norm,
                source=source,
            )
            if score > best_variant_score:
                best_variant_score = score
                best_variant_strategy = strategy
                best_variant_reasons = reasons

        if effective_expected_dtype:
            if dtype_family == effective_expected_dtype:
                best_variant_score += 0.06
                best_variant_reasons.append(f"dtype_compatible:{effective_expected_dtype}")
            elif dtype_family != "unknown":
                best_variant_score -= 0.08
                best_variant_reasons.append(
                    f"dtype_mismatch:expected={effective_expected_dtype},actual={dtype_family}"
                )

        if not _is_identifier_like(requested) and dtype_family == "identifier":
            best_variant_score -= 0.06
            best_variant_reasons.append("identifier_penalty")
        elif _is_identifier_like(requested) and dtype_family == "identifier":
            best_variant_score += 0.04
            best_variant_reasons.append("identifier_bonus")

        sample_values = column_meta.get("sample_values") if isinstance(column_meta, dict) else None
        if isinstance(sample_values, list):
            normalized_samples = {normalize_text(str(item)) for item in sample_values if normalize_text(str(item))}
            if requested_norm and requested_norm in normalized_samples:
                best_variant_score -= 0.04
                best_variant_reasons.append("requested_text_matches_sample_value_penalty")

        best_by_column[column] = ColumnScore(
            column=column,
            score=_clamp(best_variant_score),
            strategy=best_variant_strategy,
            reasons=best_variant_reasons,
            dtype_family=dtype_family,
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

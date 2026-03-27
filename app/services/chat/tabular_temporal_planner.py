from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.services.chat.tabular_schema_resolver import (
    find_direct_column_mentions,
    normalize_text,
    resolve_requested_field,
)
from app.services.tabular.column_metadata_contract import sanitize_tabular_column_metadata


TIME_GRAIN_TOKENS: Dict[str, Tuple[str, ...]] = {
    "day": (
        "day",
        "days",
        "daily",
        "\u0434\u0435\u043d\u044c",
        "\u0434\u043d\u0438",
        "\u0434\u043d\u0435\u0439",
        "\u0434\u043d\u0435\u0432\u043d",
    ),
    "week": (
        "week",
        "weeks",
        "weekly",
        "\u043d\u0435\u0434\u0435\u043b\u044f",
        "\u043d\u0435\u0434\u0435\u043b\u0438",
        "\u043d\u0435\u0434\u0435\u043b\u044c",
        "\u0435\u0436\u0435\u043d\u0435\u0434\u0435\u043b",
    ),
    "month": (
        "month",
        "months",
        "monthly",
        "\u043c\u0435\u0441\u044f\u0446",
        "\u043c\u0435\u0441\u044f\u0446\u0430",
        "\u043c\u0435\u0441\u044f\u0446\u044b",
        "\u043c\u0435\u0441\u044f\u0446\u0435\u0432",
        "\u043c\u0435\u0441\u044f\u0446\u0430\u043c",
        "\u043f\u043e\u043c\u0435\u0441\u044f\u0447",
    ),
    "quarter": (
        "quarter",
        "quarters",
        "quarterly",
        "q1",
        "q2",
        "q3",
        "q4",
        "\u043a\u0432\u0430\u0440\u0442\u0430\u043b",
        "\u043a\u0432\u0430\u0440\u0442\u0430\u043b\u044b",
        "\u043f\u043e\u043a\u0432\u0430\u0440\u0442\u0430\u043b",
    ),
    "year": (
        "year",
        "years",
        "yearly",
        "annual",
        "annually",
        "\u0433\u043e\u0434",
        "\u0433\u043e\u0434\u0430",
        "\u0433\u043e\u0434\u044b",
        "\u0433\u043e\u0434\u043e\u0432",
        "\u0435\u0436\u0435\u0433\u043e\u0434",
    ),
}

_GENERIC_DATETIME_HINTS = {
    "date",
    "dates",
    "date column",
    "date field",
    "datetime",
    "datetime column",
    "datetime field",
    "time",
    "timestamp",
    "timestamps",
    "\u0434\u0430\u0442\u0430",
    "\u0434\u0430\u0442\u044b",
    "\u0434\u0430\u0442",
    "\u0432\u0440\u0435\u043c\u044f",
    "\u0432\u0440\u0435\u043c\u0435\u043d\u0438",
    "\u0432\u0440\u0435\u043c\u0435\u043d",
}
_GENERIC_TIME_QUALIFIER_TOKENS = {
    "date",
    "dates",
    "time",
    "datetime",
    "timestamp",
    "column",
    "field",
    "the",
    "a",
    "an",
    "\u0438\u0437",
    "\u043f\u043e",
    "\u0432",
    "\u043d\u0430",
    "\u0438",
    "\u0434\u0430\u0442",
    "\u0434\u0430\u0442\u044b",
    "\u0434\u0430\u0442\u0435",
    "\u0434\u0430\u0442\u0430\u043c",
    "\u043c\u0435\u0441\u044f\u0446",
    "\u043c\u0435\u0441\u044f\u0446\u044b",
    "\u043c\u0435\u0441\u044f\u0446\u0430",
}

_DATETIME_NAME_TOKENS = (
    "date",
    "time",
    "timestamp",
    "datetime",
    "created",
    "updated",
    "event",
    "\u0434\u0430\u0442",
    "\u0432\u0440\u0435\u043c",
    "\u0441\u043e\u0437\u0434\u0430\u043d",
    "\u043e\u0431\u043d\u043e\u0432\u043b",
)
_METRIC_NAME_TOKENS = (
    "amount",
    "total",
    "value",
    "revenue",
    "sales",
    "price",
    "cost",
    "profit",
    "income",
    "expense",
    "spend",
    "spending",
    "\u0441\u0443\u043c\u043c",
    "\u043e\u0431\u044a\u0435\u043c",
    "\u0437\u0430\u0442\u0440\u0430\u0442",
    "\u0440\u0430\u0441\u0445\u043e\u0434",
    "\u0432\u044b\u0440\u0443\u0447",
    "\u0441\u0442\u043e\u0438\u043c",
)
_NORM_RE = re.compile(r"[^a-z0-9_ ]+")


def _norm(text: str) -> str:
    return _NORM_RE.sub(" ", str(text or "").lower()).strip()


def detect_requested_time_grain(query: str) -> Optional[str]:
    normalized = normalize_text(query)
    if not normalized:
        return None
    for grain, tokens in TIME_GRAIN_TOKENS.items():
        for token in tokens:
            token_norm = normalize_text(token)
            if token_norm and f" {token_norm} " in f" {normalized} ":
                return grain
    return None


def _truncate_clause(candidate: str) -> str:
    text = normalize_text(candidate)
    if not text:
        return ""
    text = re.split(
        r"\b(and|with|where|for|by|in|on|from|using|then|\u0438|\u0441|\u0434\u043b\u044f|\u0433\u0434\u0435|\u043a\u043e\u0433\u0434\u0430|\u043f\u043e|\u0438\u0437|\u0432\u044b\u0434\u0435\u043b\u0438\u0432|\u0442\u043e\u043b\u044c\u043a\u043e)\b",
        text,
        maxsplit=1,
    )[0]
    return normalize_text(text)


def _extract_time_grain_qualifier(*, query: str, requested_time_grain: str) -> Optional[str]:
    q = normalize_text(query)
    grain = normalize_text(requested_time_grain)
    if not q or not grain:
        return None
    patterns = (
        rf"\b{re.escape(grain)}\s+of\s+([a-z\u0430-\u044f\u04510-9_ ]{{2,120}})",
        rf"\b{re.escape(grain)}\s+from\s+([a-z\u0430-\u044f\u04510-9_ ]{{2,120}})",
        rf"\b{re.escape(grain)}\s+\u0438\u0437\s+([a-z\u0430-\u044f\u04510-9_ ]{{2,120}})",
    )
    for pattern in patterns:
        match = re.search(pattern, q)
        if not match:
            continue
        qualifier = _truncate_clause(str(match.group(1) or ""))
        if not qualifier:
            continue
        tokens = [token for token in qualifier.split() if token and token not in _GENERIC_TIME_QUALIFIER_TOKENS]
        if not tokens:
            continue
        return " ".join(tokens)
    return None


def extract_datetime_source_hint(query: str) -> Optional[str]:
    q = normalize_text(query)
    if not q:
        return None

    patterns = (
        r"\buse\s+([a-z\u0430-\u044f\u04510-9_ ]{2,120})",
        r"\bfrom\s+([a-z\u0430-\u044f\u04510-9_ ]{2,120})",
        r"\busing\s+([a-z\u0430-\u044f\u04510-9_ ]{2,120})",
        r"\bwith\s+([a-z\u0430-\u044f\u04510-9_ ]{2,120})",
        r"\b\u0438\u0437\s+([a-z\u0430-\u044f\u04510-9_ ]{2,120})",
    )
    for pattern in patterns:
        match = re.search(pattern, q)
        if not match:
            continue
        candidate = _truncate_clause(str(match.group(1) or ""))
        if not candidate:
            continue
        candidate = re.sub(
            r"\b(column|field|\u043a\u043e\u043b\u043e\u043d\u043a[\u0430-\u044f]*|\u043f\u043e\u043b[\u0435-\u044f]*)\b$",
            "",
            candidate,
        ).strip()
        if not candidate:
            continue
        return candidate
    return None


def has_temporal_grouping_signal(query: str) -> bool:
    q = normalize_text(query)
    if not q:
        return False
    if detect_requested_time_grain(q):
        if "group by" in q:
            return True
        if " by " in f" {q} ":
            return True
        if " \u043f\u043e " in f" {q} ":
            return True
        if " over time " in f" {q} ":
            return True
        if " \u0434\u0438\u043d\u0430\u043c" in f" {q} ":
            return True
    return False


def _column_aliases(table: Any) -> Dict[str, str]:
    aliases = getattr(table, "column_aliases", None)
    if not isinstance(aliases, dict):
        return {}
    return {str(key): str(value) for key, value in aliases.items()}


def _column_metadata(table: Any) -> Dict[str, Dict[str, Any]]:
    raw = getattr(table, "column_metadata", None)
    columns = [str(col) for col in list(getattr(table, "columns", []) or [])]
    aliases = _column_aliases(table)
    metadata, _stats = sanitize_tabular_column_metadata(
        raw_metadata=raw,
        columns=columns,
        aliases=aliases,
    )
    return metadata


def _dtype_family_from_text(dtype_text: str) -> str:
    text = normalize_text(dtype_text)
    if not text:
        return "unknown"
    if any(token in text for token in ("date", "time", "timestamp")):
        return "datetime"
    if any(token in text for token in ("int", "float", "double", "decimal", "numeric", "number")):
        return "numeric"
    if any(token in text for token in ("bool", "boolean")):
        return "boolean"
    if any(token in text for token in ("string", "str", "text", "object", "category")):
        return "categorical"
    return "unknown"


def _is_datetime_candidate(*, column: str, alias: str, metadata: Dict[str, Any]) -> bool:
    if _dtype_family_from_text(str(metadata.get("dtype") or "")) == "datetime":
        return True
    descriptor = normalize_text(" ".join([column, alias, str(metadata.get("display_name") or "")]))
    return any(token in descriptor for token in _DATETIME_NAME_TOKENS)


def _score_datetime_candidate(
    *,
    query_norm: str,
    source_hint_norm: str,
    column: str,
    alias: str,
    metadata: Dict[str, Any],
) -> Tuple[float, List[str]]:
    score = 0.0
    reasons: List[str] = []
    column_norm = normalize_text(column)
    alias_norm = normalize_text(alias)
    display_norm = normalize_text(str(metadata.get("display_name") or ""))
    dtype_family = _dtype_family_from_text(str(metadata.get("dtype") or ""))
    if dtype_family == "datetime":
        score += 0.55
        reasons.append("dtype_datetime")
    descriptor = " ".join([column_norm, alias_norm, display_norm]).strip()
    if any(token in descriptor for token in _DATETIME_NAME_TOKENS):
        score += 0.18
        reasons.append("name_datetime_signal")
    for variant in (column_norm, alias_norm, display_norm):
        if variant and f" {variant} " in f" {query_norm} ":
            score += 0.2
            reasons.append("query_mentions_candidate")
            break
    if source_hint_norm:
        for variant in (column_norm, alias_norm, display_norm):
            if not variant:
                continue
            if variant == source_hint_norm:
                score += 0.24
                reasons.append("source_hint_exact_match")
                break
            if f" {source_hint_norm} " in f" {variant} " or f" {variant} " in f" {source_hint_norm} ":
                score += 0.14
                reasons.append("source_hint_overlap")
                break
    return max(0.0, min(1.0, score)), reasons


@dataclass(frozen=True)
class TemporalResolution:
    requested_time_grain: Optional[str]
    source_datetime_field: Optional[str]
    derived_grouping_dimension: Optional[str]
    temporal_plan_status: str
    candidate_datetime_fields: List[str]
    scored_datetime_candidates: List[Dict[str, Any]]
    source_datetime_hint: Optional[str]
    fallback_reason: str = "none"

    def as_debug(self) -> Dict[str, Any]:
        return {
            "requested_time_grain": self.requested_time_grain,
            "source_datetime_field": self.source_datetime_field,
            "derived_temporal_dimension": self.derived_grouping_dimension,
            "temporal_plan_status": self.temporal_plan_status,
            "candidate_datetime_fields": list(self.candidate_datetime_fields),
            "scored_datetime_candidates": list(self.scored_datetime_candidates),
            "source_datetime_hint": self.source_datetime_hint,
            "fallback_reason": self.fallback_reason,
        }


@dataclass(frozen=True)
class TemporalMeasureResolution:
    status: str
    measure_column: Optional[str]
    candidate_columns: List[str]
    scored_candidates: List[Dict[str, Any]]
    fallback_reason: str = "none"

    def as_debug(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "measure_column": self.measure_column,
            "candidate_columns": list(self.candidate_columns),
            "scored_candidates": list(self.scored_candidates),
            "fallback_reason": self.fallback_reason,
        }


def resolve_temporal_grouping(
    *,
    query: str,
    table: Any,
    requested_time_grain: Optional[str],
    source_datetime_hint: Optional[str],
    min_confidence: float = 0.58,
    ambiguity_gap: float = 0.08,
) -> TemporalResolution:
    grain = str(requested_time_grain or "").strip().lower() or None
    hint_text = str(source_datetime_hint or "").strip() or None
    hint_norm = normalize_text(hint_text or "")
    if not grain:
        return TemporalResolution(
            requested_time_grain=None,
            source_datetime_field=None,
            derived_grouping_dimension=None,
            temporal_plan_status="not_requested",
            candidate_datetime_fields=[],
            scored_datetime_candidates=[],
            source_datetime_hint=hint_text,
            fallback_reason="not_requested",
        )

    hinted_resolution_payload: Dict[str, Any] | None = None
    if hint_text and hint_norm not in _GENERIC_DATETIME_HINTS:
        hinted_resolution = resolve_requested_field(
            requested_field_text=hint_text,
            table=table,
            expected_dtype_family="datetime",
        )
        hinted_resolution_payload = {
            "status": hinted_resolution.status,
            "candidate_columns": list(hinted_resolution.candidate_columns),
            "scored_candidates": list(hinted_resolution.scored_candidates),
            "match_score": hinted_resolution.match_score,
            "match_strategy": hinted_resolution.match_strategy,
        }
        if hinted_resolution.status == "matched" and hinted_resolution.matched_column:
            source_column = str(hinted_resolution.matched_column)
            return TemporalResolution(
                requested_time_grain=grain,
                source_datetime_field=source_column,
                derived_grouping_dimension=f"{grain}({source_column})",
                temporal_plan_status="resolved",
                candidate_datetime_fields=list(hinted_resolution.candidate_columns),
                scored_datetime_candidates=list(hinted_resolution.scored_candidates),
                source_datetime_hint=hint_text,
            )

    aliases = _column_aliases(table)
    metadata = _column_metadata(table)
    query_norm = normalize_text(query)
    datetime_candidates: List[str] = []
    scored: List[Dict[str, Any]] = []
    for raw_column in list(getattr(table, "columns", []) or []):
        column = str(raw_column)
        alias = str(aliases.get(column, ""))
        column_meta = metadata.get(column, {})
        if not _is_datetime_candidate(column=column, alias=alias, metadata=column_meta):
            continue
        score, reasons = _score_datetime_candidate(
            query_norm=query_norm,
            source_hint_norm=hint_norm,
            column=column,
            alias=alias,
            metadata=column_meta,
        )
        datetime_candidates.append(column)
        scored.append(
            {
                "column": column,
                "score": round(float(score), 6),
                "reasons": reasons,
            }
        )

    if hinted_resolution_payload and hinted_resolution_payload.get("scored_candidates"):
        scored = list(hinted_resolution_payload["scored_candidates"])
        datetime_candidates = [str(item) for item in hinted_resolution_payload.get("candidate_columns") or []]

    if not datetime_candidates:
        return TemporalResolution(
            requested_time_grain=grain,
            source_datetime_field=None,
            derived_grouping_dimension=None,
            temporal_plan_status="no_datetime_source",
            candidate_datetime_fields=[],
            scored_datetime_candidates=[],
            source_datetime_hint=hint_text,
            fallback_reason="no_datetime_source",
        )

    qualifier = _extract_time_grain_qualifier(query=query, requested_time_grain=grain)
    if qualifier:
        normalized_candidates = []
        for candidate in datetime_candidates:
            alias = str(aliases.get(candidate, ""))
            candidate_meta = metadata.get(candidate, {})
            descriptor = normalize_text(" ".join([candidate, alias, str(candidate_meta.get("display_name") or "")]))
            normalized_candidates.append((candidate, descriptor))
        matched_by_qualifier = [
            candidate
            for candidate, descriptor in normalized_candidates
            if qualifier and (
                f" {qualifier} " in f" {descriptor} "
                or any(f" {token} " in f" {descriptor} " for token in qualifier.split())
            )
        ]
        if not matched_by_qualifier:
            return TemporalResolution(
                requested_time_grain=grain,
                source_datetime_field=None,
                derived_grouping_dimension=None,
                temporal_plan_status="ambiguous_datetime_source",
                candidate_datetime_fields=list(datetime_candidates),
                scored_datetime_candidates=list(scored),
                source_datetime_hint=hint_text,
                fallback_reason="datetime_qualifier_not_matched",
            )

    if len(datetime_candidates) == 1:
        source_column = str(datetime_candidates[0])
        return TemporalResolution(
            requested_time_grain=grain,
            source_datetime_field=source_column,
            derived_grouping_dimension=f"{grain}({source_column})",
            temporal_plan_status="resolved",
            candidate_datetime_fields=list(datetime_candidates),
            scored_datetime_candidates=list(scored),
            source_datetime_hint=hint_text,
            fallback_reason="none",
        )

    mentioned_datetime_fields = [
        str(column)
        for column in find_direct_column_mentions(query, table)
        if str(column) in {str(item) for item in datetime_candidates}
    ]
    if len(mentioned_datetime_fields) == 1:
        source_column = str(mentioned_datetime_fields[0])
        return TemporalResolution(
            requested_time_grain=grain,
            source_datetime_field=source_column,
            derived_grouping_dimension=f"{grain}({source_column})",
            temporal_plan_status="resolved",
            candidate_datetime_fields=list(datetime_candidates),
            scored_datetime_candidates=list(scored),
            source_datetime_hint=hint_text,
        )
    if len(mentioned_datetime_fields) > 1:
        return TemporalResolution(
            requested_time_grain=grain,
            source_datetime_field=None,
            derived_grouping_dimension=None,
            temporal_plan_status="ambiguous_datetime_source",
            candidate_datetime_fields=list(datetime_candidates),
            scored_datetime_candidates=list(scored),
            source_datetime_hint=hint_text,
            fallback_reason="ambiguous_datetime_source",
        )

    ranked = sorted(
        scored,
        key=lambda item: (-float(item.get("score", 0.0) or 0.0), len(str(item.get("column") or ""))),
    )
    top = ranked[0] if ranked else None
    second = ranked[1] if len(ranked) > 1 else None
    if not top:
        return TemporalResolution(
            requested_time_grain=grain,
            source_datetime_field=None,
            derived_grouping_dimension=None,
            temporal_plan_status="no_datetime_source",
            candidate_datetime_fields=list(datetime_candidates),
            scored_datetime_candidates=list(scored),
            source_datetime_hint=hint_text,
            fallback_reason="no_datetime_source",
        )
    top_score = float(top.get("score", 0.0) or 0.0)
    second_score = float(second.get("score", 0.0) or 0.0) if second else None
    if top_score < float(min_confidence):
        return TemporalResolution(
            requested_time_grain=grain,
            source_datetime_field=None,
            derived_grouping_dimension=None,
            temporal_plan_status="ambiguous_datetime_source",
            candidate_datetime_fields=list(datetime_candidates),
            scored_datetime_candidates=list(ranked),
            source_datetime_hint=hint_text,
            fallback_reason="ambiguous_datetime_source",
        )
    if second_score is not None and (top_score - second_score) < float(ambiguity_gap):
        return TemporalResolution(
            requested_time_grain=grain,
            source_datetime_field=None,
            derived_grouping_dimension=None,
            temporal_plan_status="ambiguous_datetime_source",
            candidate_datetime_fields=list(datetime_candidates),
            scored_datetime_candidates=list(ranked),
            source_datetime_hint=hint_text,
            fallback_reason="ambiguous_datetime_source",
        )

    source_column = str(top.get("column") or "")
    return TemporalResolution(
        requested_time_grain=grain,
        source_datetime_field=source_column or None,
        derived_grouping_dimension=f"{grain}({source_column})" if source_column else None,
        temporal_plan_status="resolved" if source_column else "no_datetime_source",
        candidate_datetime_fields=list(datetime_candidates),
        scored_datetime_candidates=list(ranked),
        source_datetime_hint=hint_text,
        fallback_reason="none" if source_column else "no_datetime_source",
    )


def build_temporal_bucket_expression(
    *,
    datetime_sql_expr: str,
    requested_time_grain: str,
) -> Dict[str, str]:
    grain = str(requested_time_grain or "").strip().lower()
    timestamp_expr = f"TRY_CAST({datetime_sql_expr} AS TIMESTAMP)"
    if grain == "day":
        bucket_expr = f"strftime({timestamp_expr}, '%Y-%m-%d')"
        derived = "day"
    elif grain == "week":
        bucket_expr = f"strftime(date_trunc('week', {timestamp_expr}), '%Y-%m-%d')"
        derived = "week"
    elif grain == "month":
        bucket_expr = f"strftime({timestamp_expr}, '%Y-%m')"
        derived = "month"
    elif grain == "quarter":
        bucket_expr = (
            f"concat(strftime({timestamp_expr}, '%Y'), '-Q', CAST(quarter({timestamp_expr}) AS VARCHAR))"
        )
        derived = "quarter"
    else:
        bucket_expr = f"strftime({timestamp_expr}, '%Y')"
        derived = "year"
    return {
        "bucket_expr": bucket_expr,
        "where_clause": f"WHERE {timestamp_expr} IS NOT NULL",
        "order_by": "ORDER BY bucket ASC",
        "derived_time_grain": derived,
    }


def _token_overlap_score(query_tokens: Sequence[str], candidate_tokens: Sequence[str]) -> float:
    left = {token for token in query_tokens if token}
    right = {token for token in candidate_tokens if token}
    if not left or not right:
        return 0.0
    overlap = left.intersection(right)
    if not overlap:
        return 0.0
    return float(len(overlap) / max(1, len(left)))


def resolve_temporal_measure_column(
    *,
    query: str,
    table: Any,
    requested_metric_text: Optional[str],
    min_confidence: float = 0.52,
    ambiguity_gap: float = 0.08,
) -> TemporalMeasureResolution:
    query_norm = normalize_text(query)
    query_tokens = query_norm.split()
    aliases = _column_aliases(table)
    metadata = _column_metadata(table)

    if requested_metric_text:
        resolution = resolve_requested_field(
            requested_field_text=requested_metric_text,
            table=table,
            expected_dtype_family="numeric",
        )
        if resolution.status == "matched" and resolution.matched_column:
            matched_column = str(resolution.matched_column)
            alias = str(aliases.get(matched_column, ""))
            column_meta = metadata.get(matched_column, {})
            dtype_family = _dtype_family_from_text(str(column_meta.get("dtype") or ""))
            descriptor = normalize_text(" ".join([matched_column, alias]))
            metric_like = any(token in descriptor for token in _METRIC_NAME_TOKENS)
            if dtype_family == "numeric" or metric_like:
                return TemporalMeasureResolution(
                    status="resolved",
                    measure_column=matched_column,
                    candidate_columns=list(resolution.candidate_columns),
                    scored_candidates=list(resolution.scored_candidates),
                )
        if resolution.status == "ambiguous":
            return TemporalMeasureResolution(
                status="ambiguous",
                measure_column=None,
                candidate_columns=list(resolution.candidate_columns),
                scored_candidates=list(resolution.scored_candidates),
                fallback_reason="ambiguous_measure_column",
            )

    scored: List[Dict[str, Any]] = []
    for raw_column in list(getattr(table, "columns", []) or []):
        column = str(raw_column)
        alias = str(aliases.get(column, ""))
        column_meta = metadata.get(column, {})
        dtype_family = _dtype_family_from_text(str(column_meta.get("dtype") or ""))
        column_norm = normalize_text(column)
        alias_norm = normalize_text(alias)
        descriptor = " ".join([column_norm, alias_norm]).strip()
        score = 0.0
        reasons: List[str] = []
        if dtype_family == "numeric":
            score += 0.52
            reasons.append("dtype_numeric")
        if any(token in descriptor for token in _METRIC_NAME_TOKENS):
            score += 0.22
            reasons.append("name_metric_signal")
        query_has_metric_hint = any(token in query_tokens for token in _METRIC_NAME_TOKENS)
        descriptor_has_metric_hint = any(token in descriptor for token in _METRIC_NAME_TOKENS)
        if query_has_metric_hint and descriptor_has_metric_hint:
            score += 0.34
            reasons.append("query_metric_semantic_hint")
        overlap = _token_overlap_score(query_tokens, descriptor.split())
        if overlap > 0.0:
            score += min(0.22, 0.22 * overlap)
            reasons.append(f"query_overlap={round(overlap, 3)}")
        if requested_metric_text:
            requested_norm = normalize_text(requested_metric_text)
            if requested_norm and (
                f" {requested_norm} " in f" {column_norm} "
                or f" {requested_norm} " in f" {alias_norm} "
            ):
                score += 0.2
                reasons.append("requested_metric_overlap")
        score = max(0.0, min(1.0, score))
        scored.append(
            {
                "column": column,
                "score": round(score, 6),
                "reasons": reasons,
            }
        )

    ranked = sorted(
        scored,
        key=lambda item: (-float(item.get("score", 0.0) or 0.0), len(str(item.get("column") or ""))),
    )
    candidates = [str(item.get("column") or "") for item in ranked if str(item.get("column") or "").strip()]
    if not ranked:
        return TemporalMeasureResolution(
            status="no_numeric_measure",
            measure_column=None,
            candidate_columns=[],
            scored_candidates=[],
            fallback_reason="no_numeric_measure",
        )

    top = ranked[0]
    second = ranked[1] if len(ranked) > 1 else None
    top_score = float(top.get("score", 0.0) or 0.0)
    second_score = float(second.get("score", 0.0) or 0.0) if second else None
    if top_score < float(min_confidence):
        return TemporalMeasureResolution(
            status="no_numeric_measure",
            measure_column=None,
            candidate_columns=candidates,
            scored_candidates=ranked,
            fallback_reason="no_numeric_measure",
        )
    if second_score is not None and (top_score - second_score) < float(ambiguity_gap):
        return TemporalMeasureResolution(
            status="ambiguous",
            measure_column=None,
            candidate_columns=candidates,
            scored_candidates=ranked,
            fallback_reason="ambiguous_measure_column",
        )

    return TemporalMeasureResolution(
        status="resolved",
        measure_column=str(top.get("column") or "") or None,
        candidate_columns=candidates,
        scored_candidates=ranked,
    )


def build_temporal_aggregation_plan(
    *,
    requested_time_grain: Optional[str],
    source_datetime_field: Optional[str],
    derived_grouping_dimension: Optional[str],
    operation: Optional[str],
    measure_column: Optional[str],
    status: str,
    fallback_reason: str = "none",
) -> Dict[str, Any]:
    return {
        "requested_time_grain": str(requested_time_grain or "") or None,
        "source_datetime_field": str(source_datetime_field or "") or None,
        "derived_grouping_dimension": str(derived_grouping_dimension or "") or None,
        "operation": str(operation or "count"),
        "measure_column": str(measure_column or "") or None,
        "status": str(status or "unknown"),
        "fallback_reason": str(fallback_reason or "none"),
    }

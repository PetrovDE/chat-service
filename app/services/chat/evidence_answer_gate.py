from __future__ import annotations

from dataclasses import dataclass, field
import logging
import re
from typing import Any, Dict, Mapping, Sequence

from app.services.llm.manager import llm_manager

logger = logging.getLogger(__name__)

_FILE_READY_RESOLUTION_STATUSES = {
    "conversation_match",
    "resolved_unique",
    "skipped_explicit_file_ids",
}
_FILE_AWARE_SELECTED_ROUTE_EXCLUDE = {"", "unknown", "general_chat"}
_FILE_AWARE_RETRIEVAL_MODE_EXCLUDE = {"", "unknown", "assistant_direct"}
_DATA_CUE_RE = re.compile(
    r"(?:\b(?:file|files|dataset|table|tables|sheet|sheets|schema|column|columns|field|fields|rows|records|distribution|breakdown)\b|"
    r"(?:\u0444\u0430\u0439\u043b|\u0434\u0430\u043d\u043d|\u0442\u0430\u0431\u043b\u0438\u0446|\u043b\u0438\u0441\u0442|\u0441\u0445\u0435\u043c|\u0441\u0442\u043e\u043b\u0431|\u043a\u043e\u043b\u043e\u043d\u043a|\u0441\u0442\u0440\u043e\u043a|\u0437\u0430\u043f\u0438\u0441|\u0440\u0430\u0441\u043f\u0440\u0435\u0434\u0435\u043b))"
)
_TABULAR_SPECIFIC_CUE_RE = re.compile(
    r"(?:\b(?:schema|column|columns|field|fields|table|tables|sheet|sheets|row|rows|record|records|distribution|breakdown|group by|filter)\b|"
    r"(?:\u0441\u0445\u0435\u043c|\u0441\u0442\u043e\u043b\u0431|\u043a\u043e\u043b\u043e\u043d\u043a|\u0442\u0430\u0431\u043b\u0438\u0446|\u043b\u0438\u0441\u0442|\u0441\u0442\u0440\u043e\u043a|\u0437\u0430\u043f\u0438\u0441|\u0440\u0430\u0441\u043f\u0440\u0435\u0434\u0435\u043b|\u0433\u0440\u0443\u043f))"
)
_GENERAL_CODING_QUERY_RE = re.compile(
    r"(?:\b(?:python|pandas|numpy|matplotlib|seaborn|plotly|snippet|tutorial|example|api|function|class|code|script)\b|"
    r"\b(?:write|generate|provide|show)\s+code\b|"
    r"(?:\u043f\u0438\u0442\u043e\u043d|\u043a\u043e\u0434))"
)
_FILE_ACCESS_LIMITATION_RE = re.compile(
    r"(?:\b(?:i|we)\b[^.]{0,120}\b(?:cannot|can't|do not|don't)\b[^.]{0,120}\b(?:access|open|read|view|see|analyze)\b[^.]{0,140}\b(?:file|files|attachment|attachments|attached|dataset|data)\b)|"
    r"(?:\bas an ai\b[^.]{0,200}\b(?:cannot|can't|do not|don't)\b[^.]{0,120}\b(?:access|open|read|view|see)\b)|"
    r"(?:\u043d\u0435\s+\u0438\u043c\u0435\u044e\s+\u0434\u043e\u0441\u0442\u0443\u043f)|"
    r"(?:\u043d\u0435\u0442\s+\u0434\u043e\u0441\u0442\u0443\u043f\u0430)|"
    r"(?:\u043d\u0435\s+\u043c\u043e\u0433\u0443\s+\u043e\u0442\u043a\u0440\u044b\u0442\u044c[^.]{0,80}\u0444\u0430\u0439\u043b)"
)


@dataclass(frozen=True)
class EvidenceGateOutcome:
    response_text: str
    changed: bool
    applied: bool
    mode: str
    reason: str
    debug_updates: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidenceRuntimeState:
    query_is_file_aware: bool
    route_is_file_aware: bool
    file_context_available: bool
    evidence_available: bool
    selected_route: str
    retrieval_mode: str
    execution_route: str
    file_resolution_status: str
    retrieval_hits: int
    followup_context_used: bool
    prior_tabular_intent_reused: bool


def _as_str_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in values:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _response_claims_missing_file_access(response_text: str) -> bool:
    return bool(_FILE_ACCESS_LIMITATION_RE.search(str(response_text or "").strip().lower()))


def _route_likely_file_aware(*, rag_debug: Mapping[str, Any]) -> bool:
    selected_route = str(rag_debug.get("selected_route") or "").strip().lower()
    retrieval_mode = str(rag_debug.get("retrieval_mode") or "").strip().lower()
    execution_route = str(rag_debug.get("execution_route") or "").strip().lower()
    file_resolution_status = str(rag_debug.get("file_resolution_status") or "").strip().lower()
    if execution_route in {"tabular_sql", "complex_analytics"}:
        return True
    if selected_route not in _FILE_AWARE_SELECTED_ROUTE_EXCLUDE:
        return True
    if retrieval_mode not in _FILE_AWARE_RETRIEVAL_MODE_EXCLUDE:
        return True
    if bool(rag_debug.get("followup_context_used", False)) or bool(rag_debug.get("prior_tabular_intent_reused", False)):
        return True
    if file_resolution_status in _FILE_READY_RESOLUTION_STATUSES:
        return bool(_as_str_list(rag_debug.get("resolved_file_ids")) or _as_str_list(rag_debug.get("file_ids")))
    return False


def _query_likely_file_aware(*, query: str, rag_debug: Mapping[str, Any]) -> bool:
    text = str(query or "").strip().lower()
    if not text:
        return False
    coding_signal = bool(_GENERAL_CODING_QUERY_RE.search(text))
    data_signal = bool(_DATA_CUE_RE.search(text))
    tabular_signal = bool(_TABULAR_SPECIFIC_CUE_RE.search(text))
    route_file_aware = _route_likely_file_aware(rag_debug=rag_debug)
    if coding_signal and not data_signal and not route_file_aware:
        return False
    if coding_signal and data_signal and not tabular_signal and not route_file_aware:
        return False
    return bool(data_signal or tabular_signal or route_file_aware)


def _file_context_available(*, rag_debug: Mapping[str, Any], context_docs: Sequence[Mapping[str, Any]]) -> bool:
    if context_docs:
        return True
    file_resolution_status = str(rag_debug.get("file_resolution_status") or "").strip().lower()
    retrieval_hits = _safe_int(rag_debug.get("retrieval_hits", rag_debug.get("retrieved_chunks_total", 0)), default=0)
    if retrieval_hits > 0:
        return True
    if _as_str_list(rag_debug.get("file_ids")):
        return True
    if file_resolution_status in _FILE_READY_RESOLUTION_STATUSES:
        return bool(_as_str_list(rag_debug.get("resolved_file_ids")))
    return False


def _evidence_available(
    *,
    rag_debug: Mapping[str, Any],
    context_docs: Sequence[Mapping[str, Any]],
    rag_sources: Sequence[str],
) -> bool:
    if _as_str_list(rag_debug.get("unmatched_requested_fields")):
        return False
    if context_docs:
        return True
    if _safe_int(rag_debug.get("retrieval_hits", rag_debug.get("retrieved_chunks_total", 0)), default=0) > 0:
        return True
    if _safe_int(rag_debug.get("rows_retrieved_total", 0), default=0) > 0:
        return True
    if _as_str_list(rag_debug.get("matched_columns")):
        return True
    if list(rag_sources or []):
        return True
    selected_route = str(rag_debug.get("selected_route") or "").strip().lower()
    if selected_route == "schema_question":
        return bool(_as_str_list(rag_debug.get("resolved_file_ids") or rag_debug.get("file_ids")))
    return False


def _recent_user_turns(*, generation_kwargs: Mapping[str, Any], limit: int = 3) -> list[str]:
    history = generation_kwargs.get("conversation_history")
    if not isinstance(history, list):
        return []
    user_turns: list[str] = []
    for item in history:
        if not isinstance(item, Mapping):
            continue
        if str(item.get("role") or "").strip().lower() != "user":
            continue
        content = str(item.get("content") or "").strip()
        if content:
            user_turns.append(content)
    return user_turns[-limit:]


def _build_sources_preview(rag_sources: Sequence[str], *, max_items: int = 6) -> str:
    items = [str(item or "").strip() for item in list(rag_sources or []) if str(item or "").strip()]
    return "none" if not items else "; ".join(items[:max_items])


def _build_context_preview(
    *,
    context_docs: Sequence[Mapping[str, Any]],
    max_items: int = 4,
    max_chars_per_item: int = 260,
) -> str:
    lines: list[str] = []
    for idx, raw_doc in enumerate(list(context_docs or [])[:max_items], start=1):
        if not isinstance(raw_doc, Mapping):
            continue
        metadata = raw_doc.get("metadata")
        metadata_dict = metadata if isinstance(metadata, Mapping) else {}
        source_name = str(metadata_dict.get("filename") or metadata_dict.get("source") or metadata_dict.get("file_name") or f"doc_{idx}").strip()
        content = str(raw_doc.get("content") or raw_doc.get("text") or "").strip().replace("\n", " ")
        if not content:
            continue
        if len(content) > max_chars_per_item:
            content = content[: max_chars_per_item - 3].rstrip() + "..."
        lines.append(f"- {source_name}: {content}")
    return "none" if not lines else "\n".join(lines)


def _build_runtime_state(
    *,
    query: str,
    rag_debug: Mapping[str, Any],
    context_docs: Sequence[Mapping[str, Any]],
    rag_sources: Sequence[str],
) -> EvidenceRuntimeState:
    selected_route = str(rag_debug.get("selected_route") or "").strip().lower() or "unknown"
    retrieval_mode = str(rag_debug.get("retrieval_mode") or "").strip().lower() or "unknown"
    execution_route = str(rag_debug.get("execution_route") or "").strip().lower() or "unknown"
    file_resolution_status = str(rag_debug.get("file_resolution_status") or "").strip().lower() or "unknown"
    retrieval_hits = _safe_int(rag_debug.get("retrieval_hits", rag_debug.get("retrieved_chunks_total", 0)), default=0)
    return EvidenceRuntimeState(
        query_is_file_aware=_query_likely_file_aware(query=query, rag_debug=rag_debug),
        route_is_file_aware=_route_likely_file_aware(rag_debug=rag_debug),
        file_context_available=_file_context_available(rag_debug=rag_debug, context_docs=context_docs),
        evidence_available=_evidence_available(rag_debug=rag_debug, context_docs=context_docs, rag_sources=rag_sources),
        selected_route=selected_route,
        retrieval_mode=retrieval_mode,
        execution_route=execution_route,
        file_resolution_status=file_resolution_status,
        retrieval_hits=retrieval_hits,
        followup_context_used=bool(rag_debug.get("followup_context_used", False)),
        prior_tabular_intent_reused=bool(rag_debug.get("prior_tabular_intent_reused", False)),
    )


def should_buffer_file_aware_stream_output(
    *,
    query: str,
    rag_debug: Mapping[str, Any] | None,
    context_docs: Sequence[Mapping[str, Any]] | None,
    rag_sources: Sequence[str] | None = None,
) -> bool:
    state = _build_runtime_state(
        query=query,
        rag_debug=dict(rag_debug or {}),
        context_docs=list(context_docs or []),
        rag_sources=list(rag_sources or []),
    )
    return bool(state.query_is_file_aware and state.route_is_file_aware and state.file_context_available)


def _compose_prompt(
    *,
    query: str,
    raw_response: str,
    state: EvidenceRuntimeState,
    rag_debug: Mapping[str, Any],
    context_docs: Sequence[Mapping[str, Any]],
    rag_sources: Sequence[str],
    generation_kwargs: Mapping[str, Any],
) -> str:
    history_preview = "\n".join(f"- {text}" for text in _recent_user_turns(generation_kwargs=generation_kwargs)) or "none"
    resolved_files = _as_str_list(rag_debug.get("resolved_file_ids") or rag_debug.get("file_ids"))
    matched_columns = _as_str_list(rag_debug.get("matched_columns"))
    unmatched_fields = _as_str_list(rag_debug.get("unmatched_requested_fields"))
    return (
        "Evidence-grounded answer composition task.\n"
        "Rewrite the draft into a final answer grounded in runtime state.\n\n"
        f"Current user question:\n{query}\n\n"
        "Follow-up continuity context:\n"
        f"- followup_context_used: {state.followup_context_used}\n"
        f"- prior_tabular_intent_reused: {state.prior_tabular_intent_reused}\n"
        f"- recent_user_turns:\n{history_preview}\n\n"
        "Runtime route and state:\n"
        f"- selected_route: {state.selected_route}\n"
        f"- retrieval_mode: {state.retrieval_mode}\n"
        f"- execution_route: {state.execution_route}\n"
        f"- file_resolution_status: {state.file_resolution_status}\n"
        f"- file_context_available: {state.file_context_available}\n"
        f"- evidence_available: {state.evidence_available}\n"
        f"- retrieval_hits: {state.retrieval_hits}\n"
        f"- resolved_file_ids: {resolved_files or ['none']}\n"
        f"- matched_columns: {matched_columns or ['none']}\n"
        f"- unmatched_requested_fields: {unmatched_fields or ['none']}\n"
        f"- sources_preview: {_build_sources_preview(rag_sources)}\n\n"
        f"Retrieved context preview:\n{_build_context_preview(context_docs=context_docs)}\n\n"
        f"Draft answer:\n{raw_response}\n\n"
        "Rules:\n"
        "1) Answer the current user question directly.\n"
        "2) Do not claim missing file access when file_context_available is true.\n"
        "3) Use only runtime evidence; do not invent columns or values.\n"
        "4) If evidence is insufficient, state exactly what is missing (column, metric, filter, scope).\n"
        "5) Keep the response concise.\n"
    )


def _missing_evidence_text(*, rag_debug: Mapping[str, Any], state: EvidenceRuntimeState) -> str:
    unmatched_fields = _as_str_list(rag_debug.get("unmatched_requested_fields"))
    matched_columns = _as_str_list(rag_debug.get("matched_columns"))
    requested_time_grain = str(rag_debug.get("requested_time_grain") or "").strip().lower()
    source_datetime_field = str(rag_debug.get("source_datetime_field") or "").strip()
    if unmatched_fields:
        return (
            "I can access the attached files, but I do not have confident evidence for requested field(s): "
            f"{', '.join(unmatched_fields[:4])}. Missing: exact column mapping. "
            f"Closest matched columns: {', '.join(matched_columns[:6]) if matched_columns else 'none'}."
        )
    if requested_time_grain and not source_datetime_field:
        return (
            "I can access the attached files, but this answer needs a datetime source column. "
            f"Missing: the column to use for {requested_time_grain} grouping."
        )
    if not state.evidence_available:
        return (
            "I can access the attached files, but I do not have enough retrieved evidence for this request yet. "
            "Missing: target column, metric, and filter (or timeframe) needed for the answer."
        )
    return (
        "I can access the attached files, but I need one more detail to answer precisely. "
        "Missing: exact target column/metric or the specific filter/scope to apply."
    )


def _clarification_outcome(*, raw_response: str, reason: str, rag_debug: Mapping[str, Any], state: EvidenceRuntimeState) -> EvidenceGateOutcome:
    clarification_text = _missing_evidence_text(rag_debug=rag_debug, state=state)
    return EvidenceGateOutcome(
        response_text=clarification_text,
        changed=clarification_text != raw_response,
        applied=True,
        mode="clarification",
        reason=reason,
        debug_updates={
            "evidence_gate_applied": True,
            "evidence_gate_mode": "clarification",
            "evidence_gate_reason": reason,
            "requires_clarification": True,
            "clarification_prompt": clarification_text,
            "fallback_type": "evidence_guard",
            "fallback_reason": reason,
            "controlled_response_state": "evidence_clarification",
        },
    )


async def _run_grounded_compose(*, generation_kwargs: Mapping[str, Any], compose_prompt: str) -> str:
    compose_kwargs = dict(generation_kwargs)
    compose_kwargs["prompt"] = compose_prompt
    try:
        compose_kwargs["temperature"] = min(float(compose_kwargs.get("temperature", 0.2) or 0.2), 0.3)
    except Exception:
        compose_kwargs["temperature"] = 0.2
    compose_kwargs["max_tokens"] = min(max(256, _safe_int(compose_kwargs.get("max_tokens"), default=1200)), 1200)
    try:
        composed = await llm_manager.generate_response(**compose_kwargs)
    except Exception:
        logger.warning("Evidence-gate compose call failed", exc_info=True)
        return ""
    return str((composed or {}).get("response") or "").strip()


async def _apply_file_aware_compose(
    *,
    query: str,
    raw_response: str,
    state: EvidenceRuntimeState,
    rag_debug: Mapping[str, Any],
    context_docs: Sequence[Mapping[str, Any]],
    rag_sources: Sequence[str],
    generation_kwargs: Mapping[str, Any],
) -> EvidenceGateOutcome:
    if not state.evidence_available:
        return _clarification_outcome(raw_response=raw_response, reason="insufficient_evidence", rag_debug=rag_debug, state=state)
    composed_text = await _run_grounded_compose(
        generation_kwargs=generation_kwargs,
        compose_prompt=_compose_prompt(
            query=query,
            raw_response=raw_response,
            state=state,
            rag_debug=rag_debug,
            context_docs=context_docs,
            rag_sources=rag_sources,
            generation_kwargs=generation_kwargs,
        ),
    )
    if composed_text:
        if state.file_context_available and _response_claims_missing_file_access(composed_text):
            return _clarification_outcome(
                raw_response=raw_response,
                reason="composed_contradictory_file_access_claim",
                rag_debug=rag_debug,
                state=state,
            )
        return EvidenceGateOutcome(
            response_text=composed_text,
            changed=composed_text != raw_response,
            applied=True,
            mode="llm_compose",
            reason="file_aware_evidence_first_compose",
            debug_updates={
                "evidence_gate_applied": True,
                "evidence_gate_mode": "llm_compose",
                "evidence_gate_reason": "file_aware_evidence_first_compose",
            },
        )
    if state.file_context_available and _response_claims_missing_file_access(raw_response):
        return _clarification_outcome(
            raw_response=raw_response,
            reason="contradictory_file_access_claim",
            rag_debug=rag_debug,
            state=state,
        )
    return EvidenceGateOutcome(response_text=raw_response, changed=False, applied=False, mode="none", reason="compose_empty_response")


async def enforce_evidence_grounding(
    *,
    query: str,
    raw_response: str,
    rag_debug: Mapping[str, Any] | None,
    context_docs: Sequence[Mapping[str, Any]] | None,
    rag_sources: Sequence[str] | None,
    generation_kwargs: Mapping[str, Any],
) -> EvidenceGateOutcome:
    response_text = str(raw_response or "").strip()
    if not response_text:
        return EvidenceGateOutcome(response_text=response_text, changed=False, applied=False, mode="none", reason="empty_response")
    rag_debug_payload = dict(rag_debug or {})
    docs = list(context_docs or [])
    sources = list(rag_sources or [])
    state = _build_runtime_state(query=query, rag_debug=rag_debug_payload, context_docs=docs, rag_sources=sources)
    if state.query_is_file_aware and state.route_is_file_aware and state.file_context_available:
        outcome = await _apply_file_aware_compose(
            query=query,
            raw_response=response_text,
            state=state,
            rag_debug=rag_debug_payload,
            context_docs=docs,
            rag_sources=sources,
            generation_kwargs=generation_kwargs,
        )
    elif state.file_context_available and state.query_is_file_aware and _response_claims_missing_file_access(response_text):
        outcome = _clarification_outcome(
            raw_response=response_text,
            reason="contradictory_file_access_claim",
            rag_debug=rag_debug_payload,
            state=state,
        )
    else:
        outcome = EvidenceGateOutcome(
            response_text=response_text,
            changed=False,
            applied=False,
            mode="none",
            reason="no_file_aware_gate_needed",
        )
    if outcome.applied:
        logger.info(
            (
                "evidence_gate_applied mode=%s reason=%s changed=%s query_file_aware=%s "
                "route_file_aware=%s file_context_available=%s evidence_available=%s "
                "selected_route=%s retrieval_mode=%s execution_route=%s file_resolution_status=%s retrieval_hits=%s"
            ),
            outcome.mode,
            outcome.reason,
            outcome.changed,
            state.query_is_file_aware,
            state.route_is_file_aware,
            state.file_context_available,
            state.evidence_available,
            state.selected_route,
            state.retrieval_mode,
            state.execution_route,
            state.file_resolution_status,
            state.retrieval_hits,
        )
    return outcome


def apply_evidence_debug_updates(*, rag_debug: Any, outcome: EvidenceGateOutcome) -> None:
    if isinstance(rag_debug, dict) and isinstance(outcome.debug_updates, dict) and outcome.debug_updates:
        rag_debug.update(outcome.debug_updates)


async def run_evidence_gate(
    *,
    query: str,
    raw_response: str,
    rag_debug: Any,
    context_docs: Sequence[Mapping[str, Any]] | None,
    rag_sources: Sequence[str] | None,
    generation_kwargs: Mapping[str, Any],
) -> EvidenceGateOutcome:
    outcome = await enforce_evidence_grounding(
        query=query,
        raw_response=raw_response,
        rag_debug=rag_debug if isinstance(rag_debug, dict) else None,
        context_docs=context_docs if isinstance(context_docs, list) else [],
        rag_sources=rag_sources if isinstance(rag_sources, list) else [],
        generation_kwargs=generation_kwargs,
    )
    apply_evidence_debug_updates(rag_debug=rag_debug, outcome=outcome)
    return outcome

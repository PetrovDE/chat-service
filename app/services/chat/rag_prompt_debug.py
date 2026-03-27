from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Optional, Tuple

from app.observability.metrics import inc_counter
from app.observability.context import request_id_ctx
from app.services.chat.controlled_debug import annotate_controlled_debug

logger = logging.getLogger(__name__)


def infer_fallback_meta(
    *,
    rag_debug: Optional[Dict[str, Any]],
    resolution_meta: Dict[str, Any],
) -> Tuple[str, str]:
    payload = rag_debug if isinstance(rag_debug, dict) else {}
    file_resolution_status = str(resolution_meta.get("file_resolution_status") or "")
    selected_route = str(payload.get("selected_route") or "")
    retrieval_mode = str(payload.get("retrieval_mode") or "")
    requires_clarification = bool(payload.get("requires_clarification", False))

    if file_resolution_status == "not_found":
        return "unresolved_file_not_found", "file_name_not_found"
    if file_resolution_status == "ambiguous":
        return "ambiguous_file", "multiple_file_matches"
    if file_resolution_status == "no_context_files":
        return "no_context", "no_ready_files_in_chat"
    if selected_route == "unsupported_missing_column" or retrieval_mode == "tabular_sql" and str(
        payload.get("fallback_reason") or ""
    ) == "missing_required_columns":
        return "unsupported_missing_column", "missing_required_columns"
    if retrieval_mode == "narrative_no_retrieval":
        return "retrieval_empty", "no_relevant_chunks"
    if retrieval_mode == "narrative_error":
        return "retrieval_runtime_error", str(payload.get("executor_error_code") or "retrieval_runtime_error")
    if requires_clarification:
        return "clarification", str(payload.get("fallback_reason") or "clarification_required")
    return "none", str(payload.get("fallback_reason") or "none")


def inject_file_resolution_debug(
    *,
    rag_debug: Optional[Dict[str, Any]],
    resolution_meta: Dict[str, Any],
    preferred_lang: str,
    query: str,
    user_id: Optional[uuid.UUID],
    conversation_id: uuid.UUID,
) -> Optional[Dict[str, Any]]:
    if rag_debug is None and not resolution_meta:
        return None
    payload: Dict[str, Any] = dict(rag_debug or {})
    payload["detected_language"] = preferred_lang
    payload["requested_file_names"] = list(resolution_meta.get("requested_file_names") or [])
    payload["resolved_file_names"] = list(resolution_meta.get("resolved_file_names") or [])
    payload["resolved_file_ids"] = list(resolution_meta.get("resolved_file_ids") or [])
    payload["resolved_upload_ids"] = list(resolution_meta.get("resolved_upload_ids") or [])
    payload["resolved_document_ids"] = list(resolution_meta.get("resolved_document_ids") or [])
    payload["file_resolution_status"] = str(resolution_meta.get("file_resolution_status") or "not_requested")
    payload["request_id"] = str(payload.get("request_id") or request_id_ctx.get() or "").strip() or None
    payload["conversation_id"] = str(payload.get("conversation_id") or conversation_id)
    payload["user_id"] = str(payload.get("user_id") or user_id) if user_id is not None else None
    if payload.get("file_id") is None and payload["resolved_file_ids"]:
        payload["file_id"] = str(payload["resolved_file_ids"][0])
    if payload.get("upload_id") is None and payload["resolved_upload_ids"]:
        payload["upload_id"] = str(payload["resolved_upload_ids"][0])
    if payload.get("document_id") is None and payload["resolved_document_ids"]:
        payload["document_id"] = str(payload["resolved_document_ids"][0])
    fallback_type, fallback_reason = infer_fallback_meta(rag_debug=payload, resolution_meta=resolution_meta)
    return annotate_controlled_debug(
        rag_debug=payload,
        query=query,
        user_id=user_id,
        conversation_id=conversation_id,
        detected_language=preferred_lang,
        file_resolution_status=payload["file_resolution_status"],
        resolved_file_ids=payload.get("resolved_file_ids") or payload.get("file_ids") or [],
        fallback_type=fallback_type,
        fallback_reason=fallback_reason,
        selected_route=str(
            payload.get("selected_route")
            or payload.get("retrieval_mode")
            or payload.get("execution_route")
            or "unknown"
        ),
        detected_intent=str(payload.get("detected_intent") or payload.get("intent") or "unknown"),
    )


def log_file_resolution_event(
    *,
    user_id: uuid.UUID,
    conversation_id: uuid.UUID,
    resolution_meta: Dict[str, Any],
) -> None:
    logger.info(
        "file_reference_resolution uid=%s chat_id=%s status=%s requested_file_names=%s resolved_file_names=%s resolved_file_ids=%s detected_language=%s",
        str(user_id),
        str(conversation_id),
        str(resolution_meta.get("file_resolution_status") or "not_requested"),
        ",".join([str(item) for item in (resolution_meta.get("requested_file_names") or [])]),
        ",".join([str(item) for item in (resolution_meta.get("resolved_file_names") or [])]),
        ",".join([str(item) for item in (resolution_meta.get("resolved_file_ids") or [])]),
        str(resolution_meta.get("detected_language") or ""),
    )


def log_planner_decision_payload(payload: Dict[str, Any]) -> None:
    logger.info(
        "Query planner decision: route=%s intent=%s strategy_mode=%s confidence=%.2f requires_clarification=%s reasons=%s",
        payload.get("route"),
        payload.get("intent"),
        payload.get("strategy_mode"),
        float(payload.get("confidence", 0.0) or 0.0),
        bool(payload.get("requires_clarification", False)),
        payload.get("reason_codes") or [],
    )


def log_fallback_cache_event(
    *,
    user_id: Optional[uuid.UUID],
    conversation_id: uuid.UUID,
    rag_debug: Optional[Dict[str, Any]],
) -> None:
    payload = rag_debug if isinstance(rag_debug, dict) else {}
    fallback_type = str(payload.get("fallback_type") or "none")
    fallback_reason = str(payload.get("fallback_reason") or "none")
    cache_hit = bool(payload.get("cache_hit", False))
    cache_version = str(payload.get("cache_key_version") or "unknown")
    selected_route = str(payload.get("selected_route") or "unknown")
    detected_intent = str(payload.get("detected_intent") or payload.get("intent") or "unknown")
    response_language = str(payload.get("response_language") or payload.get("detected_language") or "")
    try:
        inc_counter(
            "rag_controlled_fallback_total",
            fallback_type=fallback_type,
            fallback_reason=fallback_reason,
            selected_route=selected_route,
            detected_intent=detected_intent,
            response_language=response_language or "unknown",
        )
        inc_counter(
            "rag_response_cache_observation_total",
            cache_hit=str(cache_hit).lower(),
            cache_key_version=cache_version,
        )
    except Exception:
        logger.debug("Fallback/cache counter emission failed", exc_info=True)
    logger.info(
        (
            "rag_fallback_cache uid=%s chat_id=%s fallback_type=%s fallback_reason=%s "
            "cache_hit=%s cache_miss=%s cache_key_version=%s response_language=%s "
            "selected_route=%s detected_intent=%s resolved_file_ids=%s"
        ),
        str(user_id) if user_id is not None else "anonymous",
        str(conversation_id),
        fallback_type,
        fallback_reason,
        str(cache_hit).lower(),
        str(bool(payload.get("cache_miss", False))).lower(),
        cache_version,
        response_language,
        selected_route,
        detected_intent,
        ",".join([str(item) for item in (payload.get("resolved_file_ids") or [])]),
    )

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Iterable, List, Optional

from app.services.chat.language import normalize_preferred_response_language

CACHE_KEY_VERSION = "v2-route-lang-fileaware"
_WS_RE = re.compile(r"\s+")


def _normalize_query(value: str) -> str:
    text = str(value or "").strip().lower()
    return _WS_RE.sub(" ", text)


def _normalized_ids(values: Optional[Iterable[Any]]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in values or []:
        candidate = str(item or "").strip()
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        out.append(candidate)
    return sorted(out)


def build_cache_observability(
    *,
    user_id: Optional[Any],
    conversation_id: Optional[Any],
    query: str,
    resolved_file_ids: Optional[Iterable[Any]],
    file_resolution_status: str,
    detected_language: str,
    selected_route: str,
    detected_intent: str,
) -> Dict[str, Any]:
    normalized_query = _normalize_query(query)
    cache_key_payload = {
        "version": CACHE_KEY_VERSION,
        "user_id": str(user_id) if user_id is not None else "anonymous",
        "conversation_id": str(conversation_id) if conversation_id is not None else "none",
        "query_norm": normalized_query,
        "query_hash": hashlib.sha256(normalized_query.encode("utf-8")).hexdigest()[:16],
        "resolved_file_ids": _normalized_ids(resolved_file_ids),
        "file_resolution_status": str(file_resolution_status or "not_requested"),
        "detected_language": normalize_preferred_response_language(detected_language),
        "selected_route": str(selected_route or "unknown"),
        "detected_intent": str(detected_intent or "unknown"),
    }
    cache_key_raw = json.dumps(cache_key_payload, ensure_ascii=False, sort_keys=True)
    cache_key = hashlib.sha256(cache_key_raw.encode("utf-8")).hexdigest()[:24]
    return {
        "cache_hit": False,
        "cache_miss": True,
        "cache_key_version": CACHE_KEY_VERSION,
        "cache_key": cache_key,
        "cache_scope": "response_cache_disabled",
    }


def annotate_controlled_debug(
    *,
    rag_debug: Optional[Dict[str, Any]],
    query: str,
    user_id: Optional[Any],
    conversation_id: Optional[Any],
    detected_language: str,
    file_resolution_status: str,
    resolved_file_ids: Optional[Iterable[Any]],
    fallback_type: str,
    fallback_reason: str,
    selected_route: Optional[str] = None,
    detected_intent: Optional[str] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = dict(rag_debug or {})
    normalized_language = normalize_preferred_response_language(detected_language)

    effective_selected_route = str(
        selected_route
        or payload.get("selected_route")
        or payload.get("retrieval_mode")
        or payload.get("execution_route")
        or "unknown"
    )
    effective_detected_intent = str(
        detected_intent
        or payload.get("detected_intent")
        or payload.get("intent")
        or "unknown"
    )
    effective_resolved_file_ids = _normalized_ids(
        resolved_file_ids if resolved_file_ids is not None else payload.get("resolved_file_ids") or payload.get("file_ids")
    )

    payload["detected_language"] = normalized_language
    payload["response_language"] = normalized_language
    payload["file_resolution_status"] = str(file_resolution_status or payload.get("file_resolution_status") or "not_requested")
    payload["resolved_file_ids"] = effective_resolved_file_ids
    payload["selected_route"] = effective_selected_route
    payload["detected_intent"] = effective_detected_intent
    payload["fallback_type"] = str(fallback_type or payload.get("fallback_type") or "none")
    payload["fallback_reason"] = str(fallback_reason or payload.get("fallback_reason") or "none")
    payload.update(
        build_cache_observability(
            user_id=user_id,
            conversation_id=conversation_id,
            query=query,
            resolved_file_ids=effective_resolved_file_ids,
            file_resolution_status=payload["file_resolution_status"],
            detected_language=normalized_language,
            selected_route=effective_selected_route,
            detected_intent=effective_detected_intent,
        )
    )
    return payload

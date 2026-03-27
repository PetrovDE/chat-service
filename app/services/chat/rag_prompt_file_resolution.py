from __future__ import annotations

import logging
import re
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.chat.controlled_response_composer import (
    build_ambiguous_file_message,
    build_file_not_found_message,
    build_no_context_message as build_no_context_controlled_message,
)

logger = logging.getLogger(__name__)
RagPromptResult = Tuple[str, bool, Optional[Dict[str, Any]], List[Dict[str, Any]], List[str], List[str]]

_QUOTED_FILENAME_TOKEN_RE = re.compile(
    r"[\"'`\u00ab]([^\"'`\u00bb]{1,220}\.[A-Za-z0-9]{1,10})[\"'`\u00bb]"
)
_BARE_FILENAME_TOKEN_RE = re.compile(
    r"(?<![A-Za-z\u0410-\u042f\u0430-\u044f\u0401\u04510-9._\-])([A-Za-z\u0410-\u042f\u0430-\u044f\u0401\u04510-9._\-\[\]()]{1,220}\.[A-Za-z0-9]{1,10})(?![A-Za-z\u0410-\u042f\u0430-\u044f\u0401\u04510-9._\-])"
)
_STORED_PREFIX_RE = re.compile(r"^[0-9a-fA-F\-]{8,}_")


def _normalize_filename_token(value: str) -> str:
    text = str(value or "").strip().strip("`'\"")
    text = text.replace("\\", "/")
    if "/" in text:
        text = text.split("/")[-1]
    text = re.sub(r"\s+", " ", text).strip().lower()
    return text


def _extract_filename_candidates(query: str) -> List[str]:
    candidates: List[str] = []
    seen = set()
    text = str(query or "")
    raw_hits = list(_QUOTED_FILENAME_TOKEN_RE.findall(text))
    raw_hits.extend(_BARE_FILENAME_TOKEN_RE.findall(text))
    for raw in raw_hits:
        value = str(raw or "").strip().strip(".,;:!?)]}\u00bb")
        normalized = _normalize_filename_token(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(value)
    return candidates


def _collect_file_aliases(file_obj: Any) -> List[str]:
    aliases: List[str] = []
    original_filename = str(getattr(file_obj, "original_filename", "") or "").strip()
    stored_filename = str(getattr(file_obj, "stored_filename", "") or "").strip()
    if original_filename:
        aliases.append(original_filename)
    if stored_filename:
        aliases.append(stored_filename)
        aliases.append(_STORED_PREFIX_RE.sub("", stored_filename))

    custom_metadata = getattr(file_obj, "custom_metadata", None)
    if isinstance(custom_metadata, dict):
        for key in ("display_name", "filename", "original_filename", "source_filename"):
            value = str(custom_metadata.get(key) or "").strip()
            if value:
                aliases.append(value)

    out: List[str] = []
    seen = set()
    for alias in aliases:
        normalized = _normalize_filename_token(alias)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _find_candidate_matches(candidate: str, files: Sequence[Any]) -> List[Any]:
    normalized_candidate = _normalize_filename_token(candidate)
    if not normalized_candidate:
        return []
    matched: List[Any] = []
    for file_obj in files:
        aliases = _collect_file_aliases(file_obj)
        if normalized_candidate in aliases:
            matched.append(file_obj)
    return matched


def _deduplicate_files(files: Sequence[Any]) -> List[Any]:
    unique: List[Any] = []
    seen = set()
    for file_obj in files:
        file_id = str(getattr(file_obj, "id", "") or "")
        if not file_id or file_id in seen:
            continue
        seen.add(file_id)
        unique.append(file_obj)
    return unique


def _extract_optional_source_ids(file_obj: Any) -> Tuple[Optional[str], Optional[str]]:
    custom_metadata = getattr(file_obj, "custom_metadata", None)
    if not isinstance(custom_metadata, dict):
        return None, None
    source_payload = custom_metadata.get("source")
    source_dict = source_payload if isinstance(source_payload, dict) else {}
    upload_id = str(custom_metadata.get("upload_id") or source_dict.get("upload_id") or "").strip() or None
    document_id = str(custom_metadata.get("document_id") or source_dict.get("document_id") or "").strip() or None
    return upload_id, document_id


def _collect_optional_source_ids(files: Sequence[Any]) -> Tuple[List[str], List[str]]:
    upload_ids: List[str] = []
    document_ids: List[str] = []
    seen_upload = set()
    seen_document = set()
    for file_obj in files:
        upload_id, document_id = _extract_optional_source_ids(file_obj)
        if upload_id and upload_id not in seen_upload:
            seen_upload.add(upload_id)
            upload_ids.append(upload_id)
        if document_id and document_id not in seen_document:
            seen_document.add(document_id)
            document_ids.append(document_id)
    return upload_ids, document_ids


async def _load_user_ready_files_for_resolution(
    *,
    crud_file_module: Any,
    db: AsyncSession,
    user_id: uuid.UUID,
) -> List[Any]:
    getter = getattr(crud_file_module, "get_user_ready_files_for_resolution", None)
    if callable(getter):
        return list(await getter(db, user_id=user_id, limit=300))
    fallback = getattr(crud_file_module, "get_processed_files", None)
    if callable(fallback):
        return list(await fallback(db, user_id=user_id))
    return []


def _format_match_option(file_obj: Any, *, preferred_lang: str) -> str:
    filename = str(getattr(file_obj, "original_filename", "") or getattr(file_obj, "stored_filename", "") or "unknown")
    file_id = str(getattr(file_obj, "id", ""))
    created_at = getattr(file_obj, "created_at", None)
    created_at_text = str(created_at).strip() if created_at is not None else None
    if preferred_lang == "ru":
        if created_at_text:
            return f"`{filename}` (file_id={file_id}, created_at={created_at_text})"
        return f"`{filename}` (file_id={file_id})"
    if created_at_text:
        return f"`{filename}` (file_id={file_id}, created_at={created_at_text})"
    return f"`{filename}` (file_id={file_id})"


def _build_not_found_message(*, missing_candidates: List[str], preferred_lang: str) -> str:
    return build_file_not_found_message(
        preferred_lang=preferred_lang,
        missing_candidates=missing_candidates,
    )


def _build_ambiguous_message(
    *,
    ambiguous: Dict[str, List[Any]],
    preferred_lang: str,
) -> str:
    ambiguous_options: Dict[str, List[str]] = {}
    for candidate, matches in list(ambiguous.items())[:3]:
        ambiguous_options[candidate] = [
            _format_match_option(file_obj, preferred_lang=preferred_lang)
            for file_obj in matches[:5]
        ]
    return build_ambiguous_file_message(
        preferred_lang=preferred_lang,
        ambiguous_options=ambiguous_options,
    )


def build_no_context_message(*, preferred_lang: str) -> str:
    return build_no_context_controlled_message(
        preferred_lang=preferred_lang,
    )


def _build_file_resolution_clarification_result(
    *,
    prompt: str,
    preferred_lang: str,
    rag_mode: Optional[str],
    top_k: int,
    files: List[Any],
    resolution_meta: Dict[str, Any],
) -> RagPromptResult:
    resolution_status = str(resolution_meta.get("file_resolution_status") or "not_requested")
    controlled_state = {
        "not_found": "file_not_found",
        "ambiguous": "ambiguous_file",
        "no_context_files": "no_context",
    }.get(resolution_status, "clarification")
    rag_debug: Dict[str, Any] = {
        "intent": "file_resolution",
        "retrieval_mode": "file_resolution",
        "execution_route": "clarification",
        "requires_clarification": True,
        "clarification_prompt": prompt,
        "controlled_response_state": controlled_state,
        "executor_attempted": False,
        "executor_status": "not_attempted",
        "executor_error_code": None,
        "artifacts_count": 0,
        "analytical_mode_used": False,
        "rag_mode": rag_mode or "auto",
        "rag_mode_effective": "file_resolution",
        "detected_language": preferred_lang,
        "file_ids": [str(getattr(file_obj, "id", "")) for file_obj in files if getattr(file_obj, "id", None) is not None],
        "retrieval_policy": {
            "mode": "clarification",
            "query_profile": "file_resolution",
            "requested_top_k": top_k,
            "effective_top_k": 0,
            "expected_chunks_total": sum(int(getattr(file_obj, "chunks_count", 0) or 0) for file_obj in files),
            "escalation": {"attempted": False, "applied": False, "reason": "file_resolution_controlled_response"},
            "row_escalation": {
                "attempted": False,
                "applied": False,
                "reason": "file_resolution_controlled_response",
            },
        },
    }
    rag_debug.update(resolution_meta)
    return prompt, False, rag_debug, [], [], []


async def load_conversation_files(
    *,
    crud_file_module,
    db: AsyncSession,
    conversation_id: uuid.UUID,
    user_id: uuid.UUID,
    file_ids: Optional[List[str]],
) -> Optional[List[Any]]:
    try:
        files = await crud_file_module.get_conversation_files(db, conversation_id=conversation_id, user_id=user_id)
        logger.info("Conversation files (completed): %d", len(files))
    except Exception as exc:
        logger.warning("Could not fetch conversation files: %s", exc)
        return None

    if file_ids:
        allowed_ids = {str(x) for x in file_ids}
        files = [file_obj for file_obj in files if str(file_obj.id) in allowed_ids]
        logger.info("Conversation files filtered by payload file_ids: %d", len(files))

    ready_statuses = {"ready", "completed", "partial_success", "partial_failed"}
    eligible: List[Any] = []
    skipped_without_active: List[str] = []
    for file_obj in files:
        if not hasattr(file_obj, "active_processing"):
            eligible.append(file_obj)
            continue
        active_processing = getattr(file_obj, "active_processing", None)
        processing_id = getattr(active_processing, "id", None) if active_processing is not None else None
        processing_status = str(getattr(active_processing, "status", "") or "").lower()
        if processing_id is None or processing_status not in ready_statuses:
            skipped_without_active.append(str(getattr(file_obj, "id", "-")))
            continue
        eligible.append(file_obj)

    if skipped_without_active:
        logger.warning(
            "Skipped files without active ready processing profile: count=%d file_ids=%s",
            len(skipped_without_active),
            ",".join(skipped_without_active),
        )
    return eligible


async def resolve_file_references(
    *,
    crud_file_module: Any,
    db: AsyncSession,
    user_id: uuid.UUID,
    conversation_id: uuid.UUID,
    query: str,
    files: List[Any],
    file_ids: Optional[List[str]],
    preferred_lang: str,
    rag_mode: Optional[str],
    top_k: int,
) -> Tuple[List[Any], Dict[str, Any], Optional[RagPromptResult]]:
    requested_file_names = _extract_filename_candidates(query)
    resolution_meta: Dict[str, Any] = {
        "detected_language": preferred_lang,
        "requested_file_names": requested_file_names,
        "resolved_file_names": [],
        "resolved_file_ids": [],
        "resolved_upload_ids": [],
        "resolved_document_ids": [],
        "file_resolution_status": "not_requested",
    }
    if not requested_file_names:
        return files, resolution_meta, None

    if file_ids:
        resolution_meta["file_resolution_status"] = "skipped_explicit_file_ids"
        return files, resolution_meta, None

    conversation_matches: Dict[str, List[Any]] = {}
    unresolved_candidates: List[str] = []
    for candidate in requested_file_names:
        matches = _find_candidate_matches(candidate, files)
        conversation_matches[candidate] = matches
        if not matches:
            unresolved_candidates.append(candidate)

    if not unresolved_candidates:
        resolved = _deduplicate_files([item for values in conversation_matches.values() for item in values])
        resolved_upload_ids, resolved_document_ids = _collect_optional_source_ids(resolved)
        resolution_meta["file_resolution_status"] = "conversation_match"
        resolution_meta["resolved_file_ids"] = [str(getattr(item, "id")) for item in resolved]
        resolution_meta["resolved_file_names"] = [
            str(getattr(item, "original_filename", "") or getattr(item, "stored_filename", "") or "")
            for item in resolved
        ]
        resolution_meta["resolved_upload_ids"] = resolved_upload_ids
        resolution_meta["resolved_document_ids"] = resolved_document_ids
        return files, resolution_meta, None

    try:
        user_ready_files = await _load_user_ready_files_for_resolution(
            crud_file_module=crud_file_module,
            db=db,
            user_id=user_id,
        )
    except Exception as exc:
        logger.warning("Failed to load user ready files for filename resolution: %s", exc)
        user_ready_files = []

    ambiguous: Dict[str, List[Any]] = {}
    missing: List[str] = []
    unique_additions: List[Any] = []
    for candidate in unresolved_candidates:
        matches = _find_candidate_matches(candidate, user_ready_files)
        matches = _deduplicate_files(matches)
        if not matches:
            missing.append(candidate)
            continue
        if len(matches) > 1:
            ambiguous[candidate] = matches
            continue
        unique_additions.append(matches[0])

    if missing:
        resolution_meta["file_resolution_status"] = "not_found"
        prompt = _build_not_found_message(missing_candidates=missing, preferred_lang=preferred_lang)
        result = _build_file_resolution_clarification_result(
            prompt=prompt,
            preferred_lang=preferred_lang,
            rag_mode=rag_mode,
            top_k=top_k,
            files=files,
            resolution_meta=resolution_meta,
        )
        return files, resolution_meta, result

    if ambiguous:
        resolution_meta["file_resolution_status"] = "ambiguous"
        prompt = _build_ambiguous_message(ambiguous=ambiguous, preferred_lang=preferred_lang)
        result = _build_file_resolution_clarification_result(
            prompt=prompt,
            preferred_lang=preferred_lang,
            rag_mode=rag_mode,
            top_k=top_k,
            files=files,
            resolution_meta=resolution_meta,
        )
        return files, resolution_meta, result

    unique_additions = _deduplicate_files(unique_additions)
    for file_obj in unique_additions:
        file_id = getattr(file_obj, "id", None)
        if file_id is None:
            continue
        try:
            await crud_file_module.add_file_to_conversation(
                db,
                file_id=file_id,
                conversation_id=conversation_id,
                attached_by_user_id=user_id,
            )
        except Exception as exc:
            logger.warning(
                "Failed to auto-attach resolved file file_id=%s chat_id=%s: %s",
                str(file_id),
                str(conversation_id),
                exc,
            )

    merged = _deduplicate_files([*files, *unique_additions])
    resolved_upload_ids, resolved_document_ids = _collect_optional_source_ids(unique_additions)
    resolution_meta["file_resolution_status"] = "resolved_unique"
    resolution_meta["resolved_file_ids"] = [str(getattr(item, "id")) for item in unique_additions if getattr(item, "id", None)]
    resolution_meta["resolved_file_names"] = [
        str(getattr(item, "original_filename", "") or getattr(item, "stored_filename", "") or "")
        for item in unique_additions
    ]
    resolution_meta["resolved_upload_ids"] = resolved_upload_ids
    resolution_meta["resolved_document_ids"] = resolved_document_ids
    return merged, resolution_meta, None

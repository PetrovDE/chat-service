from __future__ import annotations

import logging
import re
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.observability.metrics import inc_counter
from app.crud import crud_file
from app.domain.chat.query_planner import (
    INTENT_TABULAR_COMBINED,
    ROUTE_COMPLEX_ANALYTICS,
    ROUTE_DETERMINISTIC_ANALYTICS,
    plan_query,
)
from app.rag.retriever import rag_retriever
from app.services.chat.complex_analytics import execute_complex_analytics_path
from app.services.chat.controlled_debug import annotate_controlled_debug
from app.services.chat.full_file_analysis import build_full_file_map_reduce_prompt
from app.services.chat.language import (
    apply_language_policy_to_prompt,
    detect_preferred_response_language,
    localized_text,
)
from app.services.chat.postprocess import build_rag_caveats
from app.services.chat.rag_prompt_narrative import run_narrative_retrieval_path
from app.services.chat.rag_prompt_routes import (
    build_clarification_route_result,
    maybe_run_complex_analytics_route,
    maybe_run_deterministic_route,
)
from app.services.chat.tabular_sql import execute_tabular_sql_path

logger = logging.getLogger(__name__)
RagPromptResult = Tuple[str, bool, Optional[Dict[str, Any]], List[Dict[str, Any]], List[str], List[str]]

_QUOTED_FILENAME_TOKEN_RE = re.compile(
    r"[\"'`«]([^\"'`»]{1,220}\.[A-Za-z0-9]{1,10})[\"'`»]"
)
_BARE_FILENAME_TOKEN_RE = re.compile(
    r"(?<![A-Za-zА-Яа-яЁё0-9._\-])([A-Za-zА-Яа-яЁё0-9._\-\[\]()]{1,220}\.[A-Za-z0-9]{1,10})(?![A-Za-zА-Яа-яЁё0-9._\-])"
)
_STORED_PREFIX_RE = re.compile(r"^[0-9a-fA-F\-]{8,}_")


def _resolve_builder_dependencies(
    *,
    full_file_prompt_builder,
    rag_caveats_builder,
    crud_file_module,
    rag_retriever_client,
    query_planner,
):
    return (
        full_file_prompt_builder or build_full_file_map_reduce_prompt,
        rag_caveats_builder or build_rag_caveats,
        crud_file_module or crud_file,
        rag_retriever_client or rag_retriever,
        query_planner or plan_query,
    )


async def _load_conversation_files(
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
            # Test doubles and legacy plain objects without processing relation.
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
        value = str(raw or "").strip().strip(".,;:!?)]}»")
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
    listed = ", ".join([f"`{item}`" for item in missing_candidates[:5]])
    return localized_text(
        preferred_lang=preferred_lang,
        ru=(
            f"Не нашёл файл(ы) {listed} среди ваших обработанных файлов. "
            "Проверьте имя файла или дождитесь завершения обработки."
        ),
        en=(
            f"I could not find file(s) {listed} among your processed files. "
            "Please verify the filename or wait until processing is complete."
        ),
    )


def _build_ambiguous_message(
    *,
    ambiguous: Dict[str, List[Any]],
    preferred_lang: str,
) -> str:
    lines: List[str] = []
    for candidate, matches in list(ambiguous.items())[:3]:
        if preferred_lang == "ru":
            lines.append(f"Для `{candidate}` найдено несколько вариантов:")
        else:
            lines.append(f"Multiple matches were found for `{candidate}`:")
        for idx, file_obj in enumerate(matches[:5], start=1):
            lines.append(f"{idx}. {_format_match_option(file_obj, preferred_lang=preferred_lang)}")
    header = localized_text(
        preferred_lang=preferred_lang,
        ru="Уточните, какой файл использовать:",
        en="Please clarify which file should be used:",
    )
    return "\n".join([header, *lines]).strip()


def _build_no_context_message(*, preferred_lang: str) -> str:
    return localized_text(
        preferred_lang=preferred_lang,
        ru=(
            "В этом чате нет готовых файлов для ответа по данным. "
            "Прикрепите файл к чату или укажите файл по имени, и я продолжу."
        ),
        en=(
            "There are no ready files in this chat for file-based answering. "
            "Attach a file to this chat or reference a filename, and I will continue."
        ),
    )


def _infer_fallback_meta(
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


def _build_file_resolution_clarification_result(
    *,
    prompt: str,
    preferred_lang: str,
    rag_mode: Optional[str],
    top_k: int,
    files: List[Any],
    resolution_meta: Dict[str, Any],
) -> RagPromptResult:
    rag_debug: Dict[str, Any] = {
        "intent": "file_resolution",
        "retrieval_mode": "file_resolution",
        "execution_route": "clarification",
        "requires_clarification": True,
        "clarification_prompt": prompt,
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


async def _resolve_file_references(
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
        resolution_meta["file_resolution_status"] = "conversation_match"
        resolution_meta["resolved_file_ids"] = [str(getattr(item, "id")) for item in resolved]
        resolution_meta["resolved_file_names"] = [
            str(getattr(item, "original_filename", "") or getattr(item, "stored_filename", "") or "")
            for item in resolved
        ]
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
    resolution_meta["file_resolution_status"] = "resolved_unique"
    resolution_meta["resolved_file_ids"] = [str(getattr(item, "id")) for item in unique_additions if getattr(item, "id", None)]
    resolution_meta["resolved_file_names"] = [
        str(getattr(item, "original_filename", "") or getattr(item, "stored_filename", "") or "")
        for item in unique_additions
    ]
    return merged, resolution_meta, None


def _inject_file_resolution_debug(
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
    payload["file_resolution_status"] = str(resolution_meta.get("file_resolution_status") or "not_requested")
    fallback_type, fallback_reason = _infer_fallback_meta(rag_debug=payload, resolution_meta=resolution_meta)
    payload = annotate_controlled_debug(
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
    return payload


def _log_file_resolution_event(
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


def _log_planner_decision_payload(payload: Dict[str, Any]) -> None:
    logger.info(
        "Query planner decision: route=%s intent=%s strategy_mode=%s confidence=%.2f requires_clarification=%s reasons=%s",
        payload.get("route"),
        payload.get("intent"),
        payload.get("strategy_mode"),
        float(payload.get("confidence", 0.0) or 0.0),
        bool(payload.get("requires_clarification", False)),
        payload.get("reason_codes") or [],
    )


def _log_fallback_cache_event(
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
        str(bool(payload.get("cache_miss", True))).lower(),
        cache_version,
        response_language,
        selected_route,
        detected_intent,
        ",".join([str(item) for item in (payload.get("resolved_file_ids") or [])]),
    )


async def _handle_planned_routes(
    *,
    query: str,
    user_id: uuid.UUID,
    conversation_id: uuid.UUID,
    planner_decision: Any,
    planner_decision_payload: Dict[str, Any],
    files: List[Any],
    expected_chunks_total: int,
    rag_mode: Optional[str],
    top_k: int,
    model_source: Optional[str],
    provider_mode: Optional[str],
    model_name: Optional[str],
    preferred_lang: str,
    rag_retriever_client: Any,
) -> Optional[RagPromptResult]:
    if planner_decision.requires_clarification:
        return build_clarification_route_result(
            planner_decision=planner_decision,
            planner_decision_payload=planner_decision_payload,
            files=files,
            rag_mode=rag_mode,
            top_k=top_k,
            preferred_lang=preferred_lang,
        )

    if planner_decision.route == ROUTE_COMPLEX_ANALYTICS:
        return await maybe_run_complex_analytics_route(
            query=query,
            files=files,
            planner_decision_payload=planner_decision_payload,
            expected_chunks_total=expected_chunks_total,
            rag_mode=rag_mode,
            top_k=top_k,
            preferred_lang=preferred_lang,
            model_source=model_source,
            provider_mode=provider_mode,
            model_name=model_name,
            complex_analytics_executor=execute_complex_analytics_path,
        )

    if planner_decision.route == ROUTE_DETERMINISTIC_ANALYTICS:
        return await maybe_run_deterministic_route(
            query=query,
            user_id=user_id,
            conversation_id=conversation_id,
            files=files,
            planner_decision=planner_decision,
            planner_decision_payload=planner_decision_payload,
            expected_chunks_total=expected_chunks_total,
            rag_mode=rag_mode,
            top_k=top_k,
            preferred_lang=preferred_lang,
            tabular_sql_executor=execute_tabular_sql_path,
            rag_retriever_client=rag_retriever_client,
            is_combined_intent=bool(planner_decision.intent == INTENT_TABULAR_COMBINED),
        )
    return None


async def build_rag_prompt(
    *,
    db: AsyncSession,
    user_id: Optional[uuid.UUID],
    conversation_id: uuid.UUID,
    query: str,
    top_k: int = 3,
    file_ids: Optional[List[str]] = None,
    model_source: Optional[str] = None,
    provider_mode: Optional[str] = None,
    model_name: Optional[str] = None,
    rag_mode: Optional[str] = None,
    prompt_max_chars: Optional[int] = None,
    crud_file_module=None,
    rag_retriever_client=None,
    full_file_prompt_builder: Optional[
        Callable[..., Awaitable[Tuple[str, Dict[str, Any]]]]
    ] = None,
    rag_caveats_builder: Optional[
        Callable[..., List[str]]
    ] = None,
    query_planner=None,
):
    preferred_lang = detect_preferred_response_language(query)
    final_prompt = apply_language_policy_to_prompt(prompt=query, preferred_lang=preferred_lang)
    empty_result = (final_prompt, False, None, [], [], [])
    (
        full_file_prompt_builder,
        rag_caveats_builder,
        crud_file_module,
        rag_retriever_client,
        query_planner,
    ) = _resolve_builder_dependencies(
        full_file_prompt_builder=full_file_prompt_builder,
        rag_caveats_builder=rag_caveats_builder,
        crud_file_module=crud_file_module,
        rag_retriever_client=rag_retriever_client,
        query_planner=query_planner,
    )

    if not user_id:
        return empty_result

    files = await _load_conversation_files(
        crud_file_module=crud_file_module,
        db=db,
        conversation_id=conversation_id,
        user_id=user_id,
        file_ids=file_ids,
    )
    if files is None:
        files = []

    files, resolution_meta, file_resolution_result = await _resolve_file_references(
        crud_file_module=crud_file_module,
        db=db,
        user_id=user_id,
        conversation_id=conversation_id,
        query=query,
        files=list(files),
        file_ids=file_ids,
        preferred_lang=preferred_lang,
        rag_mode=rag_mode,
        top_k=top_k,
    )
    _log_file_resolution_event(user_id=user_id, conversation_id=conversation_id, resolution_meta=resolution_meta)
    if file_resolution_result is not None:
        prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = file_resolution_result
        rag_debug = _inject_file_resolution_debug(
            rag_debug=rag_debug,
            resolution_meta=resolution_meta,
            preferred_lang=preferred_lang,
            query=query,
            user_id=user_id,
            conversation_id=conversation_id,
        )
        _log_fallback_cache_event(user_id=user_id, conversation_id=conversation_id, rag_debug=rag_debug)
        return prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources

    if not files:
        no_context_prompt = _build_no_context_message(preferred_lang=preferred_lang)
        no_context_debug: Dict[str, Any] = {
            "intent": "no_context",
            "detected_intent": "no_context",
            "retrieval_mode": "no_context_files",
            "execution_route": "clarification",
            "requires_clarification": True,
            "clarification_prompt": no_context_prompt,
            "executor_attempted": False,
            "executor_status": "not_attempted",
            "executor_error_code": None,
            "artifacts_count": 0,
            "analytical_mode_used": False,
            "rag_mode": rag_mode or "auto",
            "rag_mode_effective": "no_context_files",
            "file_ids": [],
            "selected_route": "no_context",
            "retrieval_policy": {
                "mode": "clarification",
                "query_profile": "no_context",
                "requested_top_k": top_k,
                "effective_top_k": 0,
                "expected_chunks_total": 0,
                "escalation": {"attempted": False, "applied": False, "reason": "no_ready_files_in_chat"},
                "row_escalation": {"attempted": False, "applied": False, "reason": "no_ready_files_in_chat"},
            },
        }
        resolution_meta = dict(resolution_meta or {})
        resolution_meta["file_resolution_status"] = "no_context_files"
        no_context_debug = _inject_file_resolution_debug(
            rag_debug=no_context_debug,
            resolution_meta=resolution_meta,
            preferred_lang=preferred_lang,
            query=query,
            user_id=user_id,
            conversation_id=conversation_id,
        )
        _log_fallback_cache_event(user_id=user_id, conversation_id=conversation_id, rag_debug=no_context_debug)
        return no_context_prompt, False, no_context_debug, [], [], []

    planner_decision = query_planner(query=query, files=files)
    planner_decision_payload = dict(planner_decision.as_dict())
    planner_decision_payload["detected_language"] = preferred_lang
    planner_decision_payload["file_resolution_status"] = resolution_meta.get("file_resolution_status")
    _log_planner_decision_payload(planner_decision_payload)

    expected_chunks_total = sum(int(getattr(file_obj, "chunks_count", 0) or 0) for file_obj in files)
    special_route_result = await _handle_planned_routes(
        query=query,
        user_id=user_id,
        conversation_id=conversation_id,
        planner_decision=planner_decision,
        planner_decision_payload=planner_decision_payload,
        files=files,
        expected_chunks_total=expected_chunks_total,
        rag_mode=rag_mode,
        top_k=top_k,
        model_source=model_source,
        provider_mode=provider_mode,
        model_name=model_name,
        preferred_lang=preferred_lang,
        rag_retriever_client=rag_retriever_client,
    )
    if special_route_result is not None:
        prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = special_route_result
        rag_debug = _inject_file_resolution_debug(
            rag_debug=rag_debug,
            resolution_meta=resolution_meta,
            preferred_lang=preferred_lang,
            query=query,
            user_id=user_id,
            conversation_id=conversation_id,
        )
        _log_fallback_cache_event(user_id=user_id, conversation_id=conversation_id, rag_debug=rag_debug)
        return prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources

    prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = await run_narrative_retrieval_path(
        query=query,
        user_id=user_id,
        conversation_id=conversation_id,
        files=files,
        top_k=top_k,
        rag_mode=rag_mode,
        model_source=model_source,
        model_name=model_name,
        preferred_lang=preferred_lang,
        prompt_max_chars=prompt_max_chars,
        planner_decision_payload=planner_decision_payload,
        rag_retriever_client=rag_retriever_client,
        full_file_prompt_builder=full_file_prompt_builder,
        rag_caveats_builder=rag_caveats_builder,
        initial_final_prompt=final_prompt,
    )
    rag_debug = _inject_file_resolution_debug(
        rag_debug=rag_debug,
        resolution_meta=resolution_meta,
        preferred_lang=preferred_lang,
        query=query,
        user_id=user_id,
        conversation_id=conversation_id,
    )
    _log_fallback_cache_event(user_id=user_id, conversation_id=conversation_id, rag_debug=rag_debug)
    return prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources

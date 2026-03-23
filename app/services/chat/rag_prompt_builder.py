from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.crud import crud_file
from app.domain.chat.query_planner import (
    INTENT_TABULAR_COMBINED,
    ROUTE_COMPLEX_ANALYTICS,
    ROUTE_DETERMINISTIC_ANALYTICS,
    plan_query,
)
from app.rag.retriever import rag_retriever
from app.services.chat.complex_analytics import execute_complex_analytics_path
from app.services.chat.full_file_analysis import build_full_file_map_reduce_prompt
from app.services.chat.language import apply_language_policy_to_prompt, detect_preferred_response_language
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
    files = eligible
    return files


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
        )

    if planner_decision.route == ROUTE_COMPLEX_ANALYTICS:
        return await maybe_run_complex_analytics_route(
            query=query,
            files=files,
            planner_decision_payload=planner_decision_payload,
            expected_chunks_total=expected_chunks_total,
            rag_mode=rag_mode,
            top_k=top_k,
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
    if not files:
        return empty_result

    planner_decision = query_planner(query=query, files=files)
    planner_decision_payload = dict(planner_decision.as_dict())
    _log_planner_decision_payload(planner_decision_payload)

    expected_chunks_total = sum(int(getattr(file_obj, "chunks_count", 0) or 0) for file_obj in files)
    special_route_result = await _handle_planned_routes(
        query=query, user_id=user_id, conversation_id=conversation_id,
        planner_decision=planner_decision, planner_decision_payload=planner_decision_payload,
        files=files, expected_chunks_total=expected_chunks_total, rag_mode=rag_mode, top_k=top_k,
        model_source=model_source, provider_mode=provider_mode, model_name=model_name, preferred_lang=preferred_lang,
        rag_retriever_client=rag_retriever_client,
    )
    if special_route_result is not None:
        return special_route_result

    return await run_narrative_retrieval_path(
        query=query, user_id=user_id, conversation_id=conversation_id, files=files, top_k=top_k,
        rag_mode=rag_mode, model_source=model_source, model_name=model_name, preferred_lang=preferred_lang,
        prompt_max_chars=prompt_max_chars, planner_decision_payload=planner_decision_payload,
        rag_retriever_client=rag_retriever_client, full_file_prompt_builder=full_file_prompt_builder,
        rag_caveats_builder=rag_caveats_builder, initial_final_prompt=final_prompt,
    )

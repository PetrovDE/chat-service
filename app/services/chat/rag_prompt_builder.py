from __future__ import annotations

import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.crud import crud_file
from app.domain.chat.query_planner import (
    INTENT_TABULAR_COMBINED,
    ROUTE_COMPLEX_ANALYTICS,
    ROUTE_DETERMINISTIC_ANALYTICS,
    plan_query,
)
from app.rag.retriever import rag_retriever
from app.services.chat import rag_prompt_debug as prompt_debug
from app.services.chat import rag_prompt_file_resolution as file_resolution
from app.services.chat import rag_prompt_intent as intent_classifier
from app.services.chat.complex_analytics import execute_complex_analytics_path
from app.services.chat.full_file_analysis import build_full_file_map_reduce_prompt
from app.services.chat.language import (
    apply_language_policy_to_prompt,
    detect_preferred_response_language,
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


def _finalize_result_with_resolution_debug(
    *,
    result: RagPromptResult,
    resolution_meta: Dict[str, Any],
    preferred_lang: str,
    query: str,
    user_id: Optional[uuid.UUID],
    conversation_id: uuid.UUID,
) -> RagPromptResult:
    prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = result
    rag_debug = prompt_debug.inject_file_resolution_debug(
        rag_debug=rag_debug,
        resolution_meta=resolution_meta,
        preferred_lang=preferred_lang,
        query=query,
        user_id=user_id,
        conversation_id=conversation_id,
    )
    prompt_debug.log_fallback_cache_event(
        user_id=user_id,
        conversation_id=conversation_id,
        rag_debug=rag_debug,
    )
    return prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources


def _build_general_chat_debug_payload(
    *,
    files: List[Any],
    rag_mode: Optional[str],
    top_k: int,
) -> Dict[str, Any]:
    return {
        "intent": "general_chat",
        "detected_intent": "general_chat",
        "retrieval_mode": "assistant_direct",
        "execution_route": "narrative",
        "requires_clarification": False,
        "executor_attempted": False,
        "executor_status": "not_attempted",
        "executor_error_code": None,
        "artifacts_count": 0,
        "analytical_mode_used": False,
        "rag_mode": rag_mode or "auto",
        "rag_mode_effective": "assistant_direct",
        "file_ids": [str(getattr(file_obj, "id")) for file_obj in files if getattr(file_obj, "id", None) is not None],
        "selected_route": "general_chat",
        "fallback_type": "none",
        "fallback_reason": "none",
        "retrieval_policy": {
            "mode": "assistant_direct",
            "query_profile": "general_chat",
            "requested_top_k": top_k,
            "effective_top_k": 0,
            "expected_chunks_total": 0,
            "escalation": {"attempted": False, "applied": False, "reason": "general_chat_route"},
            "row_escalation": {"attempted": False, "applied": False, "reason": "general_chat_route"},
        },
    }


def _build_no_context_debug_payload(
    *,
    top_level_intent: str,
    no_context_prompt: str,
    rag_mode: Optional[str],
    top_k: int,
) -> Dict[str, Any]:
    return {
        "intent": top_level_intent,
        "detected_intent": top_level_intent,
        "retrieval_mode": "no_context_files",
        "execution_route": "clarification",
        "requires_clarification": True,
        "clarification_prompt": no_context_prompt,
        "controlled_response_state": "no_context",
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
) -> RagPromptResult:
    preferred_lang = detect_preferred_response_language(query)
    final_prompt = apply_language_policy_to_prompt(prompt=query, preferred_lang=preferred_lang)
    empty_result: RagPromptResult = (final_prompt, False, None, [], [], [])
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

    files = await file_resolution.load_conversation_files(
        crud_file_module=crud_file_module,
        db=db,
        conversation_id=conversation_id,
        user_id=user_id,
        file_ids=file_ids,
    )
    if files is None:
        files = []

    files, resolution_meta, file_resolution_result = await file_resolution.resolve_file_references(
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
    prompt_debug.log_file_resolution_event(
        user_id=user_id,
        conversation_id=conversation_id,
        resolution_meta=resolution_meta,
    )
    if file_resolution_result is not None:
        return _finalize_result_with_resolution_debug(
            result=file_resolution_result,
            resolution_meta=resolution_meta,
            preferred_lang=preferred_lang,
            query=query,
            user_id=user_id,
            conversation_id=conversation_id,
        )

    top_level_intent = intent_classifier.classify_top_level_intent(
        query=query,
        resolution_meta=resolution_meta,
    )
    logger.info(
        "Top-level route intent: intent=%s file_count=%d file_resolution_status=%s",
        top_level_intent,
        len(files),
        str(resolution_meta.get("file_resolution_status") or "not_requested"),
    )

    if top_level_intent == "general_chat":
        return _finalize_result_with_resolution_debug(
            result=(
                final_prompt,
                False,
                _build_general_chat_debug_payload(files=files, rag_mode=rag_mode, top_k=top_k),
                [],
                [],
                [],
            ),
            resolution_meta=resolution_meta,
            preferred_lang=preferred_lang,
            query=query,
            user_id=user_id,
            conversation_id=conversation_id,
        )

    if not files:
        no_context_prompt = file_resolution.build_no_context_message(preferred_lang=preferred_lang)
        resolution_meta = dict(resolution_meta or {})
        resolution_meta["file_resolution_status"] = "no_context_files"
        return _finalize_result_with_resolution_debug(
            result=(
                no_context_prompt,
                False,
                _build_no_context_debug_payload(
                    top_level_intent=top_level_intent,
                    no_context_prompt=no_context_prompt,
                    rag_mode=rag_mode,
                    top_k=top_k,
                ),
                [],
                [],
                [],
            ),
            resolution_meta=resolution_meta,
            preferred_lang=preferred_lang,
            query=query,
            user_id=user_id,
            conversation_id=conversation_id,
        )

    planner_decision = query_planner(query=query, files=files)
    planner_decision_payload = dict(planner_decision.as_dict())
    planner_decision_payload["detected_language"] = preferred_lang
    planner_decision_payload["file_resolution_status"] = resolution_meta.get("file_resolution_status")
    prompt_debug.log_planner_decision_payload(planner_decision_payload)

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
        return _finalize_result_with_resolution_debug(
            result=special_route_result,
            resolution_meta=resolution_meta,
            preferred_lang=preferred_lang,
            query=query,
            user_id=user_id,
            conversation_id=conversation_id,
        )

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
    return _finalize_result_with_resolution_debug(
        result=(prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources),
        resolution_meta=resolution_meta,
        preferred_lang=preferred_lang,
        query=query,
        user_id=user_id,
        conversation_id=conversation_id,
    )

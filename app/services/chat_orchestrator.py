from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.crud import crud_conversation, crud_message
from app.db.models import User
from app.schemas import ChatMessage, ChatResponse
from app.services.chat.context import (
    build_conversation_history as _build_conversation_history,
    build_rag_conversation_memory as _build_rag_conversation_memory,
    should_include_assistant_history_for_generation as _should_include_assistant_history_for_generation,
)
from app.services.chat.language import (
    detect_preferred_response_language as _detect_preferred_response_language,
)
from app.services.chat.orchestrator_helpers import (
    build_generation_kwargs as _helper_build_generation_kwargs,
    build_rag_debug_payload as _helper_build_rag_debug_payload,
    clarification_text as _helper_clarification_text,
    default_route_telemetry as _helper_default_route_telemetry,
    execution_telemetry as _helper_execution_telemetry,
    executor_short_circuit_text as _helper_executor_short_circuit_text,
    extract_artifacts as _helper_extract_artifacts,
    planner_requires_clarification as _helper_planner_requires_clarification,
    postprocess_generated_answer as _helper_postprocess_generated_answer,
    resolve_provider_selection as _helper_resolve_provider_selection,
    should_run_critic as _helper_should_run_critic,
)
from app.services.chat.orchestrator_runtime import (
    run_nonstream_chat as _run_nonstream_chat,
    stream_chat_events as _stream_chat_events,
)
from app.services.chat.rag_prompt_builder import build_rag_prompt
from app.services.llm.provider_clients import ProviderRegistry
from app.services.llm.manager import llm_manager

logger = logging.getLogger(__name__)


class ChatOrchestrator:
    @staticmethod
    def _default_route_telemetry(
        *,
        route_mode: str = "policy",
        provider_selected: Optional[str] = None,
        provider_effective: str = "aihub",
        aihub_attempted: bool = False,
    ) -> Dict[str, Any]:
        return _helper_default_route_telemetry(
            route_mode=route_mode,
            provider_selected=provider_selected,
            provider_effective=provider_effective,
            aihub_attempted=aihub_attempted,
        )

    @staticmethod
    def _planner_requires_clarification(ctx: Dict[str, Any]) -> bool:
        return _helper_planner_requires_clarification(ctx)

    @staticmethod
    def _executor_short_circuit_text(ctx: Dict[str, Any]) -> Optional[str]:
        return _helper_executor_short_circuit_text(ctx)

    @staticmethod
    def _execution_telemetry(ctx: Dict[str, Any]) -> Dict[str, Any]:
        return _helper_execution_telemetry(ctx)

    @staticmethod
    def _extract_artifacts(ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
        return _helper_extract_artifacts(ctx)

    @staticmethod
    def _clarification_text(ctx: Dict[str, Any]) -> str:
        return _helper_clarification_text(ctx)

    @staticmethod
    def _build_generation_kwargs(*, chat_data: ChatMessage, ctx: Dict[str, Any]) -> Dict[str, Any]:
        return _helper_build_generation_kwargs(chat_data=chat_data, ctx=ctx)

    @staticmethod
    def _should_run_critic(*, chat_data: ChatMessage, ctx: Dict[str, Any]) -> bool:
        return _helper_should_run_critic(chat_data=chat_data, ctx=ctx)

    async def _postprocess_generated_answer(
        self,
        *,
        chat_data: ChatMessage,
        ctx: Dict[str, Any],
        raw_answer: str,
        include_stream_events: bool = False,
    ) -> Dict[str, Any]:
        return await _helper_postprocess_generated_answer(
            chat_data=chat_data,
            ctx=ctx,
            raw_answer=raw_answer,
            include_stream_events=include_stream_events,
        )

    async def _get_or_create_conversation(
        self,
        *,
        db: AsyncSession,
        chat_data: ChatMessage,
        user_id: Optional[uuid.UUID],
    ):
        if chat_data.conversation_id:
            conversation = await crud_conversation.get(db, id=chat_data.conversation_id)
            if not conversation:
                raise HTTPException(status_code=404, detail="Conversation not found")
            if conversation.user_id != user_id:
                raise HTTPException(status_code=403, detail="Access denied")
            return conversation

        from app.schemas.conversation import ConversationCreate

        default_source = (settings.DEFAULT_MODEL_SOURCE or "aihub").strip().lower() or "aihub"
        selected_source = (chat_data.model_source or default_source).strip().lower()
        effective_source = ProviderRegistry.normalize_source(selected_source)
        effective_model = chat_data.model_name or llm_manager.provider_registry.resolve_chat_model(effective_source, None)

        conv_data = ConversationCreate(
            title=chat_data.message[:100] if len(chat_data.message) <= 100 else chat_data.message[:97] + "...",
            model_source=selected_source,
            model_name=effective_model,
        )
        return await crud_conversation.create_for_user(db=db, obj_in=conv_data, user_id=user_id)

    @staticmethod
    def _resolve_provider_selection(*, chat_data: ChatMessage, conversation: Any) -> Dict[str, Any]:
        return _helper_resolve_provider_selection(chat_data=chat_data, conversation=conversation)

    @staticmethod
    def _log_route_event(
        *,
        route_telemetry: Dict[str, Any],
        execution_telemetry: Dict[str, Any],
        conversation_id: uuid.UUID,
        stream: bool,
    ) -> None:
        logger.info(
            "chat_route_decision conversation_id=%s stream=%s route_mode=%s provider_selected=%s provider_effective=%s model_route=%s fallback_attempted=%s fallback_reason=%s aihub_attempted=%s execution_route=%s executor_attempted=%s executor_status=%s executor_error_code=%s artifacts_count=%s",
            str(conversation_id),
            str(bool(stream)).lower(),
            route_telemetry.get("route_mode"),
            route_telemetry.get("provider_selected"),
            route_telemetry.get("provider_effective"),
            route_telemetry.get("model_route"),
            route_telemetry.get("fallback_attempted"),
            route_telemetry.get("fallback_reason"),
            route_telemetry.get("aihub_attempted"),
            execution_telemetry.get("execution_route"),
            execution_telemetry.get("executor_attempted"),
            execution_telemetry.get("executor_status"),
            execution_telemetry.get("executor_error_code"),
            execution_telemetry.get("artifacts_count"),
        )

    @staticmethod
    def _build_rag_debug_payload(
        *,
        rag_debug: Optional[Dict[str, Any]],
        context_docs,
        rag_sources,
        llm_tokens_used: Optional[int],
        provider_debug: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        return _helper_build_rag_debug_payload(
            rag_debug=rag_debug,
            context_docs=context_docs,
            rag_sources=rag_sources,
            llm_tokens_used=llm_tokens_used,
            provider_debug=provider_debug,
        )

    async def _prepare_request_context(
        self,
        *,
        chat_data: ChatMessage,
        db: AsyncSession,
        user_id: Optional[uuid.UUID],
    ) -> Dict[str, Any]:
        conversation = await self._get_or_create_conversation(db=db, chat_data=chat_data, user_id=user_id)
        conversation_id = conversation.id
        provider_selection = self._resolve_provider_selection(chat_data=chat_data, conversation=conversation)

        await crud_message.create_message(db=db, conversation_id=conversation_id, role="user", content=chat_data.message)
        messages = await crud_message.get_last_messages(
            db,
            conversation_id=conversation_id,
            count=settings.CHAT_HISTORY_MAX_MESSAGES,
        )
        conversation_history = _build_conversation_history(messages)

        final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = await build_rag_prompt(
            db=db,
            user_id=user_id,
            conversation_id=conversation_id,
            query=chat_data.message,
            top_k=8,
            file_ids=chat_data.file_ids,
            model_source=provider_selection["provider_source_selected_raw"],
            provider_mode=provider_selection["provider_mode"],
            model_name=provider_selection["provider_model_effective"],
            rag_mode=chat_data.rag_mode,
            prompt_max_chars=chat_data.prompt_max_chars,
        )

        history_for_generation = conversation_history
        if rag_used:
            include_assistant = _should_include_assistant_history_for_generation(rag_debug)
            history_for_generation = _build_rag_conversation_memory(
                conversation_history,
                max_messages=6,
                include_assistant=include_assistant,
            )

        return {
            "conversation_id": conversation_id,
            **provider_selection,
            "final_prompt": final_prompt,
            "rag_used": rag_used,
            "rag_debug": rag_debug,
            "context_docs": context_docs,
            "rag_caveats": rag_caveats,
            "rag_sources": rag_sources,
            "history_for_generation": history_for_generation,
            "preferred_lang": _detect_preferred_response_language(chat_data.message),
        }

    async def chat_stream(
        self,
        *,
        chat_data: ChatMessage,
        db: AsyncSession,
        current_user: Optional[User],
    ) -> StreamingResponse:
        user_id = current_user.id if current_user else None
        username = current_user.username if current_user else "anonymous"
        logger.info("Chat(stream) from %s", username)

        ctx = await self._prepare_request_context(chat_data=chat_data, db=db, user_id=user_id)
        conversation_id = ctx["conversation_id"]
        assistant_message_id = uuid.uuid4()

        return StreamingResponse(
            _stream_chat_events(
                orchestrator=self,
                chat_data=chat_data,
                db=db,
                ctx=ctx,
                conversation_id=conversation_id,
                assistant_message_id=assistant_message_id,
            ),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
        )

    async def chat(
        self,
        *,
        chat_data: ChatMessage,
        db: AsyncSession,
        current_user: Optional[User],
    ) -> ChatResponse:
        user_id = current_user.id if current_user else None
        username = current_user.username if current_user else "anonymous"
        logger.info("Chat(non-stream) from %s", username)

        ctx = await self._prepare_request_context(chat_data=chat_data, db=db, user_id=user_id)
        conversation_id = ctx["conversation_id"]
        return await _run_nonstream_chat(
            orchestrator=self,
            chat_data=chat_data,
            db=db,
            ctx=ctx,
            conversation_id=conversation_id,
        )


chat_orchestrator = ChatOrchestrator()

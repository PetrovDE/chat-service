from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
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
)
from app.services.chat.language import (
    detect_preferred_response_language as _detect_preferred_response_language,
)
from app.services.chat.postprocess import (
    append_caveats_and_sources as _append_caveats_and_sources,
    enforce_answer_language as _enforce_answer_language,
    run_answer_critic as _run_answer_critic,
)
from app.services.chat.rag_prompt_builder import build_rag_prompt
from app.services.chat.sources_debug import (
    build_standard_rag_debug_payload as _build_standard_rag_debug_payload,
)
from app.services.llm.manager import llm_manager

logger = logging.getLogger(__name__)


class ChatOrchestrator:
    @staticmethod
    def _default_route_telemetry() -> Dict[str, Any]:
        return {
            "model_route": "aihub_primary",
            "fallback_reason": "none",
            "fallback_allowed": False,
            "fallback_policy_version": settings.LLM_FALLBACK_POLICY_VERSION,
        }

    @staticmethod
    def _planner_requires_clarification(ctx: Dict[str, Any]) -> bool:
        rag_debug = ctx.get("rag_debug")
        if not isinstance(rag_debug, dict):
            return False
        if bool(rag_debug.get("requires_clarification", False)):
            return True
        planner_decision = rag_debug.get("planner_decision")
        return bool(isinstance(planner_decision, dict) and planner_decision.get("requires_clarification", False))

    @staticmethod
    def _clarification_text(ctx: Dict[str, Any]) -> str:
        rag_debug = ctx.get("rag_debug")
        if isinstance(rag_debug, dict):
            value = str(rag_debug.get("clarification_prompt") or "").strip()
            if value:
                return value
            planner_decision = rag_debug.get("planner_decision")
            if isinstance(planner_decision, dict):
                value = str(planner_decision.get("clarification_prompt") or "").strip()
                if value:
                    return value
        return str(ctx.get("final_prompt") or "").strip()

    @staticmethod
    def _build_generation_kwargs(*, chat_data: ChatMessage, ctx: Dict[str, Any]) -> Dict[str, Any]:
        sla_tier = str(chat_data.sla_tier or "").strip().lower()
        return {
            "prompt": ctx["final_prompt"],
            "model_source": chat_data.model_source,
            "model_name": chat_data.model_name,
            "temperature": chat_data.temperature or 0.7,
            "max_tokens": chat_data.max_tokens or 2000,
            "conversation_history": ctx["history_for_generation"],
            "prompt_max_chars": chat_data.prompt_max_chars,
            "cannot_wait": bool(chat_data.cannot_wait),
            "sla_critical": sla_tier == "critical",
            "policy_class": chat_data.policy_class,
        }

    @staticmethod
    def _should_run_critic(*, chat_data: ChatMessage, ctx: Dict[str, Any]) -> bool:
        return bool(
            chat_data.summarize
            and ctx["rag_used"]
            and ctx["context_docs"]
            and settings.ENABLE_POST_ANSWER_SUMMARIZE
        )

    async def _postprocess_generated_answer(
        self,
        *,
        chat_data: ChatMessage,
        ctx: Dict[str, Any],
        raw_answer: str,
        include_stream_events: bool = False,
    ) -> Dict[str, Any]:
        answer_text, lang_meta = await _enforce_answer_language(
            answer=raw_answer,
            preferred_lang=ctx["preferred_lang"],
            model_source=chat_data.model_source,
            model_name=chat_data.model_name,
            prompt_max_chars=chat_data.prompt_max_chars,
        )
        refined_answer = answer_text

        if ctx["rag_used"]:
            answer_text = _append_caveats_and_sources(
                answer_text,
                ctx["rag_caveats"],
                ctx["rag_sources"],
                preferred_lang=ctx["preferred_lang"],
            )

        stream_events: List[Dict[str, Any]] = []
        if include_stream_events and lang_meta.get("applied"):
            stream_events.append(
                {"type": "final_refinement", "content": refined_answer, "language_enforced": True}
            )

        summary_text: Optional[str] = None
        critic_meta: Optional[Dict[str, Any]] = None
        if self._should_run_critic(chat_data=chat_data, ctx=ctx):
            summarized_answer, critic_meta = await _run_answer_critic(
                query=chat_data.message,
                answer=answer_text,
                context_documents=ctx["context_docs"],
                model_source=chat_data.model_source,
                model_name=chat_data.model_name,
            )
            if summarized_answer and summarized_answer != answer_text:
                summary_text = summarized_answer
                if include_stream_events:
                    stream_events.append(
                        {"type": "summary", "content": summary_text, "critic": critic_meta}
                    )
            elif include_stream_events:
                stream_events.append({"type": "critic", "critic": critic_meta})

        return {
            "answer_text": answer_text,
            "summary_text": summary_text,
            "lang_meta": lang_meta,
            "critic_meta": critic_meta,
            "stream_events": stream_events,
        }

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

        conv_data = ConversationCreate(
            title=chat_data.message[:100] if len(chat_data.message) <= 100 else chat_data.message[:97] + "...",
            model_source=chat_data.model_source or "aihub",
            model_name=chat_data.model_name or llm_manager.aihub_model,
        )
        return await crud_conversation.create_for_user(db=db, obj_in=conv_data, user_id=user_id)

    @staticmethod
    def _build_rag_debug_payload(
        *,
        rag_debug: Optional[Dict[str, Any]],
        context_docs,
        rag_sources,
        llm_tokens_used: Optional[int],
        provider_debug: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        debug_max_items = 64 if isinstance(rag_debug, dict) and rag_debug.get("retrieval_mode") == "full_file" else 8
        return _build_standard_rag_debug_payload(
            rag_debug=rag_debug,
            context_docs=context_docs,
            rag_sources=rag_sources,
            llm_tokens_used=llm_tokens_used,
            provider_debug=provider_debug,
            max_items=debug_max_items,
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
            model_source=chat_data.model_source,
            model_name=chat_data.model_name,
            rag_mode=chat_data.rag_mode,
            prompt_max_chars=chat_data.prompt_max_chars,
        )

        history_for_generation = conversation_history
        if rag_used:
            history_for_generation = _build_rag_conversation_memory(conversation_history, max_messages=6)

        return {
            "conversation_id": conversation_id,
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

        async def event_stream():
            full_response = ""
            start_time = datetime.utcnow()
            summary_text: Optional[str] = None
            route_telemetry: Dict[str, Any] = self._default_route_telemetry()

            try:
                if self._planner_requires_clarification(ctx):
                    clarification_text = self._clarification_text(ctx)
                    full_response = clarification_text
                    start_payload = {
                        "type": "start",
                        "conversation_id": str(conversation_id),
                        "message_id": str(assistant_message_id),
                        "rag_enabled": ctx["rag_used"],
                        "rag_debug": ctx["rag_debug"],
                        **route_telemetry,
                    }
                    if chat_data.rag_debug:
                        start_payload["rag_debug"] = self._build_rag_debug_payload(
                            rag_debug=ctx["rag_debug"],
                            context_docs=ctx["context_docs"],
                            rag_sources=ctx["rag_sources"],
                            llm_tokens_used=None,
                            provider_debug=None,
                        )
                    try:
                        start_payload_json = json.dumps(start_payload)
                    except ValueError:
                        logger.warning(
                            "RAG start payload is not JSON-serializable; sending reduced debug payload",
                            exc_info=True,
                        )
                        start_payload["rag_debug"] = {"serialization_error": True}
                        start_payload_json = json.dumps(start_payload)
                    yield f"data: {start_payload_json}\n\n"

                    if clarification_text:
                        yield f"data: {json.dumps({'type': 'chunk', 'content': clarification_text})}\n\n"

                    generation_time = (datetime.utcnow() - start_time).total_seconds()
                    await crud_message.create_message(
                        db=db,
                        conversation_id=conversation_id,
                        role="assistant",
                        content=clarification_text,
                        model_name=chat_data.model_name or "planner_clarification",
                        temperature=chat_data.temperature,
                        max_tokens=chat_data.max_tokens,
                        generation_time=generation_time,
                    )
                    yield (
                        "data: "
                        + json.dumps(
                            {
                                "type": "done",
                                "generation_time": generation_time,
                                "rag_used": ctx["rag_used"],
                                "summary_available": False,
                                "caveats": ctx["rag_caveats"],
                                "sources": ctx["rag_sources"],
                                **route_telemetry,
                            }
                        )
                        + "\n\n"
                    )
                    return

                generation_kwargs = self._build_generation_kwargs(chat_data=chat_data, ctx=ctx)
                routed_stream = await llm_manager.create_routed_stream(**generation_kwargs)
                route_telemetry = dict(routed_stream.telemetry.as_dict())

                start_payload = {
                    "type": "start",
                    "conversation_id": str(conversation_id),
                    "message_id": str(assistant_message_id),
                    "rag_enabled": ctx["rag_used"],
                    "rag_debug": ctx["rag_debug"],
                    **route_telemetry,
                }
                if chat_data.rag_debug:
                    start_payload["rag_debug"] = self._build_rag_debug_payload(
                        rag_debug=ctx["rag_debug"],
                        context_docs=ctx["context_docs"],
                        rag_sources=ctx["rag_sources"],
                        llm_tokens_used=None,
                        provider_debug=None,
                    )
                try:
                    start_payload_json = json.dumps(start_payload)
                except ValueError:
                    logger.warning("RAG start payload is not JSON-serializable; sending reduced debug payload", exc_info=True)
                    start_payload["rag_debug"] = {"serialization_error": True}
                    start_payload_json = json.dumps(start_payload)
                yield f"data: {start_payload_json}\n\n"

                async for chunk in routed_stream.stream:
                    full_response += chunk
                    yield f"data: {json.dumps({'type': 'chunk', 'content': chunk})}\n\n"

                route_telemetry = dict(routed_stream.telemetry.as_dict())

                generation_time = (datetime.utcnow() - start_time).total_seconds()
                postprocess = await self._postprocess_generated_answer(
                    chat_data=chat_data,
                    ctx=ctx,
                    raw_answer=full_response,
                    include_stream_events=True,
                )
                full_response = postprocess["answer_text"]
                summary_text = postprocess["summary_text"]
                for event in postprocess["stream_events"]:
                    yield f"data: {json.dumps(event)}\n\n"

                await crud_message.create_message(
                    db=db,
                    conversation_id=conversation_id,
                    role="assistant",
                    content=full_response,
                    model_name=chat_data.model_name or llm_manager.ollama_model,
                    temperature=chat_data.temperature,
                    max_tokens=chat_data.max_tokens,
                    generation_time=generation_time,
                )

                yield (
                    "data: "
                    + json.dumps(
                        {
                            "type": "done",
                            "generation_time": generation_time,
                            "rag_used": ctx["rag_used"],
                            "summary_available": bool(summary_text),
                            "caveats": ctx["rag_caveats"],
                            "sources": ctx["rag_sources"],
                            **route_telemetry,
                        }
                    )
                    + "\n\n"
                )

            except Exception as exc:
                logger.error("Streaming error: %s", exc, exc_info=True)
                error_payload = {
                    "type": "error",
                    "message": str(exc),
                    "error_type": type(exc).__name__,
                    **route_telemetry,
                }
                yield f"data: {json.dumps(error_payload)}\n\n"

        return StreamingResponse(
            event_stream(),
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

        start_time = datetime.utcnow()
        if self._planner_requires_clarification(ctx):
            response_text = self._clarification_text(ctx)
            generation_time = (datetime.utcnow() - start_time).total_seconds()
            assistant_message = await crud_message.create_message(
                db=db,
                conversation_id=conversation_id,
                role="assistant",
                content=response_text,
                model_name=chat_data.model_name or "planner_clarification",
                temperature=chat_data.temperature,
                max_tokens=chat_data.max_tokens,
                generation_time=generation_time,
            )
            rag_debug_payload = None
            if chat_data.rag_debug:
                rag_debug_payload = self._build_rag_debug_payload(
                    rag_debug=ctx["rag_debug"],
                    context_docs=ctx["context_docs"],
                    rag_sources=ctx["rag_sources"],
                    llm_tokens_used=None,
                    provider_debug=None,
                )

            route_telemetry = self._default_route_telemetry()
            return ChatResponse(
                response=response_text,
                conversation_id=conversation_id,
                message_id=assistant_message.id,
                model_used=chat_data.model_name or "planner_clarification",
                model_route=str(route_telemetry.get("model_route", "aihub_primary")),
                fallback_reason=str(route_telemetry.get("fallback_reason", "none")),
                fallback_allowed=bool(route_telemetry.get("fallback_allowed", False)),
                fallback_policy_version=str(
                    route_telemetry.get("fallback_policy_version", settings.LLM_FALLBACK_POLICY_VERSION)
                ),
                tokens_used=None,
                generation_time=generation_time,
                summary=None,
                caveats=ctx["rag_caveats"],
                sources=ctx["rag_sources"],
                rag_debug=rag_debug_payload,
            )

        generation_kwargs = self._build_generation_kwargs(chat_data=chat_data, ctx=ctx)
        result = await llm_manager.generate_response(**generation_kwargs)

        postprocess = await self._postprocess_generated_answer(
            chat_data=chat_data,
            ctx=ctx,
            raw_answer=result.get("response", ""),
            include_stream_events=False,
        )
        result["response"] = postprocess["answer_text"]
        summary_text = postprocess["summary_text"]
        if postprocess["critic_meta"] is not None:
            logger.info("RAG critic(non-stream, summarize=%s): %s", chat_data.summarize, postprocess["critic_meta"])

        generation_time = (datetime.utcnow() - start_time).total_seconds()

        assistant_message = await crud_message.create_message(
            db=db,
            conversation_id=conversation_id,
            role="assistant",
            content=result["response"],
            model_name=result["model"],
            temperature=chat_data.temperature,
            max_tokens=chat_data.max_tokens,
            tokens_used=result.get("tokens_used"),
            generation_time=generation_time,
        )

        rag_debug_payload = None
        if chat_data.rag_debug:
            rag_debug_payload = self._build_rag_debug_payload(
                rag_debug=ctx["rag_debug"],
                context_docs=ctx["context_docs"],
                rag_sources=ctx["rag_sources"],
                llm_tokens_used=result.get("tokens_used"),
                provider_debug=result.get("provider_debug"),
            )

        return ChatResponse(
            response=result["response"],
            conversation_id=conversation_id,
            message_id=assistant_message.id,
            model_used=result["model"],
            model_route=str(result.get("model_route", "aihub_primary")),
            fallback_reason=str(result.get("fallback_reason", "none")),
            fallback_allowed=bool(result.get("fallback_allowed", False)),
            fallback_policy_version=str(result.get("fallback_policy_version", settings.LLM_FALLBACK_POLICY_VERSION)),
            tokens_used=result.get("tokens_used"),
            generation_time=generation_time,
            summary=summary_text,
            caveats=ctx["rag_caveats"],
            sources=ctx["rag_sources"],
            rag_debug=rag_debug_payload,
        )


chat_orchestrator = ChatOrchestrator()

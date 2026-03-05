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
        route = "aihub_primary"
        if provider_effective == "ollama":
            route = "ollama"
        elif provider_effective == "aihub":
            route = "aihub"
        elif provider_effective == "openai":
            route = "openai"
        return {
            "model_route": route,
            "route_mode": route_mode,
            "provider_selected": provider_selected,
            "provider_effective": provider_effective,
            "fallback_reason": None,
            "fallback_allowed": False,
            "fallback_attempted": False,
            "fallback_policy_version": settings.LLM_FALLBACK_POLICY_VERSION,
            "aihub_attempted": bool(aihub_attempted),
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
    def _executor_short_circuit_text(ctx: Dict[str, Any]) -> Optional[str]:
        rag_debug = ctx.get("rag_debug")
        if not isinstance(rag_debug, dict):
            return None
        if not bool(rag_debug.get("short_circuit_response", False)):
            return None
        value = str(rag_debug.get("short_circuit_response_text") or "").strip()
        return value or None

    @staticmethod
    def _execution_telemetry(ctx: Dict[str, Any]) -> Dict[str, Any]:
        rag_debug = ctx.get("rag_debug")
        execution_route = "narrative"
        executor_attempted = False
        executor_status = "not_attempted"
        executor_error_code = None
        artifacts_count = 0

        if isinstance(rag_debug, dict):
            route_value = str(rag_debug.get("execution_route") or "").strip().lower()
            if route_value in {"tabular_sql", "complex_analytics", "narrative", "clarification"}:
                execution_route = route_value
            else:
                retrieval_mode = str(rag_debug.get("retrieval_mode") or "").strip().lower()
                if retrieval_mode.startswith("tabular_sql"):
                    execution_route = "tabular_sql"
                elif retrieval_mode.startswith("complex_analytics"):
                    execution_route = "complex_analytics"
                elif bool(rag_debug.get("requires_clarification", False)):
                    execution_route = "clarification"
            executor_attempted = bool(rag_debug.get("executor_attempted", False))
            executor_status_raw = str(rag_debug.get("executor_status") or "").strip().lower()
            if executor_status_raw in {"success", "error", "timeout", "blocked", "not_attempted"}:
                executor_status = executor_status_raw
            elif executor_attempted:
                executor_status = "success" if execution_route == "complex_analytics" else "not_attempted"
            executor_error_code = rag_debug.get("executor_error_code")
            artifacts_raw = rag_debug.get("artifacts_count")
            if artifacts_raw is None:
                artifacts_raw = len(rag_debug.get("artifacts") or [])
            try:
                artifacts_count = max(0, int(artifacts_raw or 0))
            except Exception:
                artifacts_count = 0

        return {
            "execution_route": execution_route,
            "executor_attempted": executor_attempted,
            "executor_status": executor_status,
            "executor_error_code": executor_error_code,
            "artifacts_count": artifacts_count,
        }

    @staticmethod
    def _extract_artifacts(ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
        rag_debug = ctx.get("rag_debug")
        if not isinstance(rag_debug, dict):
            return []
        raw_artifacts = rag_debug.get("artifacts")
        if not isinstance(raw_artifacts, list):
            return []

        artifacts: List[Dict[str, Any]] = []
        for raw_item in raw_artifacts[:32]:
            if not isinstance(raw_item, dict):
                continue
            item: Dict[str, Any] = {}
            for key in ("name", "path", "url", "kind", "content_type", "column"):
                value = raw_item.get(key)
                if value is None:
                    continue
                item[key] = str(value)
            if item:
                artifacts.append(item)
        return artifacts

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
            "model_source": ctx["provider_source_selected_raw"],
            "provider_mode": ctx["provider_mode"],
            "model_name": ctx["provider_model_effective"],
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
            model_source=ctx["provider_source_selected_raw"],
            provider_mode=ctx["provider_mode"],
            model_name=ctx["provider_model_effective"],
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
                model_source=ctx["provider_source_selected_raw"],
                provider_mode=ctx["provider_mode"],
                model_name=ctx["provider_model_effective"],
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
        default_source = (settings.DEFAULT_MODEL_SOURCE or "aihub").strip().lower() or "aihub"
        request_source_raw = str(chat_data.model_source or "").strip().lower() or None
        conversation_source_raw = str(getattr(conversation, "model_source", "") or "").strip().lower() or None
        selected_source_raw = request_source_raw or conversation_source_raw or default_source
        normalized_source = ProviderRegistry.normalize_source(selected_source_raw)

        request_model = str(chat_data.model_name or "").strip() or None
        conversation_model = str(getattr(conversation, "model_name", "") or "").strip() or None
        selected_model = request_model or conversation_model or llm_manager.provider_registry.resolve_chat_model(
            normalized_source,
            None,
        )

        request_mode_raw = str(chat_data.provider_mode or "").strip().lower() or None
        if normalized_source != "aihub":
            effective_mode = "explicit"
        elif request_mode_raw == "explicit":
            effective_mode = "explicit"
        else:
            effective_mode = "policy"

        return {
            "provider_source_selected_raw": selected_source_raw,
            "provider_source_effective": normalized_source,
            "provider_model_effective": selected_model,
            "provider_mode": effective_mode,
        }

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
            history_for_generation = _build_rag_conversation_memory(conversation_history, max_messages=6)

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

        async def event_stream():
            full_response = ""
            start_time = datetime.utcnow()
            summary_text: Optional[str] = None
            route_telemetry: Dict[str, Any] = self._default_route_telemetry(
                route_mode=ctx["provider_mode"],
                provider_selected=ctx["provider_source_selected_raw"],
                provider_effective=ctx["provider_source_effective"],
                aihub_attempted=False,
            )
            execution_telemetry = self._execution_telemetry(ctx)
            artifacts_payload = self._extract_artifacts(ctx)

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
                        **execution_telemetry,
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
                    self._log_route_event(
                        route_telemetry=route_telemetry,
                        execution_telemetry=execution_telemetry,
                        conversation_id=conversation_id,
                        stream=True,
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
                                "artifacts": artifacts_payload,
                                **route_telemetry,
                                **execution_telemetry,
                            }
                        )
                        + "\n\n"
                    )
                    return

                short_circuit_text = self._executor_short_circuit_text(ctx)
                if short_circuit_text:
                    full_response = short_circuit_text
                    start_payload = {
                        "type": "start",
                        "conversation_id": str(conversation_id),
                        "message_id": str(assistant_message_id),
                        "rag_enabled": ctx["rag_used"],
                        "rag_debug": ctx["rag_debug"],
                        **route_telemetry,
                        **execution_telemetry,
                    }
                    if chat_data.rag_debug:
                        start_payload["rag_debug"] = self._build_rag_debug_payload(
                            rag_debug=ctx["rag_debug"],
                            context_docs=ctx["context_docs"],
                            rag_sources=ctx["rag_sources"],
                            llm_tokens_used=None,
                            provider_debug=None,
                        )
                    yield f"data: {json.dumps(start_payload)}\n\n"
                    yield f"data: {json.dumps({'type': 'chunk', 'content': short_circuit_text})}\n\n"

                    generation_time = (datetime.utcnow() - start_time).total_seconds()
                    await crud_message.create_message(
                        db=db,
                        conversation_id=conversation_id,
                        role="assistant",
                        content=short_circuit_text,
                        model_name="complex_analytics_executor",
                        temperature=chat_data.temperature,
                        max_tokens=chat_data.max_tokens,
                        generation_time=generation_time,
                    )
                    self._log_route_event(
                        route_telemetry=route_telemetry,
                        execution_telemetry=execution_telemetry,
                        conversation_id=conversation_id,
                        stream=True,
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
                                "artifacts": artifacts_payload,
                                **route_telemetry,
                                **execution_telemetry,
                            }
                        )
                        + "\n\n"
                    )
                    return

                generation_kwargs = self._build_generation_kwargs(chat_data=chat_data, ctx=ctx)
                routed_stream = await llm_manager.create_routed_stream(**generation_kwargs)
                route_telemetry = dict(routed_stream.telemetry.as_dict())
                self._log_route_event(
                    route_telemetry=route_telemetry,
                    execution_telemetry=execution_telemetry,
                    conversation_id=conversation_id,
                    stream=True,
                )

                start_payload = {
                    "type": "start",
                    "conversation_id": str(conversation_id),
                    "message_id": str(assistant_message_id),
                    "rag_enabled": ctx["rag_used"],
                    "rag_debug": ctx["rag_debug"],
                    **route_telemetry,
                    **execution_telemetry,
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
                    model_name=ctx["provider_model_effective"],
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
                            "artifacts": artifacts_payload,
                            **route_telemetry,
                            **execution_telemetry,
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
                    **execution_telemetry,
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
        execution_telemetry = self._execution_telemetry(ctx)
        artifacts_payload = self._extract_artifacts(ctx)
        if self._planner_requires_clarification(ctx):
            response_text = self._clarification_text(ctx)
            generation_time = (datetime.utcnow() - start_time).total_seconds()
            assistant_message = await crud_message.create_message(
                db=db,
                conversation_id=conversation_id,
                role="assistant",
                content=response_text,
                model_name=ctx["provider_model_effective"] or "planner_clarification",
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

            route_telemetry = self._default_route_telemetry(
                route_mode=ctx["provider_mode"],
                provider_selected=ctx["provider_source_selected_raw"],
                provider_effective=ctx["provider_source_effective"],
                aihub_attempted=False,
            )
            self._log_route_event(
                route_telemetry=route_telemetry,
                execution_telemetry=execution_telemetry,
                conversation_id=conversation_id,
                stream=False,
            )
            return ChatResponse(
                response=response_text,
                conversation_id=conversation_id,
                message_id=assistant_message.id,
                model_used=ctx["provider_model_effective"] or "planner_clarification",
                model_route=str(route_telemetry.get("model_route", "aihub")),
                route_mode=str(route_telemetry.get("route_mode", "policy")),
                provider_selected=route_telemetry.get("provider_selected"),
                provider_effective=str(route_telemetry.get("provider_effective", "aihub")),
                fallback_reason=route_telemetry.get("fallback_reason"),
                fallback_allowed=bool(route_telemetry.get("fallback_allowed", False)),
                fallback_attempted=bool(route_telemetry.get("fallback_attempted", False)),
                fallback_policy_version=str(
                    route_telemetry.get("fallback_policy_version", settings.LLM_FALLBACK_POLICY_VERSION)
                ),
                aihub_attempted=bool(route_telemetry.get("aihub_attempted", False)),
                execution_route=str(execution_telemetry.get("execution_route", "clarification")),
                executor_attempted=bool(execution_telemetry.get("executor_attempted", False)),
                executor_status=str(execution_telemetry.get("executor_status", "not_attempted")),
                executor_error_code=execution_telemetry.get("executor_error_code"),
                artifacts_count=int(execution_telemetry.get("artifacts_count", 0) or 0),
                tokens_used=None,
                generation_time=generation_time,
                summary=None,
                caveats=ctx["rag_caveats"],
                sources=ctx["rag_sources"],
                artifacts=artifacts_payload or None,
                rag_debug=rag_debug_payload,
            )

        short_circuit_text = self._executor_short_circuit_text(ctx)
        if short_circuit_text:
            generation_time = (datetime.utcnow() - start_time).total_seconds()
            assistant_message = await crud_message.create_message(
                db=db,
                conversation_id=conversation_id,
                role="assistant",
                content=short_circuit_text,
                model_name="complex_analytics_executor",
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
            route_telemetry = self._default_route_telemetry(
                route_mode=ctx["provider_mode"],
                provider_selected=ctx["provider_source_selected_raw"],
                provider_effective=ctx["provider_source_effective"],
                aihub_attempted=False,
            )
            self._log_route_event(
                route_telemetry=route_telemetry,
                execution_telemetry=execution_telemetry,
                conversation_id=conversation_id,
                stream=False,
            )
            return ChatResponse(
                response=short_circuit_text,
                conversation_id=conversation_id,
                message_id=assistant_message.id,
                model_used="complex_analytics_executor",
                model_route=str(route_telemetry.get("model_route", "aihub")),
                route_mode=str(route_telemetry.get("route_mode", "policy")),
                provider_selected=route_telemetry.get("provider_selected"),
                provider_effective=str(route_telemetry.get("provider_effective", "aihub")),
                fallback_reason=route_telemetry.get("fallback_reason"),
                fallback_allowed=bool(route_telemetry.get("fallback_allowed", False)),
                fallback_attempted=bool(route_telemetry.get("fallback_attempted", False)),
                fallback_policy_version=str(
                    route_telemetry.get("fallback_policy_version", settings.LLM_FALLBACK_POLICY_VERSION)
                ),
                aihub_attempted=bool(route_telemetry.get("aihub_attempted", False)),
                execution_route=str(execution_telemetry.get("execution_route", "complex_analytics")),
                executor_attempted=bool(execution_telemetry.get("executor_attempted", False)),
                executor_status=str(execution_telemetry.get("executor_status", "success")),
                executor_error_code=execution_telemetry.get("executor_error_code"),
                artifacts_count=int(execution_telemetry.get("artifacts_count", 0) or 0),
                tokens_used=None,
                generation_time=generation_time,
                summary=None,
                caveats=ctx["rag_caveats"],
                sources=ctx["rag_sources"],
                artifacts=artifacts_payload or None,
                rag_debug=rag_debug_payload,
            )

        generation_kwargs = self._build_generation_kwargs(chat_data=chat_data, ctx=ctx)
        result = await llm_manager.generate_response(**generation_kwargs)
        self._log_route_event(
            route_telemetry=result,
            execution_telemetry=execution_telemetry,
            conversation_id=conversation_id,
            stream=False,
        )

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
            route_mode=str(result.get("route_mode", "policy")),
            provider_selected=result.get("provider_selected"),
            provider_effective=str(result.get("provider_effective", "aihub")),
            fallback_reason=result.get("fallback_reason"),
            fallback_allowed=bool(result.get("fallback_allowed", False)),
            fallback_attempted=bool(result.get("fallback_attempted", False)),
            fallback_policy_version=str(result.get("fallback_policy_version", settings.LLM_FALLBACK_POLICY_VERSION)),
            aihub_attempted=bool(result.get("aihub_attempted", False)),
            execution_route=str(execution_telemetry.get("execution_route", "narrative")),
            executor_attempted=bool(execution_telemetry.get("executor_attempted", False)),
            executor_status=str(execution_telemetry.get("executor_status", "not_attempted")),
            executor_error_code=execution_telemetry.get("executor_error_code"),
            artifacts_count=int(execution_telemetry.get("artifacts_count", 0) or 0),
            tokens_used=result.get("tokens_used"),
            generation_time=generation_time,
            summary=summary_text,
            caveats=ctx["rag_caveats"],
            sources=ctx["rag_sources"],
            artifacts=artifacts_payload or None,
            rag_debug=rag_debug_payload,
        )


chat_orchestrator = ChatOrchestrator()

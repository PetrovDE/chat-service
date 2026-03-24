from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.crud import crud_message
from app.schemas import ChatMessage, ChatResponse
from app.services.chat.language import localized_text
from app.services.llm.manager import llm_manager

logger = logging.getLogger(__name__)
def _build_rag_debug_payload_if_enabled(
    *,
    orchestrator: Any,
    chat_data: ChatMessage,
    ctx: Dict[str, Any],
    llm_tokens_used: Optional[int],
    provider_debug: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if not chat_data.rag_debug:
        return None
    return orchestrator._build_rag_debug_payload(
        rag_debug=ctx["rag_debug"],
        context_docs=ctx["context_docs"],
        rag_sources=ctx["rag_sources"],
        llm_tokens_used=llm_tokens_used,
        provider_debug=provider_debug,
    )
def _build_chat_response(
    *,
    response_text: str,
    conversation_id: uuid.UUID,
    message_id: uuid.UUID,
    model_used: str,
    route_telemetry: Dict[str, Any],
    execution_telemetry: Dict[str, Any],
    generation_time: float,
    rag_caveats: Any,
    rag_sources: Any,
    artifacts_payload: Any,
    rag_debug_payload: Optional[Dict[str, Any]],
    tokens_used: Optional[int] = None,
    summary: Optional[str] = None,
    default_execution_route: str = "narrative",
    default_executor_status: str = "not_attempted",
) -> ChatResponse:
    return ChatResponse(
        response=response_text,
        conversation_id=conversation_id,
        message_id=message_id,
        model_used=model_used,
        model_route=str(route_telemetry.get("model_route", "aihub_primary")),
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
        execution_route=str(execution_telemetry.get("execution_route", default_execution_route)),
        executor_attempted=bool(execution_telemetry.get("executor_attempted", False)),
        executor_status=str(execution_telemetry.get("executor_status", default_executor_status)),
        executor_error_code=execution_telemetry.get("executor_error_code"),
        artifacts_count=int(execution_telemetry.get("artifacts_count", 0) or 0),
        tokens_used=tokens_used,
        generation_time=generation_time,
        summary=summary,
        caveats=rag_caveats,
        sources=rag_sources,
        artifacts=artifacts_payload or None,
        rag_debug=rag_debug_payload,
    )
async def stream_chat_events(
    *,
    orchestrator: Any,
    chat_data: ChatMessage,
    db: AsyncSession,
    ctx: Dict[str, Any],
    conversation_id: uuid.UUID,
    assistant_message_id: uuid.UUID,
) -> AsyncGenerator[str, None]:
    full_response = ""
    start_time = datetime.utcnow()
    summary_text: Optional[str] = None
    route_telemetry: Dict[str, Any] = orchestrator._default_route_telemetry(
        route_mode=ctx["provider_mode"],
        provider_selected=ctx["provider_source_selected_raw"],
        provider_effective=ctx["provider_source_effective"],
        aihub_attempted=False,
    )
    execution_telemetry = orchestrator._execution_telemetry(ctx)
    artifacts_payload = orchestrator._extract_artifacts(ctx)

    try:
        if orchestrator._planner_requires_clarification(ctx):
            clarification_text = orchestrator._clarification_text(ctx)
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
                start_payload["rag_debug"] = orchestrator._build_rag_debug_payload(
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
            orchestrator._log_route_event(
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

        short_circuit_text = orchestrator._executor_short_circuit_text(ctx)
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
                start_payload["rag_debug"] = orchestrator._build_rag_debug_payload(
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
            orchestrator._log_route_event(
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

        generation_kwargs = orchestrator._build_generation_kwargs(chat_data=chat_data, ctx=ctx)
        routed_stream = await llm_manager.create_routed_stream(**generation_kwargs)
        route_telemetry = dict(routed_stream.telemetry.as_dict())
        orchestrator._log_route_event(
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
            start_payload["rag_debug"] = orchestrator._build_rag_debug_payload(
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
        postprocess = await orchestrator._postprocess_generated_answer(
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
        preferred_lang = str(ctx.get("preferred_lang") or "ru")
        controlled_message = localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "Ошибка внутреннего runtime при формировании ответа по текущему контексту файлов. "
                "Повторите запрос."
            ),
            en=(
                "Internal runtime error while building an answer from the current file context. "
                "Please retry the request."
            ),
        )
        error_payload = {
            "type": "error",
            "message": controlled_message,
            "error_type": type(exc).__name__,
            "fallback_type": "orchestrator_runtime_error",
            "fallback_reason": "runtime_exception",
            "response_language": preferred_lang,
            **route_telemetry,
            **execution_telemetry,
        }
        yield f"data: {json.dumps(error_payload)}\n\n"
async def run_nonstream_chat(
    *,
    orchestrator: Any,
    chat_data: ChatMessage,
    db: AsyncSession,
    ctx: Dict[str, Any],
    conversation_id: uuid.UUID,
) -> ChatResponse:
    start_time = datetime.utcnow()
    execution_telemetry = orchestrator._execution_telemetry(ctx)
    artifacts_payload = orchestrator._extract_artifacts(ctx)
    if orchestrator._planner_requires_clarification(ctx):
        response_text = orchestrator._clarification_text(ctx)
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
        rag_debug_payload = _build_rag_debug_payload_if_enabled(
            orchestrator=orchestrator,
            chat_data=chat_data,
            ctx=ctx,
            llm_tokens_used=None,
            provider_debug=None,
        )

        route_telemetry = orchestrator._default_route_telemetry(
            route_mode=ctx["provider_mode"],
            provider_selected=ctx["provider_source_selected_raw"],
            provider_effective=ctx["provider_source_effective"],
            aihub_attempted=False,
        )
        orchestrator._log_route_event(
            route_telemetry=route_telemetry,
            execution_telemetry=execution_telemetry,
            conversation_id=conversation_id,
            stream=False,
        )
        return _build_chat_response(
            response_text=response_text,
            conversation_id=conversation_id,
            message_id=assistant_message.id,
            model_used=ctx["provider_model_effective"] or "planner_clarification",
            route_telemetry=route_telemetry,
            execution_telemetry=execution_telemetry,
            generation_time=generation_time,
            rag_caveats=ctx["rag_caveats"],
            rag_sources=ctx["rag_sources"],
            artifacts_payload=artifacts_payload,
            rag_debug_payload=rag_debug_payload,
            tokens_used=None,
            summary=None,
            default_execution_route="clarification",
            default_executor_status="not_attempted",
        )

    short_circuit_text = orchestrator._executor_short_circuit_text(ctx)
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
        rag_debug_payload = _build_rag_debug_payload_if_enabled(
            orchestrator=orchestrator,
            chat_data=chat_data,
            ctx=ctx,
            llm_tokens_used=None,
            provider_debug=None,
        )
        route_telemetry = orchestrator._default_route_telemetry(
            route_mode=ctx["provider_mode"],
            provider_selected=ctx["provider_source_selected_raw"],
            provider_effective=ctx["provider_source_effective"],
            aihub_attempted=False,
        )
        orchestrator._log_route_event(
            route_telemetry=route_telemetry,
            execution_telemetry=execution_telemetry,
            conversation_id=conversation_id,
            stream=False,
        )
        return _build_chat_response(
            response_text=short_circuit_text,
            conversation_id=conversation_id,
            message_id=assistant_message.id,
            model_used="complex_analytics_executor",
            route_telemetry=route_telemetry,
            execution_telemetry=execution_telemetry,
            generation_time=generation_time,
            rag_caveats=ctx["rag_caveats"],
            rag_sources=ctx["rag_sources"],
            artifacts_payload=artifacts_payload,
            rag_debug_payload=rag_debug_payload,
            tokens_used=None,
            summary=None,
            default_execution_route="complex_analytics",
            default_executor_status="success",
        )

    try:
        generation_kwargs = orchestrator._build_generation_kwargs(chat_data=chat_data, ctx=ctx)
        result = await llm_manager.generate_response(**generation_kwargs)
        orchestrator._log_route_event(
            route_telemetry=result,
            execution_telemetry=execution_telemetry,
            conversation_id=conversation_id,
            stream=False,
        )

        postprocess = await orchestrator._postprocess_generated_answer(
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

        rag_debug_payload = _build_rag_debug_payload_if_enabled(
            orchestrator=orchestrator,
            chat_data=chat_data,
            ctx=ctx,
            llm_tokens_used=result.get("tokens_used"),
            provider_debug=result.get("provider_debug"),
        )

        return _build_chat_response(
            response_text=result["response"],
            conversation_id=conversation_id,
            message_id=assistant_message.id,
            model_used=result["model"],
            route_telemetry=result,
            execution_telemetry=execution_telemetry,
            generation_time=generation_time,
            rag_caveats=ctx["rag_caveats"],
            rag_sources=ctx["rag_sources"],
            artifacts_payload=artifacts_payload,
            rag_debug_payload=rag_debug_payload,
            tokens_used=result.get("tokens_used"),
            summary=summary_text,
            default_execution_route="narrative",
            default_executor_status="not_attempted",
        )
    except Exception as exc:
        logger.error("Non-stream generation error: %s", exc, exc_info=True)
        preferred_lang = str(ctx.get("preferred_lang") or "ru")
        response_text = localized_text(
            preferred_lang=preferred_lang,
            ru=(
                "Ошибка внутреннего runtime при формировании ответа по текущему контексту файлов. "
                "Повторите запрос."
            ),
            en=(
                "Internal runtime error while building an answer from the current file context. "
                "Please retry the request."
            ),
        )
        rag_debug_ctx = ctx.get("rag_debug")
        if isinstance(rag_debug_ctx, dict):
            rag_debug_ctx["fallback_type"] = "orchestrator_runtime_error"
            rag_debug_ctx["fallback_reason"] = "runtime_exception"
            rag_debug_ctx["response_language"] = preferred_lang
            rag_debug_ctx["requires_clarification"] = True
            rag_debug_ctx["clarification_prompt"] = response_text
            rag_debug_ctx["selected_route"] = str(
                rag_debug_ctx.get("selected_route")
                or rag_debug_ctx.get("retrieval_mode")
                or rag_debug_ctx.get("execution_route")
                or "narrative"
            )

        generation_time = (datetime.utcnow() - start_time).total_seconds()
        assistant_message = await crud_message.create_message(
            db=db,
            conversation_id=conversation_id,
            role="assistant",
            content=response_text,
            model_name=ctx["provider_model_effective"] or "orchestrator_fallback",
            temperature=chat_data.temperature,
            max_tokens=chat_data.max_tokens,
            generation_time=generation_time,
        )
        route_telemetry = orchestrator._default_route_telemetry(
            route_mode=ctx["provider_mode"],
            provider_selected=ctx["provider_source_selected_raw"],
            provider_effective=ctx["provider_source_effective"],
            aihub_attempted=False,
        )
        rag_debug_payload = _build_rag_debug_payload_if_enabled(
            orchestrator=orchestrator,
            chat_data=chat_data,
            ctx=ctx,
            llm_tokens_used=None,
            provider_debug={"runtime_error": type(exc).__name__},
        )
        orchestrator._log_route_event(
            route_telemetry=route_telemetry,
            execution_telemetry=execution_telemetry,
            conversation_id=conversation_id,
            stream=False,
        )
        return _build_chat_response(
            response_text=response_text,
            conversation_id=conversation_id,
            message_id=assistant_message.id,
            model_used=ctx["provider_model_effective"] or "orchestrator_fallback",
            route_telemetry=route_telemetry,
            execution_telemetry=execution_telemetry,
            generation_time=generation_time,
            rag_caveats=ctx["rag_caveats"],
            rag_sources=ctx["rag_sources"],
            artifacts_payload=artifacts_payload,
            rag_debug_payload=rag_debug_payload,
            tokens_used=None,
            summary=None,
            default_execution_route="narrative",
            default_executor_status="error",
        )

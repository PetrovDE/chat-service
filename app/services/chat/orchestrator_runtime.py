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
from app.services.chat.controlled_response_composer import build_runtime_error_message
from app.services.chat.evidence_answer_gate import (
    run_evidence_gate,
    should_buffer_file_aware_stream_output,
)
from app.services.chat.orchestrator_stream_payloads import (
    build_stream_contract_fields,
    build_stream_done_payload,
    build_stream_start_payload,
    build_stream_terminal_events,
    safe_stream_payload_json,
)
from app.services.chat.response_contract import (
    build_response_contract,
    normalize_execution_telemetry,
    normalize_route_telemetry,
)
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
    rag_debug_ctx: Optional[Dict[str, Any]],
    rag_debug_payload: Optional[Dict[str, Any]],
    debug_enabled: bool,
    tokens_used: Optional[int] = None,
    summary: Optional[str] = None,
    default_execution_route: str = "narrative",
    default_executor_status: str = "not_attempted",
) -> ChatResponse:
    normalized_route = normalize_route_telemetry(route_telemetry)
    normalized_execution = normalize_execution_telemetry(
        execution_telemetry,
        default_execution_route=default_execution_route,
        default_executor_status=default_executor_status,
    )
    response_contract = build_response_contract(
        rag_debug=rag_debug_ctx,
        execution_telemetry=normalized_execution,
        artifacts=artifacts_payload,
        debug_enabled=debug_enabled,
        debug_included=rag_debug_payload is not None,
    )
    return ChatResponse(
        response=response_text,
        conversation_id=conversation_id,
        message_id=message_id,
        model_used=model_used,
        model_route=str(normalized_route["model_route"]),
        route_mode=str(normalized_route["route_mode"]),
        provider_selected=normalized_route.get("provider_selected"),
        provider_effective=str(normalized_route["provider_effective"]),
        fallback_reason=str(normalized_route["fallback_reason"]),
        fallback_allowed=bool(normalized_route["fallback_allowed"]),
        fallback_attempted=bool(normalized_route["fallback_attempted"]),
        fallback_policy_version=str(
            normalized_route.get("fallback_policy_version", settings.LLM_FALLBACK_POLICY_VERSION)
        ),
        aihub_attempted=bool(normalized_route["aihub_attempted"]),
        execution_route=str(normalized_execution["execution_route"]),
        executor_attempted=bool(normalized_execution["executor_attempted"]),
        executor_status=str(normalized_execution["executor_status"]),
        executor_error_code=normalized_execution.get("executor_error_code"),
        artifacts_count=int(normalized_execution["artifacts_count"]),
        tokens_used=tokens_used,
        generation_time=generation_time,
        summary=summary,
        caveats=rag_caveats,
        sources=rag_sources,
        artifacts=artifacts_payload or None,
        response_contract=response_contract,
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
    rag_debug_ctx = ctx.get("rag_debug") if isinstance(ctx.get("rag_debug"), dict) else None

    try:
        if orchestrator._planner_requires_clarification(ctx):
            clarification_text = orchestrator._clarification_text(ctx)
            for event in await build_stream_terminal_events(
                orchestrator=orchestrator,
                chat_data=chat_data,
                db=db,
                ctx=ctx,
                conversation_id=conversation_id,
                assistant_message_id=assistant_message_id,
                route_telemetry=route_telemetry,
                execution_telemetry=execution_telemetry,
                artifacts_payload=artifacts_payload,
                rag_debug_ctx=rag_debug_ctx,
                response_text=clarification_text,
                model_name=chat_data.model_name or "planner_clarification",
                start_time=start_time,
                default_execution_route="clarification",
                default_executor_status="not_attempted",
                logger=logger,
            ):
                yield event
            return

        short_circuit_text = orchestrator._executor_short_circuit_text(ctx)
        if short_circuit_text:
            short_circuit_model = (
                "complex_analytics_executor"
                if str(execution_telemetry.get("execution_route") or "") == "complex_analytics"
                else "tabular_sql_executor"
            )
            for event in await build_stream_terminal_events(
                orchestrator=orchestrator,
                chat_data=chat_data,
                db=db,
                ctx=ctx,
                conversation_id=conversation_id,
                assistant_message_id=assistant_message_id,
                route_telemetry=route_telemetry,
                execution_telemetry=execution_telemetry,
                artifacts_payload=artifacts_payload,
                rag_debug_ctx=rag_debug_ctx,
                response_text=short_circuit_text,
                model_name=short_circuit_model,
                start_time=start_time,
                default_execution_route="complex_analytics",
                default_executor_status="success",
                logger=logger,
            ):
                yield event
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
        }
        start_debug_payload = None
        if chat_data.rag_debug:
            start_debug_payload = orchestrator._build_rag_debug_payload(
                rag_debug=ctx["rag_debug"],
                context_docs=ctx["context_docs"],
                rag_sources=ctx["rag_sources"],
                llm_tokens_used=None,
                provider_debug=None,
            )
        start_contract_fields = build_stream_contract_fields(
            rag_debug_ctx=rag_debug_ctx,
            route_telemetry=route_telemetry,
            execution_telemetry=execution_telemetry,
            artifacts_payload=artifacts_payload,
            debug_enabled=chat_data.rag_debug,
            debug_included=start_debug_payload is not None,
            default_execution_route="narrative",
            default_executor_status="not_attempted",
        )
        start_payload.update(start_contract_fields)
        if start_debug_payload is not None:
            start_payload["rag_debug"] = start_debug_payload
        start_payload_json = safe_stream_payload_json(start_payload, logger=logger)
        yield f"data: {start_payload_json}\n\n"

        buffer_stream_output = should_buffer_file_aware_stream_output(
            query=chat_data.message,
            rag_debug=ctx.get("rag_debug") if isinstance(ctx.get("rag_debug"), dict) else None,
            context_docs=ctx.get("context_docs") if isinstance(ctx.get("context_docs"), list) else [],
            rag_sources=ctx.get("rag_sources") if isinstance(ctx.get("rag_sources"), list) else [],
        )
        async for chunk in routed_stream.stream:
            full_response += chunk
            if not buffer_stream_output:
                yield f"data: {json.dumps({'type': 'chunk', 'content': chunk})}\n\n"

        route_telemetry = dict(routed_stream.telemetry.as_dict())
        gate_outcome = await run_evidence_gate(
            query=chat_data.message,
            generation_kwargs=generation_kwargs,
            raw_response=full_response,
            rag_debug=ctx.get("rag_debug"),
            context_docs=ctx.get("context_docs"),
            rag_sources=ctx.get("rag_sources"),
        )
        full_response = gate_outcome.response_text

        generation_time = (datetime.utcnow() - start_time).total_seconds()
        postprocess_events: list[dict[str, Any]] = []
        if gate_outcome.mode == "clarification":
            summary_text = None
        else:
            postprocess = await orchestrator._postprocess_generated_answer(
                chat_data=chat_data,
                ctx=ctx,
                raw_answer=full_response,
                include_stream_events=True,
            )
            full_response = postprocess["answer_text"]
            summary_text = postprocess["summary_text"]
            postprocess_events = postprocess["stream_events"]
        if buffer_stream_output and full_response:
            yield f"data: {json.dumps({'type': 'chunk', 'content': full_response, 'evidence_grounded': True})}\n\n"
        for event in postprocess_events:
            if buffer_stream_output and str(event.get("type") or "").strip().lower() == "final_refinement":
                continue
            yield f"data: {json.dumps(event)}\n\n"
        if not buffer_stream_output and gate_outcome.changed:
            yield f"data: {json.dumps({'type': 'final_refinement', 'content': full_response, 'evidence_grounded': True})}\n\n"

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

        done_debug_payload = None
        if chat_data.rag_debug:
            done_debug_payload = orchestrator._build_rag_debug_payload(
                rag_debug=ctx["rag_debug"],
                context_docs=ctx["context_docs"],
                rag_sources=ctx["rag_sources"],
                llm_tokens_used=None,
                provider_debug=None,
            )
        done_contract_fields = build_stream_contract_fields(
            rag_debug_ctx=rag_debug_ctx,
            route_telemetry=route_telemetry,
            execution_telemetry=execution_telemetry,
            artifacts_payload=artifacts_payload,
            debug_enabled=chat_data.rag_debug,
            debug_included=done_debug_payload is not None,
            default_execution_route="narrative",
            default_executor_status="not_attempted",
        )
        done_payload = build_stream_done_payload(
            generation_time=generation_time,
            rag_used=ctx["rag_used"],
            summary_available=bool(summary_text),
            rag_caveats=ctx["rag_caveats"],
            rag_sources=ctx["rag_sources"],
            artifacts_payload=artifacts_payload,
            contract_fields=done_contract_fields,
            rag_debug_payload=done_debug_payload,
        )
        yield "data: " + safe_stream_payload_json(done_payload, logger=logger) + "\n\n"

    except Exception as exc:
        logger.error("Streaming error: %s", exc, exc_info=True)
        preferred_lang = str(ctx.get("preferred_lang") or "ru")
        controlled_message = build_runtime_error_message(
            preferred_lang=preferred_lang,
        )
        error_rag_debug = dict(rag_debug_ctx or {})
        error_rag_debug["controlled_response_state"] = "runtime_error"
        error_rag_debug["fallback_type"] = "orchestrator_runtime_error"
        error_rag_debug["fallback_reason"] = "runtime_exception"
        error_rag_debug["response_language"] = preferred_lang
        error_rag_debug["requires_clarification"] = True
        error_rag_debug["clarification_prompt"] = controlled_message
        error_rag_debug["selected_route"] = str(
            error_rag_debug.get("selected_route")
            or error_rag_debug.get("retrieval_mode")
            or error_rag_debug.get("execution_route")
            or "narrative"
        )
        error_rag_debug_payload = None
        if chat_data.rag_debug:
            error_rag_debug_payload = orchestrator._build_rag_debug_payload(
                rag_debug=error_rag_debug,
                context_docs=ctx["context_docs"],
                rag_sources=ctx["rag_sources"],
                llm_tokens_used=None,
                provider_debug={"runtime_error": type(exc).__name__},
            )
        error_contract_fields = build_stream_contract_fields(
            rag_debug_ctx=error_rag_debug,
            route_telemetry=route_telemetry,
            execution_telemetry=execution_telemetry,
            artifacts_payload=artifacts_payload,
            debug_enabled=chat_data.rag_debug,
            debug_included=error_rag_debug_payload is not None,
            default_execution_route="unknown",
            default_executor_status="error",
        )
        error_payload = {
            "type": "error",
            "message": controlled_message,
            "error_type": type(exc).__name__,
            "controlled_response_state": "runtime_error",
            "fallback_type": "orchestrator_runtime_error",
            "fallback_reason": "runtime_exception",
            "response_language": preferred_lang,
            **error_contract_fields,
        }
        if error_rag_debug_payload is not None:
            error_payload["rag_debug"] = error_rag_debug_payload
        yield f"data: {safe_stream_payload_json(error_payload, logger=logger)}\n\n"


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
            rag_debug_ctx=ctx.get("rag_debug"),
            rag_debug_payload=rag_debug_payload,
            debug_enabled=chat_data.rag_debug,
            tokens_used=None,
            summary=None,
            default_execution_route="clarification",
            default_executor_status="not_attempted",
        )

    short_circuit_text = orchestrator._executor_short_circuit_text(ctx)
    if short_circuit_text:
        short_circuit_model = (
            "complex_analytics_executor"
            if str(execution_telemetry.get("execution_route") or "") == "complex_analytics"
            else "tabular_sql_executor"
        )
        generation_time = (datetime.utcnow() - start_time).total_seconds()
        assistant_message = await crud_message.create_message(
            db=db,
            conversation_id=conversation_id,
            role="assistant",
            content=short_circuit_text,
            model_name=short_circuit_model,
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
            model_used=short_circuit_model,
            route_telemetry=route_telemetry,
            execution_telemetry=execution_telemetry,
            generation_time=generation_time,
            rag_caveats=ctx["rag_caveats"],
            rag_sources=ctx["rag_sources"],
            artifacts_payload=artifacts_payload,
            rag_debug_ctx=ctx.get("rag_debug"),
            rag_debug_payload=rag_debug_payload,
            debug_enabled=chat_data.rag_debug,
            tokens_used=None,
            summary=None,
            default_execution_route="complex_analytics",
            default_executor_status="success",
        )

    try:
        generation_kwargs = orchestrator._build_generation_kwargs(chat_data=chat_data, ctx=ctx)
        result = await llm_manager.generate_response(**generation_kwargs)
        gate_outcome = await run_evidence_gate(
            query=chat_data.message,
            generation_kwargs=generation_kwargs,
            raw_response=result.get("response", ""),
            rag_debug=ctx.get("rag_debug"),
            context_docs=ctx.get("context_docs"),
            rag_sources=ctx.get("rag_sources"),
        )
        result["response"] = gate_outcome.response_text
        orchestrator._log_route_event(
            route_telemetry=result,
            execution_telemetry=execution_telemetry,
            conversation_id=conversation_id,
            stream=False,
        )

        if gate_outcome.mode == "clarification":
            summary_text = None
        else:
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
            rag_debug_ctx=ctx.get("rag_debug"),
            rag_debug_payload=rag_debug_payload,
            debug_enabled=chat_data.rag_debug,
            tokens_used=result.get("tokens_used"),
            summary=summary_text,
            default_execution_route="narrative",
            default_executor_status="not_attempted",
        )
    except Exception as exc:
        logger.error("Non-stream generation error: %s", exc, exc_info=True)
        preferred_lang = str(ctx.get("preferred_lang") or "ru")
        response_text = build_runtime_error_message(
            preferred_lang=preferred_lang,
        )
        rag_debug_ctx = ctx.get("rag_debug")
        if isinstance(rag_debug_ctx, dict):
            rag_debug_ctx["fallback_type"] = "orchestrator_runtime_error"
            rag_debug_ctx["fallback_reason"] = "runtime_exception"
            rag_debug_ctx["controlled_response_state"] = "runtime_error"
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
            rag_debug_ctx=ctx.get("rag_debug"),
            rag_debug_payload=rag_debug_payload,
            debug_enabled=chat_data.rag_debug,
            tokens_used=None,
            summary=None,
            default_execution_route="narrative",
            default_executor_status="error",
        )

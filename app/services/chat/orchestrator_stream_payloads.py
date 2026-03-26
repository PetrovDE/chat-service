from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Dict, Mapping, Optional, Sequence

from sqlalchemy.ext.asyncio import AsyncSession

from app.crud import crud_message
from app.schemas import ChatMessage
from app.services.chat.response_contract import (
    build_response_contract,
    normalize_execution_telemetry,
    normalize_route_telemetry,
)


def build_stream_contract_fields(
    *,
    rag_debug_ctx: Optional[Dict[str, Any]],
    route_telemetry: Dict[str, Any],
    execution_telemetry: Dict[str, Any],
    artifacts_payload: Any,
    debug_enabled: bool,
    debug_included: bool,
    default_execution_route: str = "unknown",
    default_executor_status: str = "not_attempted",
) -> Dict[str, Any]:
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
        debug_included=debug_included,
    )
    return {
        **normalized_route,
        **normalized_execution,
        "response_contract": response_contract,
    }


def build_stream_start_payload(
    *,
    conversation_id: uuid.UUID,
    assistant_message_id: uuid.UUID,
    rag_enabled: bool,
    contract_fields: Dict[str, Any],
    rag_debug_payload: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "type": "start",
        "conversation_id": str(conversation_id),
        "message_id": str(assistant_message_id),
        "rag_enabled": rag_enabled,
        **contract_fields,
    }
    if rag_debug_payload is not None:
        payload["rag_debug"] = rag_debug_payload
    return payload


def build_stream_done_payload(
    *,
    generation_time: float,
    rag_used: bool,
    summary_available: bool,
    rag_caveats: Any,
    rag_sources: Any,
    artifacts_payload: Any,
    contract_fields: Dict[str, Any],
    rag_debug_payload: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "type": "done",
        "generation_time": generation_time,
        "rag_used": rag_used,
        "summary_available": summary_available,
        "caveats": rag_caveats,
        "sources": rag_sources,
        "artifacts": artifacts_payload,
        **contract_fields,
    }
    if rag_debug_payload is not None:
        payload["rag_debug"] = rag_debug_payload
    return payload


def safe_stream_payload_json(payload: Dict[str, Any], *, logger: Any) -> str:
    try:
        return json.dumps(payload)
    except (TypeError, ValueError):
        logger.warning("Stream payload is not JSON-serializable; sending reduced debug payload", exc_info=True)
        if "rag_debug" in payload:
            payload["rag_debug"] = {"serialization_error": True}
        return json.dumps(payload)


def _build_optional_debug_payload(
    *,
    orchestrator: Any,
    chat_data: ChatMessage,
    ctx: Dict[str, Any],
    rag_debug_ctx: Mapping[str, Any],
) -> Optional[Dict[str, Any]]:
    if not chat_data.rag_debug:
        return None
    return orchestrator._build_rag_debug_payload(
        rag_debug=rag_debug_ctx,
        context_docs=ctx["context_docs"],
        rag_sources=ctx["rag_sources"],
        llm_tokens_used=None,
        provider_debug=None,
    )


async def build_stream_terminal_events(
    *,
    orchestrator: Any,
    chat_data: ChatMessage,
    db: AsyncSession,
    ctx: Dict[str, Any],
    conversation_id: uuid.UUID,
    assistant_message_id: uuid.UUID,
    route_telemetry: Dict[str, Any],
    execution_telemetry: Dict[str, Any],
    artifacts_payload: Sequence[Dict[str, Any]],
    rag_debug_ctx: Optional[Dict[str, Any]],
    response_text: str,
    model_name: str,
    start_time: datetime,
    default_execution_route: str,
    default_executor_status: str,
    logger: Any,
) -> list[str]:
    effective_debug_ctx = rag_debug_ctx or {}
    start_debug_payload = _build_optional_debug_payload(
        orchestrator=orchestrator,
        chat_data=chat_data,
        ctx=ctx,
        rag_debug_ctx=effective_debug_ctx,
    )
    start_contract_fields = build_stream_contract_fields(
        rag_debug_ctx=effective_debug_ctx,
        route_telemetry=route_telemetry,
        execution_telemetry=execution_telemetry,
        artifacts_payload=artifacts_payload,
        debug_enabled=chat_data.rag_debug,
        debug_included=start_debug_payload is not None,
        default_execution_route=default_execution_route,
        default_executor_status=default_executor_status,
    )
    start_payload = build_stream_start_payload(
        conversation_id=conversation_id,
        assistant_message_id=assistant_message_id,
        rag_enabled=ctx["rag_used"],
        contract_fields=start_contract_fields,
        rag_debug_payload=start_debug_payload,
    )
    events = [f"data: {safe_stream_payload_json(start_payload, logger=logger)}\n\n"]
    if response_text:
        events.append(f"data: {json.dumps({'type': 'chunk', 'content': response_text})}\n\n")

    generation_time = (datetime.utcnow() - start_time).total_seconds()
    await crud_message.create_message(
        db=db,
        conversation_id=conversation_id,
        role="assistant",
        content=response_text,
        model_name=model_name,
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
    done_contract_fields = build_stream_contract_fields(
        rag_debug_ctx=effective_debug_ctx,
        route_telemetry=route_telemetry,
        execution_telemetry=execution_telemetry,
        artifacts_payload=artifacts_payload,
        debug_enabled=chat_data.rag_debug,
        debug_included=start_debug_payload is not None,
        default_execution_route=default_execution_route,
        default_executor_status=default_executor_status,
    )
    done_payload = build_stream_done_payload(
        generation_time=generation_time,
        rag_used=ctx["rag_used"],
        summary_available=False,
        rag_caveats=ctx["rag_caveats"],
        rag_sources=ctx["rag_sources"],
        artifacts_payload=artifacts_payload,
        contract_fields=done_contract_fields,
        rag_debug_payload=start_debug_payload,
    )
    events.append(f"data: {safe_stream_payload_json(done_payload, logger=logger)}\n\n")
    return events

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.schemas import ChatMessage
from app.services.chat.postprocess import (
    append_caveats_and_sources as _append_caveats_and_sources,
    enforce_answer_language as _enforce_answer_language,
    run_answer_critic as _run_answer_critic,
)
from app.services.chat.sources_debug import (
    build_standard_rag_debug_payload as _build_standard_rag_debug_payload,
)
from app.services.llm.manager import llm_manager
from app.services.llm.provider_clients import ProviderRegistry


def default_route_telemetry(
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
        "fallback_reason": "none",
        "fallback_allowed": False,
        "fallback_attempted": False,
        "fallback_policy_version": settings.LLM_FALLBACK_POLICY_VERSION,
        "aihub_attempted": bool(aihub_attempted),
    }


def planner_requires_clarification(ctx: Dict[str, Any]) -> bool:
    rag_debug = ctx.get("rag_debug")
    if not isinstance(rag_debug, dict):
        return False
    if bool(rag_debug.get("requires_clarification", False)):
        return True
    planner_decision = rag_debug.get("planner_decision")
    return bool(isinstance(planner_decision, dict) and planner_decision.get("requires_clarification", False))


def executor_short_circuit_text(ctx: Dict[str, Any]) -> Optional[str]:
    rag_debug = ctx.get("rag_debug")
    if not isinstance(rag_debug, dict):
        return None
    if not bool(rag_debug.get("short_circuit_response", False)):
        return None
    value = str(rag_debug.get("short_circuit_response_text") or "").strip()
    return value or None


def execution_telemetry(ctx: Dict[str, Any]) -> Dict[str, Any]:
    rag_debug = ctx.get("rag_debug")
    execution_route = "narrative"
    executor_attempted = False
    executor_status = "not_attempted"
    executor_error_code = None
    artifacts_count = 0
    analytics_engine_mode_requested = None
    analytics_engine_mode_served = None
    analytics_engine_shadow_enabled = None
    analytics_engine_fallback_reason = None
    analytics_engine_graph_run_id = None
    analytics_engine_graph_node_path = None
    analytics_engine_graph_attempts = None
    analytics_engine_graph_stop_reason = None
    request_id = None
    user_id = None
    conversation_id = None
    file_id = None
    upload_id = None
    document_id = None

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
        if executor_status_raw in {"success", "error", "timeout", "blocked", "fallback", "not_attempted"}:
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
        analytics_engine_mode_requested = rag_debug.get("analytics_engine_mode_requested") or rag_debug.get("engine_mode_requested")
        analytics_engine_mode_served = rag_debug.get("analytics_engine_mode_served") or rag_debug.get("engine_mode_served")
        analytics_engine_shadow_enabled = rag_debug.get("analytics_engine_shadow_enabled")
        if analytics_engine_shadow_enabled is None:
            analytics_engine_shadow_enabled = rag_debug.get("shadow_mode")
        analytics_engine_fallback_reason = rag_debug.get("analytics_engine_fallback_reason") or rag_debug.get("engine_fallback_reason")
        analytics_engine_graph_run_id = rag_debug.get("analytics_engine_graph_run_id") or rag_debug.get("graph_run_id")
        analytics_engine_graph_node_path = rag_debug.get("analytics_engine_graph_node_path") or rag_debug.get("graph_node_path")
        analytics_engine_graph_attempts = rag_debug.get("analytics_engine_graph_attempts") or rag_debug.get("graph_attempts")
        analytics_engine_graph_stop_reason = rag_debug.get("analytics_engine_graph_stop_reason") or rag_debug.get("stop_reason")
        request_id = rag_debug.get("request_id")
        user_id = rag_debug.get("user_id")
        conversation_id = rag_debug.get("conversation_id")
        file_id = rag_debug.get("file_id")
        upload_id = rag_debug.get("upload_id")
        document_id = rag_debug.get("document_id")

    return {
        "execution_route": execution_route,
        "executor_attempted": executor_attempted,
        "executor_status": executor_status,
        "executor_error_code": executor_error_code,
        "artifacts_count": artifacts_count,
        "analytics_engine_mode_requested": analytics_engine_mode_requested,
        "analytics_engine_mode_served": analytics_engine_mode_served,
        "analytics_engine_shadow_enabled": analytics_engine_shadow_enabled,
        "analytics_engine_fallback_reason": analytics_engine_fallback_reason,
        "analytics_engine_graph_run_id": analytics_engine_graph_run_id,
        "analytics_engine_graph_node_path": analytics_engine_graph_node_path,
        "analytics_engine_graph_attempts": analytics_engine_graph_attempts,
        "analytics_engine_graph_stop_reason": analytics_engine_graph_stop_reason,
        "request_id": request_id,
        "user_id": user_id,
        "conversation_id": conversation_id,
        "file_id": file_id,
        "upload_id": upload_id,
        "document_id": document_id,
    }


def extract_artifacts(ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
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


def clarification_text(ctx: Dict[str, Any]) -> str:
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


def build_generation_kwargs(*, chat_data: ChatMessage, ctx: Dict[str, Any]) -> Dict[str, Any]:
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


def should_run_critic(*, chat_data: ChatMessage, ctx: Dict[str, Any]) -> bool:
    return bool(
        chat_data.summarize
        and ctx["rag_used"]
        and ctx["context_docs"]
        and settings.ENABLE_POST_ANSWER_SUMMARIZE
    )


async def postprocess_generated_answer(
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
    if should_run_critic(chat_data=chat_data, ctx=ctx):
        summarized_answer, critic_meta = await _run_answer_critic(
            query=chat_data.message,
            answer=answer_text,
            context_documents=ctx["context_docs"],
            model_source=ctx["provider_source_selected_raw"],
            provider_mode=ctx["provider_mode"],
            model_name=ctx["provider_model_effective"],
        )
        if summarized_answer and summarized_answer != answer_text:
            summary_text, _ = await _enforce_answer_language(
                answer=summarized_answer,
                preferred_lang=ctx["preferred_lang"],
                model_source=ctx["provider_source_selected_raw"],
                provider_mode=ctx["provider_mode"],
                model_name=ctx["provider_model_effective"],
                prompt_max_chars=chat_data.prompt_max_chars,
            )
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


def resolve_provider_selection(*, chat_data: ChatMessage, conversation: Any) -> Dict[str, Any]:
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


def build_rag_debug_payload(
    *,
    rag_debug: Optional[Dict[str, Any]],
    context_docs: Any,
    rag_sources: Any,
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

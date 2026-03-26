from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple
import uuid

from app.domain.chat.query_planner import QueryPlanDecision
from app.observability.slo_metrics import observe_retrieval_coverage, observe_tabular_row_coverage
from app.services.chat.complex_analytics import execute_complex_analytics_path
from app.services.chat.language import (
    ensure_controlled_message_language,
    localized_text,
)
from app.services.chat.tabular_deterministic_result import build_tabular_success_route_result
from app.services.chat.tabular_sql import execute_tabular_sql_path


RagPromptResult = Tuple[str, bool, Dict[str, Any], List[Dict[str, Any]], List[str], List[str]]
logger = logging.getLogger(__name__)


def build_clarification_route_result(
    *,
    planner_decision: QueryPlanDecision,
    planner_decision_payload: Dict[str, Any],
    files: List[Any],
    rag_mode: Optional[str],
    top_k: int,
    preferred_lang: str,
) -> RagPromptResult:
    rag_debug = {
        "planner_decision": planner_decision_payload,
        "intent": planner_decision.intent,
        "detected_intent": planner_decision.intent,
        "selected_route": "clarification_required",
        "strategy_mode": planner_decision.strategy_mode,
        "retrieval_mode": "clarification",
        "requires_clarification": True,
        "clarification_prompt": planner_decision.clarification_prompt,
        "fallback_type": "clarification",
        "fallback_reason": "clarification_required",
        "execution_route": "clarification",
        "executor_attempted": False,
        "executor_status": "not_attempted",
        "executor_error_code": None,
        "artifacts_count": 0,
        "analytical_mode_used": False,
        "rag_mode": rag_mode or "auto",
        "rag_mode_effective": "clarification",
        "followup_context_used": bool(planner_decision_payload.get("followup_context_used", False)),
        "prior_tabular_intent_reused": bool(planner_decision_payload.get("prior_tabular_intent_reused", False)),
        "file_ids": [str(file_obj.id) for file_obj in files],
        "retrieval_policy": {
            "mode": "clarification",
            "query_profile": "metric_critical",
            "requested_top_k": top_k,
            "effective_top_k": 0,
            "expected_chunks_total": sum(int(getattr(file_obj, "chunks_count", 0) or 0) for file_obj in files),
            "escalation": {"attempted": False, "applied": False, "reason": "clarification_required"},
            "row_escalation": {"attempted": False, "applied": False, "reason": "clarification_required"},
        },
    }
    final_prompt = ensure_controlled_message_language(
        text=str(planner_decision.clarification_prompt or "").strip(),
        preferred_lang=preferred_lang,
        fallback_ru=(
            "Уточните, пожалуйста, метрику и срез перед запуском детерминированной аналитики."
        ),
        fallback_en=(
            "Please clarify the metric and scope before I run deterministic analytics."
        ),
    )
    rag_debug["clarification_prompt"] = final_prompt
    return final_prompt, False, rag_debug, [], [], []


async def maybe_run_complex_analytics_route(
    *,
    query: str,
    files: List[Any],
    planner_decision_payload: Dict[str, Any],
    expected_chunks_total: int,
    rag_mode: Optional[str],
    top_k: int,
    preferred_lang: str,
    model_source: Optional[str],
    provider_mode: Optional[str],
    model_name: Optional[str],
    complex_analytics_executor=execute_complex_analytics_path,
) -> RagPromptResult:
    complex_result = await complex_analytics_executor(
        query=query,
        files=files,
        model_source=model_source,
        provider_mode=provider_mode,
        model_name=model_name,
    )
    if isinstance(complex_result, dict):
        rag_debug = dict(complex_result.get("debug") or {})
        rag_debug["planner_decision"] = planner_decision_payload
        rag_debug["strategy_mode"] = planner_decision_payload.get("strategy_mode", "analytical")
        rag_debug["file_ids"] = [str(file_obj.id) for file_obj in files]
        rag_debug["rag_mode"] = rag_mode or "auto"
        rag_debug["execution_route"] = "complex_analytics"
        rag_debug["detected_intent"] = "complex_analytics"
        rag_debug["selected_route"] = "complex_analytics"
        rag_debug["artifacts"] = list(complex_result.get("artifacts") or [])
        rag_debug["artifacts_count"] = int(
            rag_debug.get("artifacts_count", len(rag_debug.get("artifacts") or [])) or 0
        )
        rag_debug["analytical_mode_used"] = True
        rag_debug["followup_context_used"] = bool(planner_decision_payload.get("followup_context_used", False))
        rag_debug["prior_tabular_intent_reused"] = bool(
            planner_decision_payload.get("prior_tabular_intent_reused", False)
        )
        rag_sources = list(complex_result.get("sources") or [])
        status = str(complex_result.get("status") or "ok")
        if status == "ok":
            rag_debug["rag_mode_effective"] = "complex_analytics"
            rag_debug["requires_clarification"] = False
            rag_debug["executor_attempted"] = True
            rag_debug["executor_status"] = str(rag_debug.get("executor_status") or "success")
            rag_debug["short_circuit_response"] = True
            rag_debug["short_circuit_response_text"] = str(complex_result.get("final_response") or "").strip()
            rag_debug["fallback_type"] = "none"
            rag_debug["fallback_reason"] = "none"
            observe_retrieval_coverage(
                coverage_ratio=1.0,
                retrieval_mode="complex_analytics",
                expected_chunks=expected_chunks_total,
                retrieved_chunks=expected_chunks_total,
            )
            observe_tabular_row_coverage(
                coverage_ratio=1.0,
                retrieval_mode="complex_analytics",
                rows_expected_total=int(rag_debug.get("rows_expected_total", 0) or 0),
                rows_retrieved_total=int(rag_debug.get("rows_retrieved_total", 0) or 0),
            )
            final_prompt = rag_debug["short_circuit_response_text"] or str(complex_result.get("final_response") or "")
            return final_prompt, False, rag_debug, [], [], rag_sources

        rag_debug["rag_mode_effective"] = "complex_analytics_error"
        rag_debug["requires_clarification"] = True
        rag_debug["short_circuit_response"] = True
        rag_debug["fallback_type"] = "complex_analytics_fallback"
        rag_debug["fallback_reason"] = str(rag_debug.get("executor_error_code") or "complex_analytics_error")
        clarification_prompt = ensure_controlled_message_language(
            text=str(complex_result.get("clarification_prompt") or "").strip(),
            preferred_lang=preferred_lang,
            fallback_ru=(
                "Не удалось выполнить сложный аналитический сценарий в sandbox. "
                "Уточните метрику, фильтры или упростите шаги анализа."
            ),
            fallback_en=(
                "Complex analytics sandbox execution failed. "
                "Please clarify metric, filters, or simplify analysis steps."
            ),
        )
        rag_debug["clarification_prompt"] = clarification_prompt
        rag_debug["short_circuit_response_text"] = clarification_prompt
        observe_retrieval_coverage(
            coverage_ratio=0.0,
            retrieval_mode="complex_analytics_error",
            expected_chunks=expected_chunks_total,
            retrieved_chunks=0,
        )
        observe_tabular_row_coverage(
            coverage_ratio=0.0,
            retrieval_mode="complex_analytics_error",
            rows_expected_total=0,
            rows_retrieved_total=0,
        )
        return clarification_prompt, False, rag_debug, [], [], rag_sources

    rag_debug = {
        "planner_decision": planner_decision_payload,
        "strategy_mode": planner_decision_payload.get("strategy_mode", "analytical"),
        "intent": "complex_analytics",
        "detected_intent": "complex_analytics",
        "selected_route": "complex_analytics",
        "retrieval_mode": "clarification",
        "requires_clarification": True,
        "fallback_type": "complex_analytics_unavailable",
        "fallback_reason": "executor_unavailable",
        "execution_route": "complex_analytics",
        "executor_attempted": True,
        "executor_status": "error",
        "executor_error_code": "executor_unavailable",
        "artifacts_count": 0,
        "analytical_mode_used": True,
        "rag_mode": rag_mode or "auto",
        "rag_mode_effective": "complex_analytics_error",
        "file_ids": [str(file_obj.id) for file_obj in files],
        "followup_context_used": bool(planner_decision_payload.get("followup_context_used", False)),
        "prior_tabular_intent_reused": bool(planner_decision_payload.get("prior_tabular_intent_reused", False)),
        "retrieval_policy": {
            "mode": "clarification",
            "query_profile": "complex_analytics",
            "requested_top_k": top_k,
            "effective_top_k": 0,
            "expected_chunks_total": expected_chunks_total,
            "escalation": {"attempted": False, "applied": False, "reason": "executor_unavailable"},
            "row_escalation": {"attempted": False, "applied": False, "reason": "executor_unavailable"},
        },
    }
    final_prompt = localized_text(
        preferred_lang=preferred_lang,
        ru=(
            "Sandbox сложной аналитики сейчас недоступен. "
            "Повторите запрос с более узким scope или используйте детерминированный SQL (aggregate/profile)."
        ),
        en=(
            "Complex analytics sandbox is currently unavailable. "
            "Please retry with narrower scope or ask for deterministic SQL aggregate/profile."
        ),
    )
    rag_debug["clarification_prompt"] = final_prompt
    rag_debug["short_circuit_response"] = True
    rag_debug["short_circuit_response_text"] = final_prompt
    return final_prompt, False, rag_debug, [], [], []


def _active_processing_map(files: List[Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for file_obj in files:
        active_processing = getattr(file_obj, "active_processing", None)
        processing_id = getattr(active_processing, "id", None)
        if processing_id is not None:
            out[str(file_obj.id)] = str(processing_id)
    return out


def _select_combined_scope(semantic_docs: List[Dict[str, Any]], files: List[Any]) -> Tuple[List[Any], Dict[str, Any]]:
    if not semantic_docs:
        return files, {"selected_file_id": None, "selected_sheet": None}

    file_scores: Dict[str, float] = {}
    sheet_scores: Dict[Tuple[str, str], float] = {}
    for doc in semantic_docs:
        meta = doc.get("metadata") or {}
        score = float(doc.get("similarity_score", 0.0) or 0.0)
        file_id = str(meta.get("file_id") or "")
        sheet_name = str(meta.get("sheet_name") or "")
        if file_id:
            file_scores[file_id] = max(file_scores.get(file_id, 0.0), score)
        if file_id and sheet_name:
            sheet_scores[(file_id, sheet_name)] = max(sheet_scores.get((file_id, sheet_name), 0.0), score)

    selected_file_id = None
    if file_scores:
        selected_file_id = max(file_scores.items(), key=lambda item: item[1])[0]
    selected_sheet = None
    if selected_file_id:
        same_file_sheets = [(sheet, score) for (fid, sheet), score in sheet_scores.items() if fid == selected_file_id]
        if same_file_sheets:
            selected_sheet = max(same_file_sheets, key=lambda item: item[1])[0]

    scoped_files = files
    if selected_file_id:
        scoped_files = [item for item in files if str(item.id) == str(selected_file_id)] or files
    return scoped_files, {
        "selected_file_id": selected_file_id,
        "selected_sheet": selected_sheet,
    }


async def _run_combined_semantic_prefetch(
    *,
    query: str,
    user_id: uuid.UUID,
    conversation_id: uuid.UUID,
    files: List[Any],
    top_k: int,
    rag_retriever_client: Any,
) -> Tuple[List[Any], List[Dict[str, Any]], Dict[str, Any]]:
    processing_ids_by_file = _active_processing_map(files)
    file_ids = [str(file_obj.id) for file_obj in files]
    kwargs = {
        "query": query,
        "top_k": max(8, top_k * 2),
        "user_id": str(user_id),
        "conversation_id": str(conversation_id),
        "file_ids": file_ids,
        "processing_ids": [processing_ids_by_file[fid] for fid in file_ids if fid in processing_ids_by_file] or None,
        "rag_mode": "hybrid",
        "chunk_types": ["file_summary", "sheet_summary", "row_group", "column_summary"],
        "debug_return": True,
    }
    semantic_result = await rag_retriever_client.query_rag(**kwargs)
    semantic_docs = []
    semantic_debug = {}
    if isinstance(semantic_result, dict):
        semantic_docs = list(semantic_result.get("docs") or [])
        semantic_debug = dict(semantic_result.get("debug") or {})
    scoped_files, scope_debug = _select_combined_scope(semantic_docs, files)
    scope_debug["semantic_hits"] = len(semantic_docs)
    scope_debug["semantic_debug"] = semantic_debug
    return scoped_files, semantic_docs, scope_debug


async def maybe_run_deterministic_route(
    *,
    query: str,
    execution_query: Optional[str] = None,
    user_id: uuid.UUID,
    conversation_id: uuid.UUID,
    files: List[Any],
    planner_decision: QueryPlanDecision,
    planner_decision_payload: Dict[str, Any],
    expected_chunks_total: int,
    rag_mode: Optional[str],
    top_k: int,
    preferred_lang: str,
    tabular_sql_executor=execute_tabular_sql_path,
    rag_retriever_client=None,
    is_combined_intent: bool = False,
) -> RagPromptResult:
    effective_query = str(execution_query or query)
    scoped_files = files
    file_ids = [str(file_obj.id) for file_obj in files]
    processing_ids_by_file = _active_processing_map(files)
    combined_context_docs: List[Dict[str, Any]] = []
    combined_debug: Dict[str, Any] = {}
    if is_combined_intent and rag_retriever_client is not None:
        try:
            scoped_files, combined_context_docs, combined_debug = await _run_combined_semantic_prefetch(
                query=effective_query,
                user_id=user_id,
                conversation_id=conversation_id,
                files=files,
                top_k=top_k,
                rag_retriever_client=rag_retriever_client,
            )
            selected_sheet = str(combined_debug.get("selected_sheet") or "").strip()
            if selected_sheet:
                effective_query = f"{effective_query}\n[combined_scope_sheet={selected_sheet}]"
        except Exception:
            combined_debug = {"prefetch_error": "semantic_prefetch_failed"}
            combined_context_docs = []
            scoped_files = files

    tabular_sql_result = await tabular_sql_executor(query=effective_query, files=scoped_files)
    if isinstance(tabular_sql_result, dict):
        retrieval_mode = "tabular_combined" if is_combined_intent else "tabular_sql"
        if str(tabular_sql_result.get("status") or "ok") == "error":
            rag_debug = dict(tabular_sql_result.get("debug") or {})
            rag_debug["planner_decision"] = planner_decision_payload
            rag_debug["file_ids"] = [str(file_obj.id) for file_obj in files]
            rag_debug["retrieval_mode"] = retrieval_mode
            rag_debug["rag_mode"] = rag_mode or "auto"
            rag_debug["rag_mode_effective"] = f"{retrieval_mode}_error"
            rag_debug["requires_clarification"] = True
            rag_debug["execution_route"] = "tabular_sql"
            rag_debug["detected_intent"] = str(
                rag_debug.get("detected_intent")
                or rag_debug.get("intent")
                or planner_decision_payload.get("intent")
                or "tabular_error"
            )
            rag_debug["selected_route"] = str(
                rag_debug.get("selected_route")
                or planner_decision_payload.get("route")
                or retrieval_mode
            )
            rag_debug["executor_attempted"] = False
            rag_debug["executor_status"] = "not_attempted"
            rag_debug["executor_error_code"] = None
            rag_debug["artifacts_count"] = 0
            rag_debug["analytical_mode_used"] = True
            rag_debug["strategy_mode"] = "combined" if is_combined_intent else "analytical"
            rag_debug["followup_context_used"] = bool(planner_decision_payload.get("followup_context_used", False))
            rag_debug["prior_tabular_intent_reused"] = bool(
                planner_decision_payload.get("prior_tabular_intent_reused", False)
            )
            existing_fallback_type = str(rag_debug.get("fallback_type") or "").strip()
            if existing_fallback_type and existing_fallback_type != "none":
                rag_debug["fallback_type"] = existing_fallback_type
            else:
                rag_debug["fallback_type"] = (
                    "unsupported_missing_column"
                    if str(rag_debug.get("selected_route") or "") == "unsupported_missing_column"
                    else "tabular_executor_error"
                )
            rag_debug["fallback_reason"] = str(
                rag_debug.get("fallback_reason")
                or rag_debug.get("executor_error_code")
                or (
                    rag_debug.get("deterministic_error", {}).get("code")
                    if isinstance(rag_debug.get("deterministic_error"), dict)
                    else None
                )
                or "tabular_executor_error"
            )
            if combined_debug:
                rag_debug["combined_scope"] = combined_debug
            clarification_prompt = ensure_controlled_message_language(
                text=str(tabular_sql_result.get("clarification_prompt") or "").strip(),
                preferred_lang=preferred_lang,
                fallback_ru=(
                    "Не удалось выполнить детерминированный SQL-запрос. "
                    "Уточните метрику, фильтр или период и повторите запрос."
                ),
                fallback_en=(
                    "Deterministic SQL execution failed. "
                    "Please clarify metric, filter, or period and retry."
                ),
            )
            rag_debug["clarification_prompt"] = clarification_prompt
            observe_retrieval_coverage(
                coverage_ratio=0.0,
                retrieval_mode=f"{retrieval_mode}_error",
                expected_chunks=expected_chunks_total,
                retrieved_chunks=0,
            )
            observe_tabular_row_coverage(
                coverage_ratio=float(tabular_sql_result.get("row_coverage_ratio", 0.0) or 0.0),
                retrieval_mode=f"{retrieval_mode}_error",
                rows_expected_total=int(tabular_sql_result.get("rows_expected_total", 0) or 0),
                rows_retrieved_total=int(tabular_sql_result.get("rows_retrieved_total", 0) or 0),
            )
            logger.info(
                (
                    "rag_trace route=%s strategy=%s analytical_mode_used=true retrieval_mode=%s "
                    "retrieval_k=%d retrieval_hits=0 avg_similarity=0.0000 context_tokens=0 "
                    "uid=%s chat_id=%s file_ids=%s processing_ids=%s"
                ),
                "deterministic_analytics",
                rag_debug.get("strategy_mode"),
                retrieval_mode,
                top_k,
                str(user_id),
                str(conversation_id),
                ",".join(file_ids),
                ",".join(processing_ids_by_file.values()),
            )
            return clarification_prompt, False, rag_debug, [], [], []
        return build_tabular_success_route_result(
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
            is_combined_intent=is_combined_intent,
            tabular_sql_result=tabular_sql_result,
            processing_ids_by_file=processing_ids_by_file,
            combined_context_docs=combined_context_docs,
            combined_debug=combined_debug,
        )
    retrieval_mode = "tabular_combined" if is_combined_intent else "tabular_sql"
    clarification_prompt = localized_text(
        preferred_lang=preferred_lang,
        ru=(
            "Детерминированный аналитический модуль вернул невалидный payload. "
            "Повторите запрос; если проблема сохранится, сузьте формулировку."
        ),
        en=(
            "Deterministic analytics engine returned invalid payload. "
            "Please retry; if this persists, narrow the question scope."
        ),
    )
    rag_debug = {
        "planner_decision": planner_decision_payload,
        "file_ids": [str(file_obj.id) for file_obj in files],
        "retrieval_mode": retrieval_mode,
        "detected_intent": "tabular_sql",
        "selected_route": "tabular_sql_invalid_payload",
        "rag_mode": rag_mode or "auto",
        "rag_mode_effective": f"{retrieval_mode}_invalid_payload",
        "requires_clarification": True,
        "fallback_type": "tabular_invalid_payload",
        "fallback_reason": "invalid_executor_payload",
        "execution_route": "tabular_sql",
        "executor_attempted": True,
        "executor_status": "error",
        "executor_error_code": "invalid_executor_payload",
        "artifacts_count": 0,
        "analytical_mode_used": True,
        "strategy_mode": "combined" if is_combined_intent else "analytical",
        "followup_context_used": bool(planner_decision_payload.get("followup_context_used", False)),
        "prior_tabular_intent_reused": bool(planner_decision_payload.get("prior_tabular_intent_reused", False)),
        "clarification_prompt": clarification_prompt,
        "retrieval_policy": {
            "mode": "clarification",
            "query_profile": planner_decision.intent,
            "requested_top_k": top_k,
            "effective_top_k": 0,
            "expected_chunks_total": expected_chunks_total,
            "escalation": {"attempted": False, "applied": False, "reason": "invalid_executor_payload"},
            "row_escalation": {"attempted": False, "applied": False, "reason": "invalid_executor_payload"},
        },
    }
    if combined_debug:
        rag_debug["combined_scope"] = combined_debug
    observe_retrieval_coverage(
        coverage_ratio=0.0,
        retrieval_mode=f"{retrieval_mode}_invalid_payload",
        expected_chunks=expected_chunks_total,
        retrieved_chunks=0,
    )
    observe_tabular_row_coverage(
        coverage_ratio=0.0,
        retrieval_mode=f"{retrieval_mode}_invalid_payload",
        rows_expected_total=0,
        rows_retrieved_total=0,
    )
    logger.error(
        (
            "rag_trace route=%s strategy=%s analytical_mode_used=true retrieval_mode=%s "
            "retrieval_k=%d retrieval_hits=0 avg_similarity=0.0000 context_tokens=0 "
            "uid=%s chat_id=%s file_ids=%s processing_ids=%s error=%s"
        ),
        "deterministic_analytics",
        rag_debug.get("strategy_mode"),
        retrieval_mode,
        top_k,
        str(user_id),
        str(conversation_id),
        ",".join(file_ids),
        ",".join(processing_ids_by_file.values()),
        "invalid_executor_payload",
    )
    return clarification_prompt, False, rag_debug, [], [], []


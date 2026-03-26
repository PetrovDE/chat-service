from __future__ import annotations

from copy import deepcopy
import logging
from typing import Any, Dict, List, Optional, Tuple
import uuid

from app.core.config import settings
from app.observability.slo_metrics import observe_retrieval_coverage, observe_tabular_row_coverage
from app.services.chat.controlled_response_composer import (
    build_no_retrieval_message,
    build_runtime_error_message,
)
from app.services.chat.embedding_config import group_files_by_embedding_config, resolve_rag_embedding_config
from app.services.chat.language import apply_language_policy_to_prompt
from app.services.chat.rag_retrieval_helpers import (
    collect_context_and_debug as _collect_context_and_debug,
    run_grouped_retrieval as _run_grouped_retrieval,
)
from app.services.chat.retrieval_policy import build_retrieval_budget_plan, choose_escalation_plan
from app.services.chat.sources_debug import (
    build_row_coverage_stats,
    build_sources_list_with_mode,
)

logger = logging.getLogger(__name__)


async def run_narrative_retrieval_path(
    *,
    query: str,
    user_id: uuid.UUID,
    conversation_id: uuid.UUID,
    files: List[Any],
    top_k: int,
    rag_mode: Optional[str],
    model_source: Optional[str],
    model_name: Optional[str],
    preferred_lang: str,
    prompt_max_chars: Optional[int],
    planner_decision_payload: Dict[str, Any],
    rag_retriever_client: Any,
    full_file_prompt_builder: Any,
    rag_caveats_builder: Any,
    initial_final_prompt: str,
) -> Tuple[str, bool, Optional[Dict[str, Any]], List[Dict[str, Any]], List[str], List[str]]:
    final_prompt = initial_final_prompt
    rag_used = False
    rag_debug = None
    context_docs: List[Dict[str, Any]] = []
    rag_caveats: List[str] = []
    rag_sources: List[str] = []

    expected_chunks_total = sum(int(getattr(file_obj, "chunks_count", 0) or 0) for file_obj in files)
    rag_file_ids = [str(file_obj.id) for file_obj in files]
    processing_ids_by_file: Dict[str, str] = {}
    for file_obj in files:
        active_processing = getattr(file_obj, "active_processing", None)
        processing_id = getattr(active_processing, "id", None)
        if processing_id is not None:
            processing_ids_by_file[str(file_obj.id)] = str(processing_id)
    groups = group_files_by_embedding_config(files, model_source)
    embedding_mode, embedding_model = resolve_rag_embedding_config(files, model_source)

    budget_plan = build_retrieval_budget_plan(
        query=query,
        rag_mode=rag_mode,
        requested_top_k=top_k,
        expected_chunks_total=expected_chunks_total,
    )
    selected_top_k = int(budget_plan.get("effective_top_k", top_k) or top_k)
    selected_rag_mode = rag_mode

    try:
        first_results = await _run_grouped_retrieval(
            rag_retriever_client=rag_retriever_client,
            query=query,
            user_id=user_id,
            conversation_id=conversation_id,
            groups=groups,
            all_file_ids=rag_file_ids,
            processing_ids_by_file=processing_ids_by_file,
            top_k=selected_top_k,
            rag_mode=selected_rag_mode,
            embedding_mode=embedding_mode,
            embedding_model=embedding_model,
        )

        context_docs, debug_groups, _ = _collect_context_and_debug(
            rag_results=first_results,
            non_full_file_top_k=selected_top_k,
        )

        coverage_ratio_before = (
            float(len(context_docs) / expected_chunks_total)
            if expected_chunks_total > 0
            else 1.0
        )

        escalation_meta: Dict[str, Any] = {
            "attempted": False,
            "applied": False,
            "reason": None,
        }
        escalation_plan = choose_escalation_plan(
            rag_mode=selected_rag_mode,
            expected_chunks_total=expected_chunks_total,
            current_top_k=selected_top_k,
            coverage_ratio=coverage_ratio_before,
        )
        if escalation_plan:
            escalation_meta = {
                "attempted": True,
                "applied": False,
                "reason": escalation_plan.get("reason"),
                "next_mode": escalation_plan.get("next_mode"),
                "next_top_k": escalation_plan.get("next_top_k"),
                "coverage_ratio": escalation_plan.get("coverage_ratio"),
                "coverage_threshold": escalation_plan.get("coverage_threshold"),
            }

            next_top_k = int(escalation_plan.get("next_top_k") or selected_top_k)
            next_mode = escalation_plan.get("next_mode")

            second_results = await _run_grouped_retrieval(
                rag_retriever_client=rag_retriever_client,
                query=query,
                user_id=user_id,
                conversation_id=conversation_id,
                groups=groups,
                all_file_ids=rag_file_ids,
                processing_ids_by_file=processing_ids_by_file,
                top_k=next_top_k,
                rag_mode=next_mode,
                embedding_mode=embedding_mode,
                embedding_model=embedding_model,
            )
            escalated_context_docs, escalated_debug_groups, _ = _collect_context_and_debug(
                rag_results=second_results,
                non_full_file_top_k=next_top_k,
            )

            if len(escalated_context_docs) > len(context_docs):
                context_docs = escalated_context_docs
                debug_groups = escalated_debug_groups
                selected_top_k = next_top_k
                selected_rag_mode = next_mode
                escalation_meta["applied"] = True
                escalation_meta["selected_docs"] = len(context_docs)
            else:
                escalation_meta["selected_docs"] = len(context_docs)

        row_coverage_threshold = float(settings.RAG_FULL_FILE_MIN_ROW_COVERAGE)
        row_escalation_meta: Dict[str, Any] = {
            "attempted": False,
            "applied": False,
            "reason": None,
            "coverage_threshold": row_coverage_threshold,
        }
        row_stats_before = build_row_coverage_stats(context_docs)
        row_coverage_before = float(row_stats_before.get("row_coverage_ratio", 0.0) or 0.0)
        rows_expected_before = int(row_stats_before.get("rows_expected_total", 0) or 0)
        full_file_mode_detected = bool(
            (selected_rag_mode or "").lower() == "full_file"
            or any(
                isinstance(dbg, dict)
                and (dbg.get("retrieval_mode") == "full_file" or dbg.get("intent") == "analyze_full_file")
                for dbg in debug_groups
            )
        )
        if full_file_mode_detected and rows_expected_before > 0 and row_coverage_before < row_coverage_threshold:
            row_escalation_meta["attempted"] = True
            row_escalation_meta["reason"] = "low_row_coverage_full_file_repass"
            row_escalation_meta["coverage_ratio"] = row_coverage_before
            base_full_file_cap = int(settings.RAG_FULL_FILE_MAX_CHUNKS)
            max_escalation_cap = int(settings.RAG_FULL_FILE_ESCALATION_MAX_CHUNKS)
            next_full_file_cap = min(max_escalation_cap, max(base_full_file_cap * 2, len(context_docs) * 2, selected_top_k))
            row_escalation_meta["next_full_file_max_chunks"] = next_full_file_cap

            can_repass = next_full_file_cap > base_full_file_cap or (selected_rag_mode or "").lower() != "full_file"
            if can_repass:
                retried_top_k = max(selected_top_k, expected_chunks_total or selected_top_k)
                retried_results = await _run_grouped_retrieval(
                    rag_retriever_client=rag_retriever_client,
                    query=query,
                    user_id=user_id,
                    conversation_id=conversation_id,
                    groups=groups,
                    all_file_ids=rag_file_ids,
                    processing_ids_by_file=processing_ids_by_file,
                    top_k=retried_top_k,
                    rag_mode="full_file",
                    embedding_mode=embedding_mode,
                    embedding_model=embedding_model,
                    full_file_max_chunks=next_full_file_cap,
                )
                retried_docs, retried_debug_groups, _ = _collect_context_and_debug(
                    rag_results=retried_results,
                    non_full_file_top_k=retried_top_k,
                )
                retried_row_stats = build_row_coverage_stats(retried_docs)
                retried_ratio = float(retried_row_stats.get("row_coverage_ratio", 0.0) or 0.0)
                row_escalation_meta["retried_coverage_ratio"] = retried_ratio
                row_escalation_meta["retried_rows_retrieved"] = int(
                    retried_row_stats.get("rows_retrieved_total", 0) or 0
                )
                improved = retried_ratio > row_coverage_before or len(retried_docs) > len(context_docs)
                if improved:
                    context_docs = retried_docs
                    debug_groups = retried_debug_groups
                    selected_rag_mode = "full_file"
                    selected_top_k = retried_top_k
                    row_escalation_meta["applied"] = True

        rag_debug = (debug_groups[0] if debug_groups else {}) if isinstance(debug_groups, list) else {}
        if not isinstance(rag_debug, dict):
            rag_debug = {}
        else:
            rag_debug = deepcopy(rag_debug)

        rag_debug["embedding_mode"] = embedding_mode
        rag_debug["embedding_model"] = embedding_model
        rag_debug["detected_language"] = preferred_lang
        rag_debug["planner_decision"] = planner_decision_payload
        rag_debug["strategy_mode"] = planner_decision_payload.get("strategy_mode", "semantic")
        rag_debug["execution_route"] = "narrative"
        rag_debug["executor_attempted"] = False
        rag_debug["executor_status"] = "not_attempted"
        rag_debug["executor_error_code"] = None
        rag_debug["artifacts_count"] = 0
        rag_debug["analytical_mode_used"] = False
        rag_debug["file_ids"] = rag_file_ids
        rag_debug["rag_mode"] = rag_mode or "auto"
        rag_debug["rag_mode_effective"] = selected_rag_mode or rag_mode or "auto"
        rag_debug["mixed_embedding_groups"] = [
            {"mode": mode, "model": model, "file_count": len(ids)}
            for (mode, model), ids in groups.items()
        ]
        rag_debug["mixed_embeddings"] = len(groups) > 1
        rag_debug["group_count"] = len(groups)
        rag_debug["group_debug"] = [deepcopy(dbg) if isinstance(dbg, dict) else dbg for dbg in debug_groups]
        rag_debug["retrieval_policy"] = {
            **budget_plan,
            "effective_top_k": selected_top_k,
            "expected_chunks_total": expected_chunks_total,
            "escalation": escalation_meta,
            "row_escalation": row_escalation_meta,
        }

        retrieved_chunks_total = len(context_docs)
        coverage_ratio = (float(retrieved_chunks_total / expected_chunks_total) if expected_chunks_total > 0 else 0.0)
        rag_debug["retrieved_chunks_total"] = retrieved_chunks_total
        rag_debug["coverage"] = {
            "expected_chunks": expected_chunks_total,
            "retrieved_chunks": retrieved_chunks_total,
            "ratio": coverage_ratio,
            "complete": bool(expected_chunks_total == 0 or retrieved_chunks_total >= expected_chunks_total),
        }
        row_stats = build_row_coverage_stats(context_docs)
        rag_debug["rows_expected_total"] = int(row_stats.get("rows_expected_total", 0) or 0)
        rag_debug["rows_retrieved_total"] = int(row_stats.get("rows_retrieved_total", 0) or 0)
        rag_debug["rows_used_map_total"] = int(rag_debug["rows_retrieved_total"])
        rag_debug["rows_used_reduce_total"] = int(rag_debug["rows_retrieved_total"])
        rag_debug["row_coverage_ratio"] = (
            float(rag_debug["rows_used_reduce_total"] / rag_debug["rows_expected_total"])
            if rag_debug["rows_expected_total"] > 0
            else float(row_stats.get("row_coverage_ratio", 0.0))
        )
        chunk_coverage_close = bool(
            expected_chunks_total > 0 and retrieved_chunks_total >= max(1, int(expected_chunks_total * 0.9))
        )
        if (
            chunk_coverage_close
            and rag_debug.get("rows_expected_total", 0) > 0
            and rag_debug.get("row_coverage_ratio", 0.0) < row_coverage_threshold
        ):
            rag_debug["silent_row_loss_detected"] = True
            rag_debug["truncated"] = True

        retrieval_mode_observed = str(rag_debug.get("retrieval_mode") or selected_rag_mode or rag_mode or "auto")
        observe_retrieval_coverage(
            coverage_ratio=float(coverage_ratio),
            retrieval_mode=retrieval_mode_observed,
            expected_chunks=int(expected_chunks_total),
            retrieved_chunks=int(retrieved_chunks_total),
        )
        if int(rag_debug.get("rows_expected_total", 0) or 0) > 0:
            observe_tabular_row_coverage(
                coverage_ratio=float(rag_debug.get("row_coverage_ratio", 0.0) or 0.0),
                retrieval_mode=retrieval_mode_observed,
                rows_expected_total=int(rag_debug.get("rows_expected_total", 0) or 0),
                rows_retrieved_total=int(rag_debug.get("rows_retrieved_total", 0) or 0),
            )

        if context_docs:
            retrieval_mode = (rag_debug or {}).get("retrieval_mode") if isinstance(rag_debug, dict) else None
            intent = (rag_debug or {}).get("intent") if isinstance(rag_debug, dict) else None

            if retrieval_mode == "full_file" or intent == "analyze_full_file":
                final_prompt, map_reduce_meta = await full_file_prompt_builder(
                    query=query,
                    context_documents=context_docs,
                    preferred_lang=preferred_lang,
                    model_source=model_source,
                    model_name=model_name,
                    prompt_max_chars=prompt_max_chars,
                )
                rag_debug["full_file_map_reduce"] = map_reduce_meta
                rag_debug["rows_used_map_total"] = int(
                    map_reduce_meta.get("rows_used_map_total", rag_debug.get("rows_used_map_total", 0)) or 0
                )
                rag_debug["rows_used_reduce_total"] = int(
                    map_reduce_meta.get("rows_used_reduce_total", rag_debug.get("rows_used_reduce_total", 0)) or 0
                )
                if rag_debug.get("rows_expected_total", 0) > 0:
                    rag_debug["row_coverage_ratio"] = float(
                        rag_debug["rows_used_reduce_total"] / max(1, rag_debug["rows_expected_total"])
                    )
                rag_debug["truncated"] = bool(
                    rag_debug.get("truncated")
                    or
                    map_reduce_meta.get("truncated_batches")
                    or rag_debug.get("full_file_limit_hit")
                )
                if not rag_debug.get("coverage", {}).get("complete", True):
                    rag_debug["truncated"] = True
                if (
                    rag_debug.get("rows_expected_total", 0) > 0
                    and rag_debug.get("rows_used_reduce_total", 0) < rag_debug.get("rows_expected_total", 0)
                ):
                    rag_debug["truncated"] = True
            else:
                final_prompt = rag_retriever_client.build_context_prompt(query=query, context_documents=context_docs)
                final_prompt = apply_language_policy_to_prompt(prompt=final_prompt, preferred_lang=preferred_lang)

            rag_used = True
            rag_sources = build_sources_list_with_mode(
                context_documents=context_docs,
                max_items=12,
                aggregate_ranges=bool(retrieval_mode == "full_file" or intent == "analyze_full_file"),
            )
            rag_caveats = rag_caveats_builder(files=files, context_documents=context_docs, rag_debug=rag_debug)
            avg_similarity = 0.0
            if context_docs:
                avg_similarity = float(
                    sum(float(item.get("similarity_score", 0.0) or 0.0) for item in context_docs) / len(context_docs)
                )
            logger.info(
                (
                    "rag_trace route=%s strategy=%s analytical_mode_used=%s retrieval_mode=%s retrieval_k=%d "
                    "retrieval_hits=%d avg_similarity=%.4f context_tokens=%d pipeline_version=%s "
                    "embedding_model=%s embedding_dimension=%s uid=%s chat_id=%s file_ids=%s processing_ids=%s"
                ),
                "narrative_retrieval",
                rag_debug.get("strategy_mode"),
                rag_debug.get("analytical_mode_used"),
                retrieval_mode,
                selected_top_k,
                len(context_docs),
                avg_similarity,
                int(rag_debug.get("context_tokens", 0) or 0),
                str(rag_debug.get("pipeline_version") or ""),
                str(embedding_model or ""),
                str(rag_debug.get("embedding_dimension") or ""),
                str(user_id),
                str(conversation_id),
                ",".join(rag_file_ids),
                ",".join(processing_ids_by_file.values()),
            )
            logger.info(
                "RAG enabled: docs=%d mode=%s model=%s retrieval_mode=%s top_k=%d",
                len(context_docs),
                embedding_mode,
                embedding_model,
                retrieval_mode,
                selected_top_k,
            )
        else:
            logger.info("RAG: no relevant chunks")
            no_retrieval_prompt = build_no_retrieval_message(
                preferred_lang=preferred_lang,
            )
            final_prompt = no_retrieval_prompt
            rag_used = False
            rag_sources = []
            rag_caveats = []
            if not isinstance(rag_debug, dict):
                rag_debug = {}
            rag_debug["requires_clarification"] = True
            rag_debug["clarification_prompt"] = no_retrieval_prompt
            rag_debug["retrieval_mode"] = "narrative_no_retrieval"
            rag_debug["rag_mode_effective"] = "narrative_no_retrieval"
            rag_debug["detected_language"] = preferred_lang
            rag_debug["detected_intent"] = "narrative_retrieval"
            rag_debug["selected_route"] = "narrative_no_retrieval"
            rag_debug["fallback_type"] = "retrieval_empty"
            rag_debug["fallback_reason"] = "no_relevant_chunks"
            rag_debug["controlled_response_state"] = "no_retrieval"

    except Exception as exc:
        logger.exception("RAG retrieval failed with strict contract: %s", exc)
        error_prompt = build_runtime_error_message(
            preferred_lang=preferred_lang,
        )
        final_prompt = error_prompt
        rag_used = False
        rag_sources = []
        rag_caveats = []
        rag_debug = {
            "planner_decision": planner_decision_payload,
            "strategy_mode": planner_decision_payload.get("strategy_mode", "semantic"),
            "intent": "narrative_retrieval",
            "retrieval_mode": "narrative_error",
            "execution_route": "narrative",
            "requires_clarification": True,
            "clarification_prompt": error_prompt,
            "executor_attempted": False,
            "executor_status": "error",
            "executor_error_code": "retrieval_runtime_error",
            "artifacts_count": 0,
            "analytical_mode_used": False,
            "detected_language": preferred_lang,
            "detected_intent": "narrative_retrieval",
            "selected_route": "narrative_error",
            "fallback_type": "retrieval_runtime_error",
            "fallback_reason": "retrieval_runtime_error",
            "controlled_response_state": "runtime_error",
            "rag_mode": rag_mode or "auto",
            "rag_mode_effective": "narrative_error",
            "file_ids": rag_file_ids,
            "retrieval_policy": {
                **budget_plan,
                "effective_top_k": selected_top_k,
                "expected_chunks_total": expected_chunks_total,
                "escalation": {"attempted": False, "applied": False, "reason": "retrieval_runtime_error"},
                "row_escalation": {"attempted": False, "applied": False, "reason": "retrieval_runtime_error"},
            },
            "retrieved_chunks_total": 0,
            "coverage": {
                "expected_chunks": expected_chunks_total,
                "retrieved_chunks": 0,
                "ratio": 0.0,
                "complete": bool(expected_chunks_total == 0),
            },
            "rows_expected_total": 0,
            "rows_retrieved_total": 0,
            "rows_used_map_total": 0,
            "rows_used_reduce_total": 0,
            "row_coverage_ratio": 0.0,
        }
        observe_retrieval_coverage(
            coverage_ratio=0.0,
            retrieval_mode="narrative_error",
            expected_chunks=int(expected_chunks_total),
            retrieved_chunks=0,
        )
        logger.error(
            (
                "rag_trace route=%s strategy=%s analytical_mode_used=false retrieval_mode=%s retrieval_k=%d "
                "retrieval_hits=0 avg_similarity=0.0000 context_tokens=0 uid=%s chat_id=%s file_ids=%s processing_ids=%s error=%s"
            ),
            "narrative_retrieval",
            rag_debug.get("strategy_mode"),
            "narrative_error",
            selected_top_k,
            str(user_id),
            str(conversation_id),
            ",".join(rag_file_ids),
            ",".join(processing_ids_by_file.values()),
            type(exc).__name__,
        )

    return final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources

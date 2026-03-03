from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.crud import crud_file
from app.rag.retriever import rag_retriever
from app.services.chat.context import merge_context_docs
from app.services.chat.embedding_config import group_files_by_embedding_config, resolve_rag_embedding_config
from app.services.chat.full_file_analysis import build_full_file_map_reduce_prompt
from app.services.chat.language import apply_language_policy_to_prompt, detect_preferred_response_language
from app.services.chat.postprocess import build_rag_caveats
from app.services.chat.retrieval_policy import build_retrieval_budget_plan, choose_escalation_plan
from app.services.chat.sources_debug import (
    build_row_coverage_stats,
    build_sources_list,
    build_sources_list_with_mode,
)
from app.services.chat.tabular_sql import execute_tabular_sql_path

logger = logging.getLogger(__name__)


async def _run_grouped_retrieval(
    *,
    rag_retriever_client,
    query: str,
    user_id: uuid.UUID,
    conversation_id: uuid.UUID,
    groups: Dict[Tuple[str, Optional[str]], List[str]],
    all_file_ids: List[str],
    top_k: int,
    rag_mode: Optional[str],
    embedding_mode: str,
    embedding_model: Optional[str],
    full_file_max_chunks: Optional[int] = None,
) -> List[Dict[str, Any]]:
    rag_results: List[Dict[str, Any]] = []

    async def _query_with_optional_full_file_max(
        *,
        query_text: str,
        top_k_value: int,
        conv_id,
        usr_id,
        ids: List[str],
        emb_mode: str,
        emb_model: Optional[str],
        mode: Optional[str],
    ) -> Any:
        kwargs: Dict[str, Any] = {
            "query": query_text,
            "top_k": top_k_value,
            "user_id": str(usr_id),
            "conversation_id": str(conv_id),
            "file_ids": ids,
            "embedding_mode": emb_mode,
            "embedding_model": emb_model,
            "rag_mode": mode,
            "debug_return": True,
        }
        if full_file_max_chunks is not None:
            kwargs["full_file_max_chunks"] = int(full_file_max_chunks)
        try:
            return await rag_retriever_client.query_rag(**kwargs)
        except TypeError:
            kwargs.pop("full_file_max_chunks", None)
            return await rag_retriever_client.query_rag(**kwargs)

    if len(groups) == 1:
        rag_result = await _query_with_optional_full_file_max(
            query_text=query,
            top_k_value=top_k,
            usr_id=user_id,
            conv_id=conversation_id,
            ids=all_file_ids,
            emb_mode=embedding_mode,
            emb_model=embedding_model,
            mode=rag_mode,
        )
        if isinstance(rag_result, dict):
            rag_results.append(rag_result)
        return rag_results

    logger.info("RAG mixed embeddings: groups=%d", len(groups))
    group_tasks = []
    for (group_mode, group_model), group_file_ids in groups.items():
        group_tasks.append(
            _query_with_optional_full_file_max(
                query_text=query,
                top_k_value=max(top_k, 4),
                usr_id=user_id,
                conv_id=conversation_id,
                ids=group_file_ids,
                emb_mode=group_mode,
                emb_model=group_model,
                mode=rag_mode,
            )
        )
    group_results = await asyncio.gather(*group_tasks, return_exceptions=True)
    for group_result in group_results:
        if isinstance(group_result, Exception):
            logger.warning("RAG group retrieval failed: %s", group_result)
            continue
        if isinstance(group_result, dict):
            rag_results.append(group_result)

    return rag_results


def _collect_context_and_debug(
    *,
    rag_results: List[Dict[str, Any]],
    non_full_file_top_k: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bool]:
    collected_docs: List[Dict[str, Any]] = []
    debug_groups: List[Dict[str, Any]] = []
    for rag_result in rag_results:
        docs = rag_result.get("docs") or []
        dbg = rag_result.get("debug") if isinstance(rag_result.get("debug"), dict) else {}
        collected_docs.extend(docs)
        debug_groups.append(dbg)

    is_full_file_mode = any(
        isinstance(dbg, dict) and (
            dbg.get("retrieval_mode") == "full_file"
            or dbg.get("intent") == "analyze_full_file"
        )
        for dbg in debug_groups
    )

    max_docs = int(settings.RAG_FULL_FILE_MAX_CHUNKS) if is_full_file_mode else max(non_full_file_top_k * 4, 32)
    context_docs = merge_context_docs(
        collected_docs,
        max_docs=max_docs,
        sort_by_score=not is_full_file_mode,
    )
    return context_docs, debug_groups, is_full_file_mode


async def build_rag_prompt(
    *,
    db: AsyncSession,
    user_id: Optional[uuid.UUID],
    conversation_id: uuid.UUID,
    query: str,
    top_k: int = 3,
    file_ids: Optional[List[str]] = None,
    model_source: Optional[str] = None,
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
):
    preferred_lang = detect_preferred_response_language(query)
    final_prompt = apply_language_policy_to_prompt(prompt=query, preferred_lang=preferred_lang)
    rag_used = False
    rag_debug = None
    context_docs: List[Dict[str, Any]] = []
    rag_caveats: List[str] = []
    rag_sources: List[str] = []

    if full_file_prompt_builder is None:
        full_file_prompt_builder = build_full_file_map_reduce_prompt
    if rag_caveats_builder is None:
        rag_caveats_builder = build_rag_caveats
    if crud_file_module is None:
        crud_file_module = crud_file
    if rag_retriever_client is None:
        rag_retriever_client = rag_retriever

    if not user_id:
        return final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources

    try:
        files = await crud_file_module.get_conversation_files(db, conversation_id=conversation_id, user_id=user_id)
        logger.info("Conversation files (completed): %d", len(files))
    except Exception as exc:
        logger.warning("Could not fetch conversation files: %s", exc)
        return final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources

    if file_ids:
        allowed_ids = {str(x) for x in file_ids}
        files = [file_obj for file_obj in files if str(file_obj.id) in allowed_ids]
        logger.info("Conversation files filtered by payload file_ids: %d", len(files))

    if not files:
        return final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources

    expected_chunks_total = sum(int(getattr(file_obj, "chunks_count", 0) or 0) for file_obj in files)
    tabular_sql_result = await execute_tabular_sql_path(query=query, files=files)
    if isinstance(tabular_sql_result, dict):
        rag_used = True
        rag_sources = list(tabular_sql_result.get("sources") or [])
        rag_debug = dict(tabular_sql_result.get("debug") or {})
        rag_debug["file_ids"] = [str(file_obj.id) for file_obj in files]
        rag_debug["rag_mode"] = rag_mode or "auto"
        rag_debug["rag_mode_effective"] = "tabular_sql"
        rag_debug["retrieval_policy"] = {
            "mode": "tabular_sql",
            "query_profile": "aggregate",
            "requested_top_k": top_k,
            "effective_top_k": 0,
            "expected_chunks_total": expected_chunks_total,
            "escalation": {"attempted": False, "applied": False, "reason": None},
            "row_escalation": {"attempted": False, "applied": False, "reason": None},
        }
        rag_debug["retrieved_chunks_total"] = expected_chunks_total
        rag_debug["coverage"] = {
            "expected_chunks": expected_chunks_total,
            "retrieved_chunks": expected_chunks_total,
            "ratio": 1.0 if expected_chunks_total > 0 else 0.0,
            "complete": True,
        }
        rag_debug["rows_expected_total"] = int(tabular_sql_result.get("rows_expected_total", 0) or 0)
        rag_debug["rows_retrieved_total"] = int(tabular_sql_result.get("rows_retrieved_total", 0) or 0)
        rag_debug["rows_used_map_total"] = int(tabular_sql_result.get("rows_used_map_total", 0) or 0)
        rag_debug["rows_used_reduce_total"] = int(tabular_sql_result.get("rows_used_reduce_total", 0) or 0)
        rag_debug["row_coverage_ratio"] = float(tabular_sql_result.get("row_coverage_ratio", 0.0) or 0.0)
        rag_debug["truncated"] = False
        final_prompt = apply_language_policy_to_prompt(
            preferred_lang=preferred_lang,
            prompt=(
                "You are a data analyst.\n"
                "Use deterministic SQL result below as source of truth.\n"
                "Do not change numbers from SQL output.\n"
                "Return sections in order: Answer, Limitations/Missing data, Sources.\n\n"
                f"User question:\n{query}\n\n"
                f"{tabular_sql_result.get('prompt_context')}\n\n"
                "Final answer:"
            ),
        )
        return final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources

    rag_file_ids = [str(file_obj.id) for file_obj in files]
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

    except TypeError:
        context_docs = await rag_retriever_client.query_rag(
            query,
            top_k=selected_top_k,
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            debug_return=True,
        )
        if isinstance(context_docs, dict) and "docs" in context_docs:
            context_docs_list = context_docs.get("docs") or []
            rag_debug = context_docs.get("debug")
            if context_docs_list:
                final_prompt = rag_retriever_client.build_context_prompt(query=query, context_documents=context_docs_list)
                final_prompt = apply_language_policy_to_prompt(prompt=final_prompt, preferred_lang=preferred_lang)
                rag_used = True
                rag_sources = build_sources_list(context_docs_list, max_items=12)
                rag_caveats = rag_caveats_builder(files=files, context_documents=context_docs_list, rag_debug=rag_debug)

    except Exception as exc:
        logger.warning("RAG retrieval failed: %s", exc)

    return final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources

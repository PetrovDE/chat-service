from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple
import uuid

from app.core.config import settings
from app.services.chat.context import merge_context_docs

logger = logging.getLogger(__name__)


async def run_grouped_retrieval(
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


def collect_context_and_debug(
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

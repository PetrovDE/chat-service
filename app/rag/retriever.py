from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from langchain_core.documents import Document

from app.core.config import settings
from app.observability.metrics import inc_counter, observe_ms
from app.rag.embeddings import EmbeddingsManager
from app.rag.retriever_helpers import (
    build_context_prompt as build_context_prompt_helper,
    build_where as build_where_helper,
    detect_intent as detect_intent_helper,
    lexical_scores as lexical_scores_helper,
    merge_hybrid as merge_hybrid_helper,
    rerank_with_langchain as rerank_with_langchain_helper,
    resolve_intent as resolve_intent_helper,
    rows_to_documents as rows_to_documents_helper,
    select_with_coverage as select_with_coverage_helper,
    tokenize as tokenize_helper,
)
from app.rag.vector_store import VectorStoreManager

logger = logging.getLogger(__name__)


TOKEN_RE = re.compile(r"[A-Za-zÐ-Ð¯Ð°-ÑÐÑ‘0-9_]+")
COMPARE_PATTERNS: List[str] = ["compare", "difference"]


@dataclass
class RetrievalDebug:
    where: Optional[Dict[str, Any]]
    top_k: int
    fetch_k: int
    raw_count: int
    returned_count: int


class RAGRetriever:
    def __init__(self) -> None:
        self.vectorstore = VectorStoreManager()

    def _tokenize(self, text: str) -> List[str]:
        return tokenize_helper(text, TOKEN_RE)

    def _detect_intent(self, query: str) -> str:
        return detect_intent_helper(
            query,
            compare_patterns=COMPARE_PATTERNS,
        )

    def _resolve_intent(
        self,
        *,
        query: str,
        query_intent: Optional[str],
        rag_mode: Optional[str],
        file_ids: Optional[List[str]],
    ) -> str:
        return resolve_intent_helper(
            query=query,
            query_intent=query_intent,
            rag_mode=rag_mode,
            file_ids=file_ids,
            detect_intent_fn=self._detect_intent,
        )

    def _build_where(
        self,
        *,
        conversation_id: Optional[str],
        chat_id: Optional[str] = None,
        user_id: Optional[str],
        file_ids: Optional[List[str]],
        processing_ids: Optional[List[str]] = None,
        sheet_names: Optional[List[str]] = None,
        chunk_types: Optional[List[str]] = None,
        namespace: Optional[str] = None,
        embedding_mode: Optional[str] = None,
        embedding_model: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return build_where_helper(
            conversation_id=conversation_id,
            chat_id=chat_id,
            user_id=user_id,
            file_ids=file_ids,
            processing_ids=processing_ids,
            sheet_names=sheet_names,
            chunk_types=chunk_types,
            namespace=namespace,
            embedding_mode=embedding_mode,
            embedding_model=embedding_model,
        )

    def _lexical_scores(self, query: str, rows: List[Dict[str, Any]]) -> Dict[str, float]:
        return lexical_scores_helper(query, rows, tokenize_fn=self._tokenize)

    def _merge_hybrid(
        self,
        *,
        dense_rows: List[Dict[str, Any]],
        lexical_rows: List[Dict[str, Any]],
        lexical_scores: Dict[str, float],
        dense_weight: float = 0.75,
        lexical_weight: float = 0.25,
    ) -> List[Dict[str, Any]]:
        return merge_hybrid_helper(
            dense_rows=dense_rows,
            lexical_rows=lexical_rows,
            lexical_scores_map=lexical_scores,
            dense_weight=dense_weight,
            lexical_weight=lexical_weight,
        )

    def _rows_to_documents(self, rows: List[Dict[str, Any]], *, score_key: str, default_score: float = 0.0) -> List[Document]:
        return rows_to_documents_helper(rows, score_key=score_key, default_score=default_score)

    def _rerank_with_langchain(
        self,
        *,
        query: str,
        dense_rows: List[Dict[str, Any]],
        lexical_rows: List[Dict[str, Any]],
        top_k: int,
    ) -> Optional[List[Document]]:
        return rerank_with_langchain_helper(
            query=query,
            dense_rows=dense_rows,
            lexical_rows=lexical_rows,
            top_k=top_k,
        )

    def _select_with_coverage(self, rows: List[Dict[str, Any]], top_k: int, per_file_min: int = 1) -> List[Dict[str, Any]]:
        return select_with_coverage_helper(rows, top_k=top_k, per_file_min=per_file_min)

    @staticmethod
    def _is_tabular_row(row: Dict[str, Any]) -> bool:
        meta = row.get("metadata") or {}
        file_type = str(meta.get("file_type") or "").lower()
        chunk_type = str(meta.get("chunk_type") or "").lower()
        return file_type in {"xlsx", "xls", "csv", "tsv"} or chunk_type in {"file_summary", "sheet_summary", "row_group"}

    def _staged_tabular_selection(self, rows: List[Dict[str, Any]], *, top_k: int) -> List[Dict[str, Any]]:
        if not rows:
            return []
        if not any(self._is_tabular_row(r) for r in rows):
            return rows

        file_summaries: List[Dict[str, Any]] = []
        sheet_summaries: List[Dict[str, Any]] = []
        row_groups: List[Dict[str, Any]] = []
        others: List[Dict[str, Any]] = []
        for row in rows:
            meta = row.get("metadata") or {}
            chunk_type = str(meta.get("chunk_type") or "").lower()
            if chunk_type == "file_summary":
                file_summaries.append(row)
            elif chunk_type == "sheet_summary":
                sheet_summaries.append(row)
            elif chunk_type == "row_group":
                row_groups.append(row)
            else:
                others.append(row)

        out: List[Dict[str, Any]] = []
        seen_ids = set()
        seen_ranges = set()
        sheet_buckets: Dict[Tuple[str, str], int] = {}

        def append_row(item: Dict[str, Any], *, max_per_sheet: Optional[int] = None) -> bool:
            row_id = str(item.get("id") or "")
            if row_id in seen_ids:
                return False
            meta = item.get("metadata") or {}
            chunk_type = str(meta.get("chunk_type") or "").lower()
            file_key = str(meta.get("file_id") or meta.get("source") or "")
            sheet_key = str(meta.get("sheet_name") or "")
            row_start = meta.get("row_start")
            row_end = meta.get("row_end")
            if chunk_type == "row_group" and row_start is not None and row_end is not None:
                range_key = (file_key, sheet_key, int(row_start), int(row_end))
                if range_key in seen_ranges:
                    return False
            if max_per_sheet is None and chunk_type == "row_group":
                max_per_sheet = 2
            if max_per_sheet is not None:
                sheet_bucket_key = (file_key, sheet_key)
                count = int(sheet_buckets.get(sheet_bucket_key, 0) or 0)
                if count >= max_per_sheet:
                    return False
                sheet_buckets[sheet_bucket_key] = count + 1
            if chunk_type == "row_group" and row_start is not None and row_end is not None:
                seen_ranges.add((file_key, sheet_key, int(row_start), int(row_end)))
            seen_ids.add(row_id)
            out.append(item)
            return True

        for item in file_summaries[: max(2, top_k // 4)]:
            if len(out) >= top_k:
                break
            append_row(item)

        for item in sheet_summaries[: max(4, top_k // 2)]:
            if len(out) >= top_k:
                break
            append_row(item, max_per_sheet=1)

        for item in row_groups:
            if len(out) >= top_k:
                break
            append_row(item, max_per_sheet=2)

        for item in others:
            if len(out) >= top_k:
                break
            append_row(item)

        if len(out) < top_k:
            for item in rows:
                if len(out) >= top_k:
                    break
                append_row(item)
        return out

    async def retrieve(
        self,
        query: str,
        *,
        top_k: int = 5,
        fetch_k: Optional[int] = None,
        conversation_id: Optional[str] = None,
        user_id: Optional[str] = None,
        file_ids: Optional[List[str]] = None,
        processing_ids: Optional[List[str]] = None,
        embedding_mode: str = "local",
        embedding_model: Optional[str] = None,
        score_threshold: Optional[float] = None,
        return_debug: bool = False,
        query_intent: Optional[str] = None,
        rag_mode: Optional[str] = None,
        full_file_max_chunks: Optional[int] = None,
        sheet_names: Optional[List[str]] = None,
        chunk_types: Optional[List[str]] = None,
        namespace: Optional[str] = None,
    ) -> Union[List[Document], Tuple[List[Document], RetrievalDebug]]:
        t0 = time.perf_counter()
        query = (query or "").strip()
        if not query:
            docs: List[Document] = []
            debug = RetrievalDebug(where=None, top_k=top_k, fetch_k=fetch_k or 0, raw_count=0, returned_count=0)
            return (docs, debug) if return_debug else docs

        intent = self._resolve_intent(
            query=query,
            query_intent=query_intent,
            rag_mode=rag_mode,
            file_ids=file_ids,
        )
        where = self._build_where(
            conversation_id=conversation_id,
            chat_id=conversation_id,
            user_id=user_id,
            file_ids=file_ids,
            processing_ids=processing_ids,
            sheet_names=sheet_names,
            chunk_types=chunk_types,
            namespace=namespace,
            embedding_mode=embedding_mode,
            embedding_model=embedding_model,
        )
        if score_threshold is None:
            score_threshold = float(getattr(settings, "RAG_SCORE_THRESHOLD", 0.0) or 0.0)

        if intent == "analyze_full_file":
            docs = await self.retrieve_full_file(
                query,
                conversation_id=conversation_id,
                user_id=user_id,
                file_ids=file_ids,
                processing_ids=processing_ids,
                embedding_mode=embedding_mode,
                embedding_model=embedding_model,
                max_chunks=full_file_max_chunks,
            )
            inc_counter("rag_retrieve_total", intent=intent, mode="full_file")
            observe_ms("rag_retrieve_duration_ms", (time.perf_counter() - t0) * 1000.0, intent=intent)
            debug = RetrievalDebug(
                where=where,
                top_k=top_k,
                fetch_k=fetch_k or 0,
                raw_count=len(docs),
                returned_count=len(docs),
            )
            return (docs, debug) if return_debug else docs

        embedder = EmbeddingsManager(mode=embedding_mode, model=embedding_model)
        t_embed = time.perf_counter()
        q_vecs = await embedder.embedd_documents_async([query])
        observe_ms("rag_embed_duration_ms", (time.perf_counter() - t_embed) * 1000.0, mode=embedding_mode)
        if not q_vecs:
            docs = []
            debug = RetrievalDebug(where=where, top_k=top_k, fetch_k=fetch_k or 0, raw_count=0, returned_count=0)
            inc_counter("rag_retrieve_total", intent=intent, mode="hybrid", result="empty_embedding")
            observe_ms("rag_retrieve_duration_ms", (time.perf_counter() - t0) * 1000.0, intent=intent)
            return (docs, debug) if return_debug else docs
        q_vec = q_vecs[0]

        if fetch_k is None:
            fetch_k = max(top_k * int(settings.RAG_FETCH_K_MULTIPLIER), int(settings.RAG_FETCH_K_MIN))

        lexical_pool_limit = max(
            int(settings.RAG_LEXICAL_POOL_MIN),
            fetch_k * int(settings.RAG_LEXICAL_POOL_MULTIPLIER),
        )
        lexical_pool_limit = min(lexical_pool_limit, int(settings.RAG_LEXICAL_POOL_MAX))

        logger.info("RAG.retrieve(hybrid): intent=%s top_k=%d fetch_k=%d where=%s", intent, top_k, fetch_k, where)

        t_denselex = time.perf_counter()
        dense_rows_task = asyncio.to_thread(
            self.vectorstore.query,
            embedding_query=q_vec,
            top_k=fetch_k,
            filter_dict=where,
        )
        lexical_pool_task = asyncio.to_thread(
            self.vectorstore.get_by_filter,
            filter_dict=where,
            limit_per_collection=lexical_pool_limit,
        )
        dense_rows, lexical_pool = await asyncio.gather(dense_rows_task, lexical_pool_task)
        observe_ms("rag_candidates_duration_ms", (time.perf_counter() - t_denselex) * 1000.0, mode="hybrid")

        t_rerank = time.perf_counter()
        lc_docs = await asyncio.to_thread(
            self._rerank_with_langchain,
            query=query,
            dense_rows=dense_rows,
            lexical_rows=lexical_pool,
            top_k=top_k,
        )
        observe_ms("rag_rerank_duration_ms", (time.perf_counter() - t_rerank) * 1000.0, backend="langchain")

        if lc_docs is not None:
            ranked_rows: List[Dict[str, Any]] = []
            for d in lc_docs:
                sim = float(d.metadata.get("similarity_score", 0.0))
                if score_threshold is not None and sim < float(score_threshold):
                    continue
                ranked_rows.append(
                    {
                        "id": d.metadata.get("chunk_id") or d.metadata.get("doc_id") or f"tmp_{len(ranked_rows)}",
                        "content": d.page_content,
                        "metadata": d.metadata,
                        "distance": d.metadata.get("distance", 1e9),
                        "dense_score": d.metadata.get("dense_score", 0.0),
                        "lexical_score": d.metadata.get("lexical_score", 0.0),
                        "hybrid_score": d.metadata.get("similarity_score", 0.0),
                    }
                )
            selected = self._select_with_coverage(ranked_rows, top_k=top_k, per_file_min=1)
            selected = self._staged_tabular_selection(selected, top_k=top_k)
            docs = self._rows_to_documents(selected, score_key="hybrid_score")
            merged_count = len(ranked_rows)
        else:
            lexical_scores = self._lexical_scores(query, lexical_pool)
            merged = self._merge_hybrid(
                dense_rows=dense_rows,
                lexical_rows=lexical_pool,
                lexical_scores=lexical_scores,
            )

            if score_threshold is not None:
                merged = [r for r in merged if float(r.get("hybrid_score", 0.0)) >= float(score_threshold)]

            staged = self._staged_tabular_selection(merged, top_k=max(top_k * 3, 12))
            selected = self._select_with_coverage(staged, top_k=top_k, per_file_min=1)
            docs = self._rows_to_documents(selected, score_key="hybrid_score")
            merged_count = len(merged)

        debug = RetrievalDebug(
            where=where,
            top_k=top_k,
            fetch_k=fetch_k,
            raw_count=merged_count,
            returned_count=len(docs),
        )
        inc_counter("rag_retrieve_total", intent=intent, mode="hybrid", result="ok")
        observe_ms("rag_retrieve_duration_ms", (time.perf_counter() - t0) * 1000.0, intent=intent)

        return (docs, debug) if return_debug else docs

    async def retrieve_full_file(
        self,
        query: str,
        *,
        conversation_id: Optional[str] = None,
        user_id: Optional[str] = None,
        file_ids: Optional[List[str]] = None,
        processing_ids: Optional[List[str]] = None,
        embedding_mode: Optional[str] = None,
        embedding_model: Optional[str] = None,
        max_chunks: Optional[int] = None,
        sheet_names: Optional[List[str]] = None,
        chunk_types: Optional[List[str]] = None,
        namespace: Optional[str] = None,
    ) -> List[Document]:
        t0 = time.perf_counter()
        _ = query
        max_chunks = int(max_chunks or settings.RAG_FULL_FILE_MAX_CHUNKS)
        where = self._build_where(
            conversation_id=conversation_id,
            chat_id=conversation_id,
            user_id=user_id,
            file_ids=file_ids,
            processing_ids=processing_ids,
            sheet_names=sheet_names,
            chunk_types=chunk_types,
            namespace=namespace,
            embedding_mode=embedding_mode,
            embedding_model=embedding_model,
        )
        rows = await asyncio.to_thread(
            self.vectorstore.get_by_filter,
            filter_dict=where,
            limit_per_collection=max_chunks,
        )
        full_file_limit_hit = bool(max_chunks and len(rows) >= max_chunks)

        def sort_key(item: Dict[str, Any]) -> Tuple[str, int]:
            meta = item.get("metadata") or {}
            file_id = str(meta.get("file_id") or "")
            idx_raw = meta.get("chunk_index", 0)
            try:
                idx = int(idx_raw)
            except Exception:
                idx = 0
            return file_id, idx

        rows.sort(key=sort_key)
        if max_chunks and len(rows) > max_chunks:
            rows = rows[:max_chunks]

        docs: List[Document] = []
        for r in rows:
            content = (r.get("content") or "").strip()
            if not content:
                continue
            meta = dict(r.get("metadata") or {})
            row_id = str(r.get("id") or "").strip()
            file_id = str(meta.get("file_id") or "").strip()
            if row_id and not str(meta.get("chunk_id") or "").strip():
                meta["chunk_id"] = row_id
            if file_id and not str(meta.get("doc_id") or "").strip():
                meta["doc_id"] = file_id
            meta["distance"] = 0.0
            meta["similarity_score"] = 1.0
            meta["retrieval_mode"] = "full_file"
            meta["full_file_max_chunks"] = max_chunks
            meta["full_file_limit_hit"] = full_file_limit_hit
            docs.append(Document(page_content=content, metadata=meta))

        logger.info("RAG.retrieve_full_file: where=%s chunks=%d", where, len(docs))
        observe_ms("rag_retrieve_full_file_duration_ms", (time.perf_counter() - t0) * 1000.0)
        return docs

    async def query_rag(
        self,
        query: str,
        *,
        top_k: int = 5,
        fetch_k: Optional[int] = None,
        conversation_id: Optional[str] = None,
        user_id: Optional[str] = None,
        file_ids: Optional[List[str]] = None,
        processing_ids: Optional[List[str]] = None,
        embedding_mode: str = "local",
        embedding_model: Optional[str] = None,
        score_threshold: Optional[float] = None,
        debug_return: bool = False,
        rag_mode: Optional[str] = None,
        full_file_max_chunks: Optional[int] = None,
        sheet_names: Optional[List[str]] = None,
        chunk_types: Optional[List[str]] = None,
        namespace: Optional[str] = None,
    ) -> Any:
        intent = self._resolve_intent(
            query=query,
            query_intent=None,
            rag_mode=rag_mode,
            file_ids=file_ids,
        )

        if not debug_return:
            docs = await self.retrieve(
                query,
                top_k=top_k,
                fetch_k=fetch_k,
                conversation_id=conversation_id,
                user_id=user_id,
                file_ids=file_ids,
                processing_ids=processing_ids,
                embedding_mode=embedding_mode,
                embedding_model=embedding_model,
                score_threshold=score_threshold,
                return_debug=False,
                query_intent=intent,
                rag_mode=rag_mode,
                full_file_max_chunks=full_file_max_chunks,
                sheet_names=sheet_names,
                chunk_types=chunk_types,
                namespace=namespace,
            )
            return [
                {
                    "content": d.page_content,
                    "metadata": d.metadata,
                    "distance": d.metadata.get("distance", 0.0),
                    "similarity_score": d.metadata.get("similarity_score", 0.0),
                }
                for d in docs
            ]

        docs, dbg = await self.retrieve(
            query,
            top_k=top_k,
            fetch_k=fetch_k,
            conversation_id=conversation_id,
            user_id=user_id,
            file_ids=file_ids,
            processing_ids=processing_ids,
            embedding_mode=embedding_mode,
            embedding_model=embedding_model,
            score_threshold=score_threshold,
            return_debug=True,
            query_intent=intent,
            rag_mode=rag_mode,
            full_file_max_chunks=full_file_max_chunks,
            sheet_names=sheet_names,
            chunk_types=chunk_types,
            namespace=namespace,
        )
        full_file_limit_hit = False
        full_file_max_chunks = None
        if docs and intent == "analyze_full_file":
            first_meta = docs[0].metadata or {}
            full_file_limit_hit = bool(first_meta.get("full_file_limit_hit", False))
            full_file_max_chunks = first_meta.get("full_file_max_chunks")

        return {
            "docs": [
                {
                    "content": d.page_content,
                    "metadata": d.metadata,
                    "distance": d.metadata.get("distance", 0.0),
                    "similarity_score": d.metadata.get("similarity_score", 0.0),
                }
                for d in docs
            ],
            "debug": {
                "where": dbg.where,
                "top_k": dbg.top_k,
                "fetch_k": dbg.fetch_k,
                "fetch_k_applicable": bool(intent != "analyze_full_file"),
                "raw_count": dbg.raw_count,
                "returned_count": dbg.returned_count,
                "score_threshold": score_threshold,
                "intent": intent,
                "retrieval_mode": "full_file" if intent == "analyze_full_file" else "hybrid",
                "full_file_limit_hit": full_file_limit_hit,
                "full_file_max_chunks": full_file_max_chunks,
            },
        }

    def build_context_prompt(self, *, query: str, context_documents: List[Dict[str, Any]]) -> str:
        return build_context_prompt_helper(query=query, context_documents=context_documents)


rag_retriever = RAGRetriever()


# app/rag/retriever.py
from __future__ import annotations

import logging
import math
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from langchain_core.documents import Document
from langchain_core.retrievers import BaseRetriever
from pydantic import Field

try:
    from langchain_community.retrievers import BM25Retriever
except Exception:  # pragma: no cover
    BM25Retriever = None

try:
    from langchain_classic.retrievers.ensemble import EnsembleRetriever
except Exception:  # pragma: no cover
    EnsembleRetriever = None

from app.rag.embeddings import EmbeddingsManager
from app.rag.vector_store import VectorStoreManager

logger = logging.getLogger(__name__)


TOKEN_RE = re.compile(r"[A-Za-zА-Яа-яЁё0-9_]+")
FULL_FILE_PATTERNS = [
    "весь файл",
    "всему файлу",
    "весь документ",
    "всему документу",
    "полный анализ",
    "проанализируй весь",
    "проанализировать весь",
    "analyze full file",
    "analyze entire file",
    "summarize full file",
    "whole file",
    "entire document",
]
COMPARE_PATTERNS = ["сравни", "сравнить", "compare", "difference", "разница", "чем отличается"]


@dataclass
class RetrievalDebug:
    where: Optional[Dict[str, Any]]
    top_k: int
    fetch_k: int
    raw_count: int
    returned_count: int


class StaticDenseRetriever(BaseRetriever):
    """
    LangChain-compatible retriever that returns pre-ranked dense documents.
    Used together with BM25 in EnsembleRetriever.
    """

    docs: List[Document] = Field(default_factory=list)
    k: int = 20

    def _get_relevant_documents(self, query: str) -> List[Document]:  # noqa: ARG002
        return list(self.docs[: self.k])


class RAGRetriever:
    def __init__(self) -> None:
        self.vectorstore = VectorStoreManager()

    def _tokenize(self, text: str) -> List[str]:
        return [t.lower() for t in TOKEN_RE.findall((text or "").lower()) if len(t) >= 2]

    def _detect_intent(self, query: str) -> str:
        q = (query or "").strip().lower()
        if not q:
            return "fact_lookup"

        if any(p in q for p in FULL_FILE_PATTERNS):
            return "analyze_full_file"
        if any(p in q for p in COMPARE_PATTERNS):
            return "compare_files"
        if any(x in q for x in ["почему", "как связ", "why", "how does", "reason"]):
            return "multi_hop"
        return "fact_lookup"

    def _build_where(
        self,
        *,
        conversation_id: Optional[str],
        user_id: Optional[str],
        file_ids: Optional[List[str]],
    ) -> Optional[Dict[str, Any]]:
        where: Dict[str, Any] = {}
        if file_ids:
            where["file_id"] = {"$in": [str(x) for x in file_ids]}
        elif conversation_id:
            where["conversation_id"] = conversation_id

        if user_id:
            where["user_id"] = user_id

        return where if where else None

    def _lexical_scores(self, query: str, rows: List[Dict[str, Any]]) -> Dict[str, float]:
        q_tokens = self._tokenize(query)
        if not q_tokens:
            return {}

        q_counter = Counter(q_tokens)
        n_docs = max(len(rows), 1)

        df = defaultdict(int)
        doc_tokens_cache: Dict[str, Counter] = {}
        for r in rows:
            doc_id = str(r.get("id") or "")
            tokens = self._tokenize(r.get("content") or "")
            c = Counter(tokens)
            doc_tokens_cache[doc_id] = c
            for t in c.keys():
                df[t] += 1

        scores: Dict[str, float] = {}
        for r in rows:
            doc_id = str(r.get("id") or "")
            d_counter = doc_tokens_cache.get(doc_id) or Counter()
            if not d_counter:
                continue

            d_len = sum(d_counter.values())
            denom = max(d_len, 1)
            score = 0.0

            for token, q_tf in q_counter.items():
                tf = d_counter.get(token, 0)
                if tf <= 0:
                    continue
                idf = math.log(1.0 + (n_docs - df[token] + 0.5) / (df[token] + 0.5))
                score += (tf / denom) * idf * (1.0 + 0.2 * q_tf)

            if score > 0:
                scores[doc_id] = score

        if not scores:
            return {}

        max_score = max(scores.values()) or 1.0
        return {k: float(v / max_score) for k, v in scores.items()}

    def _merge_hybrid(
        self,
        *,
        dense_rows: List[Dict[str, Any]],
        lexical_rows: List[Dict[str, Any]],
        lexical_scores: Dict[str, float],
        dense_weight: float = 0.75,
        lexical_weight: float = 0.25,
    ) -> List[Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}

        for r in dense_rows:
            doc_id = str(r.get("id") or "")
            dist = float(r.get("distance", 1e9))
            dense_sim = 1.0 / (1.0 + dist)
            meta = dict(r.get("metadata") or {})
            merged[doc_id] = {
                "id": doc_id,
                "content": (r.get("content") or "").strip(),
                "metadata": meta,
                "distance": dist,
                "dense_score": dense_sim,
                "lexical_score": 0.0,
            }

        for r in lexical_rows:
            doc_id = str(r.get("id") or "")
            if not doc_id:
                continue
            lex_score = float(lexical_scores.get(doc_id, 0.0))
            if lex_score <= 0:
                continue

            row = merged.get(doc_id)
            if row is None:
                row = {
                    "id": doc_id,
                    "content": (r.get("content") or "").strip(),
                    "metadata": dict(r.get("metadata") or {}),
                    "distance": 1e9,
                    "dense_score": 0.0,
                    "lexical_score": 0.0,
                }
                merged[doc_id] = row

            row["lexical_score"] = max(float(row.get("lexical_score", 0.0)), lex_score)

        out: List[Dict[str, Any]] = []
        for row in merged.values():
            if not row.get("content"):
                continue
            hybrid = dense_weight * float(row.get("dense_score", 0.0)) + lexical_weight * float(row.get("lexical_score", 0.0))
            row["hybrid_score"] = hybrid
            out.append(row)

        out.sort(key=lambda x: float(x.get("hybrid_score", 0.0)), reverse=True)
        return out

    def _rows_to_documents(self, rows: List[Dict[str, Any]], *, score_key: str, default_score: float = 0.0) -> List[Document]:
        docs: List[Document] = []
        for r in rows:
            content = (r.get("content") or "").strip()
            if not content:
                continue
            meta = dict(r.get("metadata") or {})
            meta["distance"] = float(r.get("distance", 1e9))
            meta["dense_score"] = float(r.get("dense_score", 0.0))
            meta["lexical_score"] = float(r.get("lexical_score", 0.0))
            meta["similarity_score"] = float(r.get(score_key, default_score))
            docs.append(Document(page_content=content, metadata=meta))
        return docs

    def _rerank_with_langchain(
        self,
        *,
        query: str,
        dense_rows: List[Dict[str, Any]],
        lexical_rows: List[Dict[str, Any]],
        top_k: int,
    ) -> Optional[List[Document]]:
        """
        Hybrid rerank via LangChain retrievers:
          1) Dense candidates -> StaticDenseRetriever
          2) Lexical candidates -> BM25Retriever
          3) EnsembleRetriever (RRF)
        Returns None if LangChain hybrid components are unavailable.
        """
        if BM25Retriever is None or EnsembleRetriever is None:
            return None

        dense_docs: List[Document] = []
        for r in dense_rows:
            doc_id = str(r.get("id") or "")
            content = (r.get("content") or "").strip()
            if not content:
                continue
            meta = dict(r.get("metadata") or {})
            dist = float(r.get("distance", 1e9))
            dense_sim = 1.0 / (1.0 + dist)
            if doc_id:
                meta["doc_id"] = doc_id
            meta["distance"] = dist
            meta["dense_score"] = dense_sim
            meta["similarity_score"] = dense_sim
            dense_docs.append(Document(page_content=content, metadata=meta))

        lexical_docs: List[Document] = []
        for r in lexical_rows:
            doc_id = str(r.get("id") or "")
            content = (r.get("content") or "").strip()
            if not content:
                continue
            meta = dict(r.get("metadata") or {})
            if doc_id:
                meta["doc_id"] = doc_id
            lexical_docs.append(Document(page_content=content, metadata=meta))

        if not dense_docs and not lexical_docs:
            return []

        retrievers: List[BaseRetriever] = []
        weights: List[float] = []

        if dense_docs:
            retrievers.append(StaticDenseRetriever(docs=dense_docs, k=max(top_k * 5, 30)))
            weights.append(0.75)
        if lexical_docs:
            bm25 = BM25Retriever.from_documents(lexical_docs)
            bm25.k = max(top_k * 6, 40)
            retrievers.append(bm25)
            weights.append(0.25)

        if not retrievers:
            return []
        if len(retrievers) == 1:
            return retrievers[0].invoke(query)[:top_k]

        ensemble = EnsembleRetriever(retrievers=retrievers, weights=weights)
        docs = ensemble.invoke(query)
        return docs[: max(top_k * 3, 20)]

    def _select_with_coverage(self, rows: List[Dict[str, Any]], top_k: int, per_file_min: int = 1) -> List[Dict[str, Any]]:
        if not rows:
            return []

        selected: List[Dict[str, Any]] = []
        used_ids = set()

        by_file: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in rows:
            file_id = str((r.get("metadata") or {}).get("file_id") or "")
            if file_id:
                by_file[file_id].append(r)

        if by_file and top_k >= len(by_file):
            for _, file_rows in by_file.items():
                take = 0
                for r in file_rows:
                    doc_id = str(r.get("id") or "")
                    if doc_id in used_ids:
                        continue
                    selected.append(r)
                    used_ids.add(doc_id)
                    take += 1
                    if take >= per_file_min:
                        break

        for r in rows:
            if len(selected) >= top_k:
                break
            doc_id = str(r.get("id") or "")
            if doc_id in used_ids:
                continue
            selected.append(r)
            used_ids.add(doc_id)

        return selected[:top_k]

    async def retrieve(
        self,
        query: str,
        *,
        top_k: int = 5,
        fetch_k: Optional[int] = None,
        conversation_id: Optional[str] = None,
        user_id: Optional[str] = None,
        file_ids: Optional[List[str]] = None,
        embedding_mode: str = "local",
        embedding_model: Optional[str] = None,
        score_threshold: Optional[float] = None,
        return_debug: bool = False,
        query_intent: Optional[str] = None,
    ) -> Union[List[Document], Tuple[List[Document], RetrievalDebug]]:
        query = (query or "").strip()
        if not query:
            docs: List[Document] = []
            debug = RetrievalDebug(where=None, top_k=top_k, fetch_k=fetch_k or 0, raw_count=0, returned_count=0)
            return (docs, debug) if return_debug else docs

        intent = query_intent or self._detect_intent(query)
        where = self._build_where(conversation_id=conversation_id, user_id=user_id, file_ids=file_ids)

        if intent == "analyze_full_file":
            docs = await self.retrieve_full_file(
                query,
                conversation_id=conversation_id,
                user_id=user_id,
                file_ids=file_ids,
            )
            debug = RetrievalDebug(
                where=where,
                top_k=top_k,
                fetch_k=fetch_k or 0,
                raw_count=len(docs),
                returned_count=len(docs),
            )
            return (docs, debug) if return_debug else docs

        embedder = EmbeddingsManager(mode=embedding_mode, model=embedding_model)
        q_vecs = await embedder.embedd_documents_async([query])
        if not q_vecs:
            docs = []
            debug = RetrievalDebug(where=where, top_k=top_k, fetch_k=fetch_k or 0, raw_count=0, returned_count=0)
            return (docs, debug) if return_debug else docs
        q_vec = q_vecs[0]

        if fetch_k is None:
            fetch_k = max(top_k * 12, 40)

        logger.info("RAG.retrieve(hybrid): intent=%s top_k=%d fetch_k=%d where=%s", intent, top_k, fetch_k, where)

        dense_rows = self.vectorstore.query(
            embedding_query=q_vec,
            top_k=fetch_k,
            filter_dict=where,
        )

        lexical_pool = self.vectorstore.get_by_filter(
            filter_dict=where,
            limit_per_collection=max(fetch_k * 6, 300),
        )
        lc_docs = self._rerank_with_langchain(
            query=query,
            dense_rows=dense_rows,
            lexical_rows=lexical_pool,
            top_k=top_k,
        )

        if lc_docs is not None:
            # Coverage-aware selection by file for LangChain-ranked docs
            ranked_rows: List[Dict[str, Any]] = []
            for d in lc_docs:
                sim = float(d.metadata.get("similarity_score", 0.0))
                if score_threshold is not None and sim < float(score_threshold):
                    continue
                ranked_rows.append(
                    {
                        "id": d.metadata.get("doc_id") or f"tmp_{len(ranked_rows)}",
                        "content": d.page_content,
                        "metadata": d.metadata,
                        "distance": d.metadata.get("distance", 1e9),
                        "dense_score": d.metadata.get("dense_score", 0.0),
                        "lexical_score": d.metadata.get("lexical_score", 0.0),
                        "hybrid_score": d.metadata.get("similarity_score", 0.0),
                    }
                )
            selected = self._select_with_coverage(ranked_rows, top_k=top_k, per_file_min=1)
            docs = self._rows_to_documents(selected, score_key="hybrid_score")
            merged_count = len(ranked_rows)
        else:
            # Fallback: internal hybrid scoring if LangChain retrievers are unavailable
            lexical_scores = self._lexical_scores(query, lexical_pool)
            merged = self._merge_hybrid(
                dense_rows=dense_rows,
                lexical_rows=lexical_pool,
                lexical_scores=lexical_scores,
            )

            if score_threshold is not None:
                merged = [r for r in merged if float(r.get("hybrid_score", 0.0)) >= float(score_threshold)]

            selected = self._select_with_coverage(merged, top_k=top_k, per_file_min=1)
            docs = self._rows_to_documents(selected, score_key="hybrid_score")
            merged_count = len(merged)

        debug = RetrievalDebug(
            where=where,
            top_k=top_k,
            fetch_k=fetch_k,
            raw_count=merged_count,
            returned_count=len(docs),
        )

        return (docs, debug) if return_debug else docs

    async def retrieve_full_file(
        self,
        query: str,
        *,
        conversation_id: Optional[str] = None,
        user_id: Optional[str] = None,
        file_ids: Optional[List[str]] = None,
        max_chunks: int = 800,
    ) -> List[Document]:
        _ = query
        where = self._build_where(conversation_id=conversation_id, user_id=user_id, file_ids=file_ids)
        rows = self.vectorstore.get_by_filter(filter_dict=where, limit_per_collection=max_chunks)

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
            meta["distance"] = 0.0
            meta["similarity_score"] = 1.0
            meta["retrieval_mode"] = "full_file"
            docs.append(Document(page_content=content, metadata=meta))

        logger.info("RAG.retrieve_full_file: where=%s chunks=%d", where, len(docs))
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
        embedding_mode: str = "local",
        embedding_model: Optional[str] = None,
        score_threshold: Optional[float] = None,
        debug_return: bool = False,
    ) -> Any:
        intent = self._detect_intent(query)

        if not debug_return:
            docs = await self.retrieve(
                query,
                top_k=top_k,
                fetch_k=fetch_k,
                conversation_id=conversation_id,
                user_id=user_id,
                file_ids=file_ids,
                embedding_mode=embedding_mode,
                embedding_model=embedding_model,
                score_threshold=score_threshold,
                return_debug=False,
                query_intent=intent,
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
            embedding_mode=embedding_mode,
            embedding_model=embedding_model,
            score_threshold=score_threshold,
            return_debug=True,
            query_intent=intent,
        )
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
                "raw_count": dbg.raw_count,
                "returned_count": dbg.returned_count,
                "score_threshold": score_threshold,
                "intent": intent,
                "retrieval_mode": "full_file" if intent == "analyze_full_file" else "hybrid",
            },
        }

    def build_context_prompt(self, *, query: str, context_documents: List[Dict[str, Any]]) -> str:
        parts: List[str] = []
        for i, d in enumerate(context_documents, start=1):
            meta = d.get("metadata") or {}
            filename = meta.get("filename") or meta.get("source") or "unknown"
            chunk_index = meta.get("chunk_index", "?")
            score = d.get("similarity_score", meta.get("similarity_score", 0.0))
            content = (d.get("content") or "").strip()
            if not content:
                continue
            parts.append(f"[{i}] file={filename} chunk={chunk_index} score={score:.4f}\n{content}")

        context_block = "\n\n---\n\n".join(parts)
        if context_block:
            return (
                "You are an assistant. Answer using only the provided file context when relevant.\n"
                "If required details are missing in context, explicitly say what is missing.\n\n"
                f"Question:\n{query}\n\n"
                f"Context:\n{context_block}\n\n"
                "Answer:"
            )
        return query


rag_retriever = RAGRetriever()

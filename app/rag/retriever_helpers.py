from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple

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


class StaticDenseRetriever(BaseRetriever):
    """
    LangChain-compatible retriever that returns pre-ranked dense documents.
    Used together with BM25 in EnsembleRetriever.
    """

    docs: List[Document] = Field(default_factory=list)
    k: int = 20

    def _get_relevant_documents(self, query: str) -> List[Document]:  # noqa: ARG002
        return list(self.docs[: self.k])


def tokenize(text: str, token_re: re.Pattern[str]) -> List[str]:
    return [t.lower() for t in token_re.findall((text or "").lower()) if len(t) >= 2]


def detect_intent(query: str, *, full_file_patterns: List[str], compare_patterns: List[str]) -> str:
    q = (query or "").strip().lower()
    if not q:
        return "fact_lookup"

    if any(p in q for p in full_file_patterns):
        return "analyze_full_file"
    if any(p in q for p in compare_patterns):
        return "compare_files"
    if any(x in q for x in ["почему", "как связ", "why", "how does", "reason"]):
        return "multi_hop"
    return "fact_lookup"


def resolve_intent(
    *,
    query: str,
    query_intent: Optional[str],
    rag_mode: Optional[str],
    file_ids: Optional[List[str]],
    detect_intent_fn,
) -> str:
    mode = (rag_mode or "").strip().lower()
    if mode == "full_file":
        return "analyze_full_file"
    if mode == "hybrid":
        return "fact_lookup"

    detected = query_intent or detect_intent_fn(query)

    q = (query or "").strip().lower()
    broad_terms = ["анализ", "разбери", "разбор", "свод", "итог", "обзор", "analyze", "summary", "overview"]
    file_terms = ["файл", "документ", "sheet", "таблиц", "строк"]
    if file_ids and detected == "fact_lookup":
        if any(t in q for t in broad_terms) and any(t in q for t in file_terms):
            return "analyze_full_file"

    return detected


def build_where(
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


def lexical_scores(query: str, rows: List[Dict[str, Any]], tokenize_fn) -> Dict[str, float]:
    q_tokens = tokenize_fn(query)
    if not q_tokens:
        return {}

    q_counter = Counter(q_tokens)
    n_docs = max(len(rows), 1)

    df = defaultdict(int)
    doc_tokens_cache: Dict[str, Counter] = {}
    for r in rows:
        doc_id = str(r.get("id") or "")
        tokens = tokenize_fn(r.get("content") or "")
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


def merge_hybrid(
    *,
    dense_rows: List[Dict[str, Any]],
    lexical_rows: List[Dict[str, Any]],
    lexical_scores_map: Dict[str, float],
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
        lex_score = float(lexical_scores_map.get(doc_id, 0.0))
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


def rows_to_documents(rows: List[Dict[str, Any]], *, score_key: str, default_score: float = 0.0) -> List[Document]:
    docs: List[Document] = []
    for r in rows:
        content = (r.get("content") or "").strip()
        if not content:
            continue
        row_id = str(r.get("id") or "").strip()
        meta = dict(r.get("metadata") or {})
        file_id = str(meta.get("file_id") or "").strip()
        if row_id and not str(meta.get("chunk_id") or "").strip():
            meta["chunk_id"] = row_id
        if file_id and not str(meta.get("doc_id") or "").strip():
            meta["doc_id"] = file_id
        meta["distance"] = float(r.get("distance", 1e9))
        meta["dense_score"] = float(r.get("dense_score", 0.0))
        meta["lexical_score"] = float(r.get("lexical_score", 0.0))
        meta["similarity_score"] = float(r.get(score_key, default_score))
        docs.append(Document(page_content=content, metadata=meta))
    return docs


def rerank_with_langchain(
    *,
    query: str,
    dense_rows: List[Dict[str, Any]],
    lexical_rows: List[Dict[str, Any]],
    top_k: int,
) -> Optional[List[Document]]:
    if BM25Retriever is None or EnsembleRetriever is None:
        return None

    dense_docs: List[Document] = []
    for r in dense_rows:
        row_id = str(r.get("id") or "")
        content = (r.get("content") or "").strip()
        if not content:
            continue
        meta = dict(r.get("metadata") or {})
        dist = float(r.get("distance", 1e9))
        dense_sim = 1.0 / (1.0 + dist)
        file_id = str(meta.get("file_id") or "").strip()
        if row_id and not str(meta.get("chunk_id") or "").strip():
            meta["chunk_id"] = row_id
        if file_id and not str(meta.get("doc_id") or "").strip():
            meta["doc_id"] = file_id
        meta["distance"] = dist
        meta["dense_score"] = dense_sim
        meta["similarity_score"] = dense_sim
        dense_docs.append(Document(page_content=content, metadata=meta))

    lexical_docs: List[Document] = []
    for r in lexical_rows:
        row_id = str(r.get("id") or "")
        content = (r.get("content") or "").strip()
        if not content:
            continue
        meta = dict(r.get("metadata") or {})
        file_id = str(meta.get("file_id") or "").strip()
        if row_id and not str(meta.get("chunk_id") or "").strip():
            meta["chunk_id"] = row_id
        if file_id and not str(meta.get("doc_id") or "").strip():
            meta["doc_id"] = file_id
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


def select_with_coverage(rows: List[Dict[str, Any]], top_k: int, per_file_min: int = 1) -> List[Dict[str, Any]]:
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


def build_context_prompt(*, query: str, context_documents: List[Dict[str, Any]]) -> str:
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
            "You are an assistant. Build a detailed answer from the provided file context.\n"
            "Return three sections in this exact order:\n"
            "1) Ответ\n"
            "2) Ограничения/нехватка данных\n"
            "3) Источники (кратко)\n"
            "If details are missing in context, explicitly list what is missing.\n\n"
            f"Question:\n{query}\n\n"
            f"Context:\n{context_block}\n\n"
            "Answer:"
        )
    return query

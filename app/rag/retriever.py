# app/rag/retriever.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from langchain_core.documents import Document

from app.rag.embeddings import EmbeddingsManager
from app.rag.vector_store import VectorStoreManager

logger = logging.getLogger(__name__)


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

    async def retrieve(
        self,
        query: str,
        *,
        top_k: int = 5,
        fetch_k: Optional[int] = None,
        conversation_id: Optional[str] = None,
        user_id: Optional[str] = None,
        file_ids: Optional[List[str]] = None,          # NEW
        embedding_mode: str = "local",
        embedding_model: Optional[str] = None,
        score_threshold: Optional[float] = None,
        return_debug: bool = False,
    ) -> Union[List[Document], Tuple[List[Document], RetrievalDebug]]:
        query = (query or "").strip()
        if not query:
            docs: List[Document] = []
            debug = RetrievalDebug(where=None, top_k=top_k, fetch_k=fetch_k or 0, raw_count=0, returned_count=0)
            return (docs, debug) if return_debug else docs

        embedder = EmbeddingsManager(mode=embedding_mode, model=embedding_model)
        q_vecs = await embedder.embedd_documents_async([query])
        if not q_vecs:
            docs = []
            debug = RetrievalDebug(where=None, top_k=top_k, fetch_k=fetch_k or 0, raw_count=0, returned_count=0)
            return (docs, debug) if return_debug else docs
        q_vec = q_vecs[0]

        # Build where filter:
        # Prefer file_ids (truth from SQL conversation_files) over conversation_id from chunk metadata
        where: Dict[str, Any] = {}
        if file_ids:
            where["file_id"] = {"$in": [str(x) for x in file_ids]}
        elif conversation_id:
            where["conversation_id"] = conversation_id

        if user_id:
            where["user_id"] = user_id

        if fetch_k is None:
            fetch_k = max(top_k * 10, 30)

        logger.info("RAG.retrieve: top_k=%d fetch_k=%d where=%s", top_k, fetch_k, where if where else None)

        raw = self.vectorstore.query(
            embedding_query=q_vec,
            top_k=fetch_k,
            filter_dict=where if where else None,
        )

        try:
            raw = sorted(raw, key=lambda x: float(x.get("distance", 1e9)))
        except Exception:
            pass

        docs: List[Document] = []
        for r in raw:
            dist = float(r.get("distance", 0.0))
            sim = 1.0 / (1.0 + dist)

            if score_threshold is not None and sim < float(score_threshold):
                continue

            meta = dict(r.get("metadata") or {})
            meta["distance"] = dist
            meta["similarity_score"] = sim

            content = (r.get("content") or "").strip()
            if not content:
                continue

            docs.append(Document(page_content=content, metadata=meta))
            if len(docs) >= top_k:
                break

        debug = RetrievalDebug(
            where=where if where else None,
            top_k=top_k,
            fetch_k=fetch_k,
            raw_count=len(raw),
            returned_count=len(docs),
        )

        return (docs, debug) if return_debug else docs

    async def query_rag(
        self,
        query: str,
        *,
        top_k: int = 5,
        fetch_k: Optional[int] = None,
        conversation_id: Optional[str] = None,
        user_id: Optional[str] = None,
        file_ids: Optional[List[str]] = None,          # NEW
        embedding_mode: str = "local",
        embedding_model: Optional[str] = None,
        score_threshold: Optional[float] = None,
        debug_return: bool = False,
    ) -> Any:
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
                "Ты — помощник. Отвечай строго на основе контекста из файлов, если он релевантен.\n"
                "Если в контексте нет нужной информации — прямо скажи, что в прикрепленных файлах этого нет.\n\n"
                f"Вопрос:\n{query}\n\n"
                f"Контекст:\n{context_block}\n\n"
                "Ответ:"
            )
        return query


rag_retriever = RAGRetriever()

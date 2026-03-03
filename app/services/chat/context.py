from __future__ import annotations

import hashlib
from typing import Any, Dict, List


def build_conversation_history(messages) -> List[Dict[str, str]]:
    return [{"role": msg.role, "content": msg.content} for msg in messages[:-1]]


def merge_context_docs(
    docs: List[Dict[str, Any]],
    max_docs: int = 64,
    *,
    sort_by_score: bool = True,
) -> List[Dict[str, Any]]:
    if not docs:
        return []

    merged: List[Dict[str, Any]] = []
    seen = set()

    for doc in docs:
        meta = doc.get("metadata") or {}
        content = (doc.get("content") or "")
        content_hash = hashlib.sha1(content.encode("utf-8", errors="ignore")).hexdigest()[:16] if content else ""
        file_id = str(meta.get("file_id") or "")
        chunk_index = meta.get("chunk_index")
        doc_id = str(meta.get("doc_id") or meta.get("chunk_id") or "").strip()
        sheet_name = str(meta.get("sheet_name") or "")
        row_start = meta.get("row_start")
        row_end = meta.get("row_end")
        key = (
            doc_id,
            file_id,
            "" if chunk_index is None else str(chunk_index),
            sheet_name,
            "" if row_start is None else str(row_start),
            "" if row_end is None else str(row_end),
            content_hash,
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(doc)

    if sort_by_score:
        merged.sort(key=lambda x: float(x.get("similarity_score", 0.0)), reverse=True)
    return merged[:max_docs] if max_docs and max_docs > 0 else merged


def build_rag_conversation_memory(history: List[Dict[str, str]], max_messages: int = 6) -> List[Dict[str, str]]:
    if not history:
        return []
    tail = history[-max_messages:]
    return [{"role": m.get("role", "user"), "content": (m.get("content") or "")[:1500]} for m in tail]

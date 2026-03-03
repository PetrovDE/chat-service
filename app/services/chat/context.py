from __future__ import annotations

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
        key = (
            str(meta.get("file_id") or ""),
            str(meta.get("chunk_index") or ""),
            (doc.get("content") or "")[:120],
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

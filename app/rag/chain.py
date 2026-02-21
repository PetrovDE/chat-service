"""
LangChain RAG Chain (Runnable)

Основа взята из твоей текущей версии :contentReference[oaicite:4]{index=4}, изменения точечные:

- ADD: llm адаптер поддерживает и messages-стиль, и prompt-only (как у llm_manager.generate_response)
- ADD: режим return_prompt_only=True — удобно для /stream:
      chain вернёт final_prompt + debug, а стрим генерируешь как раньше.
- KEEP: runnable структура (retrieve -> format -> prompt/messages -> llm)
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Callable, Dict, List, Optional, Tuple

from langchain_core.documents import Document
from langchain_core.runnables import RunnableLambda, RunnablePassthrough

from app.rag.retriever import rag_retriever, RetrievalDebug


def _format_docs(docs: List[Document], max_chars: int = 12000) -> Tuple[str, int]:
    parts: List[str] = []
    used = 0
    for i, d in enumerate(docs, start=1):
        chunk = d.page_content.strip()
        if not chunk:
            continue

        header = (
            f"[{i}] file={d.metadata.get('filename', 'unknown')} "
            f"chunk={d.metadata.get('chunk_index', '?')} "
            f"score={d.metadata.get('similarity_score', 0):.4f}"
        )
        block = f"{header}\n{chunk}\n"
        if used + len(block) > max_chars:
            remain = max_chars - used
            if remain <= 0:
                break
            block = block[:remain]
        parts.append(block)
        used += len(block)
        if used >= max_chars:
            break

    return ("\n---\n".join(parts), used)


def _build_messages(system_prompt: str, user_question: str, context: str) -> List[Dict[str, str]]:
    if context:
        user_content = (
            f"Вопрос пользователя:\n{user_question}\n\n"
            f"Контекст из прикреплённых файлов (цитаты):\n{context}\n\n"
            f"Ответь, опираясь на контекст. Если данных в контексте нет — скажи, что в файлах этого нет."
        )
    else:
        user_content = user_question

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]


def _messages_to_prompt(messages: List[Dict[str, str]]) -> str:
    """
    ADD: если llm_callable принимает только prompt=..., превращаем messages в строку.
    """
    chunks = []
    for m in messages:
        role = m.get("role", "user")
        content = m.get("content", "")
        chunks.append(f"{role.upper()}:\n{content}")
    return "\n\n".join(chunks)


async def _call_llm(llm_callable: Callable[..., Any], messages: List[Dict[str, str]]) -> str:
    """
    ADD: поддерживает 2 стиля:
      - llm_callable(messages=...) -> str|dict
      - llm_callable(prompt=...)   -> str|dict
    """
    # 1) попробуем messages=
    try:
        out = await llm_callable(messages=messages)
    except TypeError:
        # 2) fallback на prompt=
        prompt = _messages_to_prompt(messages)
        out = await llm_callable(prompt=prompt)

    if isinstance(out, str):
        return out
    if isinstance(out, dict):
        if "content" in out:
            return str(out["content"])
        if "response" in out:
            return str(out["response"])
    return str(out)


def build_rag_chain(
    *,
    llm_callable: Callable[..., Any],
    system_prompt: str,
    default_top_k: int = 5,
    default_fetch_k: int = 30,
    max_context_chars: int = 12000,
    return_prompt_only: bool = False,
):
    """
    Вход:
      {
        "question": str,
        "conversation_id": str|None,
        "user_id": str|None,
        "embedding_mode": str,
        "embedding_model": str|None,
        "top_k": int|None,
        "fetch_k": int|None,
        "score_threshold": float|None
      }

    Выход:
      return_prompt_only=False:
        { "answer": str, "docs": [...], "debug": {...} }

      return_prompt_only=True:
        { "final_prompt": str, "docs": [...], "debug": {...} }
    """

    async def _retrieve_step(inp: Dict[str, Any]) -> Dict[str, Any]:
        question = (inp.get("question") or "").strip()
        top_k = int(inp.get("top_k") or default_top_k)
        fetch_k = int(inp.get("fetch_k") or default_fetch_k)
        score_threshold = inp.get("score_threshold")

        docs, dbg = await rag_retriever.retrieve(
            question,
            top_k=top_k,
            fetch_k=fetch_k,
            conversation_id=inp.get("conversation_id"),
            user_id=inp.get("user_id"),
            embedding_mode=inp.get("embedding_mode") or "local",
            embedding_model=inp.get("embedding_model"),
            score_threshold=score_threshold,
            return_debug=True,
        )

        context, used_chars = _format_docs(docs, max_chars=max_context_chars)

        return {
            **inp,
            "docs": docs,
            "retrieval_debug": dbg,
            "context": context,
            "context_chars": used_chars,
        }

    async def _final_step(inp: Dict[str, Any]) -> Dict[str, Any]:
        messages = _build_messages(system_prompt, inp.get("question", ""), inp.get("context", ""))
        final_prompt = _messages_to_prompt(messages)

        docs_preview = []
        for d in inp.get("docs") or []:
            docs_preview.append(
                {
                    "filename": d.metadata.get("filename"),
                    "chunk_index": d.metadata.get("chunk_index"),
                    "similarity_score": d.metadata.get("similarity_score"),
                    "distance": d.metadata.get("distance"),
                    "content_preview": (d.page_content[:400] + "…") if len(d.page_content) > 400 else d.page_content,
                    "metadata": {k: v for k, v in d.metadata.items() if k != "content"},
                }
            )

        dbg: RetrievalDebug = inp.get("retrieval_debug")
        debug_payload = {
            **(asdict(dbg) if dbg else {}),
            "context_chars": inp.get("context_chars", 0),
        }

        if return_prompt_only:
            return {
                "final_prompt": final_prompt,
                "docs": docs_preview,
                "debug": debug_payload,
            }

        answer = await _call_llm(llm_callable, messages)
        return {
            "answer": answer,
            "docs": docs_preview,
            "debug": debug_payload,
        }

    chain = RunnablePassthrough() | RunnableLambda(_retrieve_step) | RunnableLambda(_final_step)
    return chain

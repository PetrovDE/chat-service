from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.crud import crud_conversation, crud_file, crud_message
from app.db.models import User
from app.rag.retriever import rag_retriever
from app.schemas import ChatMessage, ChatResponse
from app.services.llm.manager import llm_manager

logger = logging.getLogger(__name__)


def _build_conversation_history(messages):
    return [{"role": msg.role, "content": msg.content} for msg in messages[:-1]]


def _normalize_source(source: Optional[str]) -> str:
    src = (source or "").strip().lower()
    if src == "corporate":
        return "aihub"
    if src in ("aihub", "openai", "ollama", "local"):
        return src
    return "local"


def _parse_file_embedding_meta(raw_value: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    raw = (raw_value or "").strip()
    if not raw:
        return None, None

    if ":" in raw:
        mode_raw, model_raw = raw.split(":", 1)
        mode = _normalize_source(mode_raw)
        model = model_raw.strip() or None
        if mode in ("local", "ollama", "aihub"):
            return ("local" if mode == "ollama" else mode), model

    return None, raw


def _resolve_rag_embedding_config(files, requested_model_source: Optional[str]) -> Tuple[str, Optional[str]]:
    fallback_mode = "aihub" if _normalize_source(requested_model_source) == "aihub" else "local"

    first_model_only: Optional[str] = None
    for f in files:
        mode, model = _parse_file_embedding_meta(getattr(f, "embedding_model", None))
        if model and not first_model_only:
            first_model_only = model
        if mode:
            return mode, model

    return fallback_mode, first_model_only


def _group_files_by_embedding_config(
    files,
    requested_model_source: Optional[str],
) -> Dict[Tuple[str, Optional[str]], List[str]]:
    fallback_mode = "aihub" if _normalize_source(requested_model_source) == "aihub" else "local"
    groups: Dict[Tuple[str, Optional[str]], List[str]] = {}

    for f in files:
        mode, model = _parse_file_embedding_meta(getattr(f, "embedding_model", None))
        resolved_mode = mode or fallback_mode
        key = (resolved_mode, model)
        groups.setdefault(key, []).append(str(f.id))

    return groups


def _merge_context_docs(
    docs: List[Dict[str, Any]],
    max_docs: int = 64,
    *,
    sort_by_score: bool = True,
) -> List[Dict[str, Any]]:
    if not docs:
        return []

    merged: List[Dict[str, Any]] = []
    seen = set()

    for d in docs:
        meta = d.get("metadata") or {}
        key = (
            str(meta.get("file_id") or ""),
            str(meta.get("chunk_index") or ""),
            (d.get("content") or "")[:120],
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(d)

    if sort_by_score:
        merged.sort(key=lambda x: float(x.get("similarity_score", 0.0)), reverse=True)
    return merged[:max_docs] if max_docs and max_docs > 0 else merged


def _build_rag_conversation_memory(history: List[Dict[str, str]], max_messages: int = 6) -> List[Dict[str, str]]:
    if not history:
        return []
    tail = history[-max_messages:]
    return [{"role": m.get("role", "user"), "content": (m.get("content") or "")[:1500]} for m in tail]


def _build_sources_list(context_documents: List[Dict[str, Any]], max_items: int = 8) -> List[str]:
    out: List[str] = []
    seen = set()
    for d in context_documents:
        meta = d.get("metadata") or {}
        filename = str(meta.get("filename") or meta.get("source") or "unknown")
        sheet = meta.get("sheet_name")
        chunk_index = meta.get("chunk_index", "?")
        row_start = meta.get("row_start")
        row_end = meta.get("row_end")
        row_part = ""
        if row_start is not None and row_end is not None:
            row_part = f" | rows={row_start}-{row_end}"
        if sheet:
            src = f"{filename} | sheet={sheet} | chunk={chunk_index}{row_part}"
        else:
            src = f"{filename} | chunk={chunk_index}{row_part}"
        if src in seen:
            continue
        seen.add(src)
        out.append(src)
        if len(out) >= max_items:
            break
    return out


def _build_top_chunks_debug(context_documents: List[Dict[str, Any]], max_items: int = 8) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    docs = context_documents if (max_items is None or max_items <= 0) else context_documents[:max_items]
    for d in docs:
        meta = d.get("metadata") or {}
        file_id = str(meta.get("file_id") or "")
        chunk_index = meta.get("chunk_index")
        doc_id = str(meta.get("doc_id") or "").strip()
        if not doc_id and file_id and chunk_index is not None:
            doc_id = f"{file_id}_{chunk_index}"
        rows.append(
            {
                "score": float(d.get("similarity_score", meta.get("similarity_score", 0.0)) or 0.0),
                "file_id": file_id,
                "doc_id": doc_id or None,
                "chunk_id": doc_id or None,
                "filename": str(meta.get("filename") or meta.get("source") or "unknown"),
                "sheet_name": meta.get("sheet_name"),
                "chunk_index": chunk_index,
                "row_start": meta.get("row_start"),
                "row_end": meta.get("row_end"),
                "total_rows": meta.get("total_rows"),
                "preview": (d.get("content") or "")[:220],
            }
        )
    return rows


def _estimate_text_tokens(text: str) -> int:
    # Fast, provider-agnostic approximation for observability/debug.
    if not text:
        return 0
    return max(1, int(len(text) / 4))


def _build_standard_rag_debug_payload(
    *,
    rag_debug: Optional[Dict[str, Any]],
    context_docs: List[Dict[str, Any]],
    rag_sources: List[str],
    llm_tokens_used: Optional[int],
    max_items: int = 8,
) -> Dict[str, Any]:
    payload = dict(rag_debug or {})
    top_chunks = _build_top_chunks_debug(context_docs, max_items=max_items)
    avg_score = (
        sum(float(item.get("score", 0.0) or 0.0) for item in top_chunks) / len(top_chunks)
        if top_chunks
        else 0.0
    )
    context_tokens = sum(_estimate_text_tokens((d.get("content") or "")) for d in context_docs)
    payload["filters"] = payload.get("filters") or payload.get("where")
    payload["top_chunks"] = top_chunks
    payload["top_chunks_limit"] = max_items
    payload["top_chunks_total"] = len(context_docs)
    payload["sources"] = rag_sources
    payload["retrieval_hits"] = int(payload.get("returned_count", len(context_docs)) or 0)
    payload["retrieved_chunks_total"] = len(context_docs)
    payload["avg_score"] = float(avg_score)
    payload["context_tokens"] = int(context_tokens)
    payload["llm_tokens_used"] = llm_tokens_used
    return payload


def _build_rag_caveats(
    *,
    files: List[Any],
    context_documents: List[Dict[str, Any]],
    rag_debug: Optional[Dict[str, Any]],
) -> List[str]:
    caveats: List[str] = []
    partial_files = []
    for f in files:
        status = str(getattr(f, "is_processed", "") or "")
        if status == "partial_success":
            progress = {}
            custom_meta = getattr(f, "custom_metadata", None)
            if isinstance(custom_meta, dict):
                progress = custom_meta.get("ingestion_progress") if isinstance(custom_meta.get("ingestion_progress"), dict) else {}
            expected = int(progress.get("total_chunks_expected", 0) or 0)
            failed = int(progress.get("chunks_failed", 0) or 0)
            partial_files.append(f"{getattr(f, 'original_filename', 'unknown')} (bad={failed}, expected={expected})")

    if partial_files:
        caveats.append("Some files were indexed partially: " + "; ".join(partial_files[:5]))
    if not context_documents:
        caveats.append("No relevant chunks were retrieved for this query.")
    coverage = rag_debug.get("coverage") if isinstance(rag_debug, dict) and isinstance(rag_debug.get("coverage"), dict) else {}
    if coverage:
        expected = int(coverage.get("expected_chunks", 0) or 0)
        retrieved = int(coverage.get("retrieved_chunks", 0) or 0)
        complete = bool(coverage.get("complete", False))
        if expected > 0 and not complete:
            caveats.append(
                f"Full-file coverage is incomplete: retrieved {retrieved}/{expected} chunks."
            )
    if isinstance(rag_debug, dict) and rag_debug.get("truncated"):
        caveats.append("Context was truncated by retrieval limits; answer may be incomplete.")
    return caveats


def _append_caveats_and_sources(answer: str, caveats: List[str], sources: List[str]) -> str:
    lines = [answer.strip()]
    lines.append("\n\n### Ограничения/нехватка данных")
    if caveats:
        for c in caveats:
            lines.append(f"- {c}")
    else:
        lines.append("- Существенных ограничений контекста не обнаружено.")
    lines.append("\n### Источники (кратко)")
    if sources:
        for s in sources:
            lines.append(f"- {s}")
    else:
        lines.append("- Релевантные источники не найдены.")
    return "\n".join(lines).strip()


def _build_critic_context(context_documents: List[Dict[str, Any]], max_chars: int = 12000) -> str:
    parts: List[str] = []
    used = 0
    for i, d in enumerate(context_documents, start=1):
        meta = d.get("metadata") or {}
        filename = meta.get("filename") or "unknown"
        chunk_index = meta.get("chunk_index", "?")
        content = (d.get("content") or "").strip()
        if not content:
            continue
        block = f"[{i}] file={filename} chunk={chunk_index}\n{content}\n"
        if used + len(block) > max_chars:
            remain = max_chars - used
            if remain <= 0:
                break
            block = block[:remain]
        parts.append(block)
        used += len(block)
        if used >= max_chars:
            break
    return "\n---\n".join(parts)


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    raw = (text or "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None

    candidate = raw[start:end + 1]
    try:
        return json.loads(candidate)
    except Exception:
        return None


async def _run_answer_critic(
    *,
    query: str,
    answer: str,
    context_documents: List[Dict[str, Any]],
    model_source: Optional[str],
    model_name: Optional[str],
) -> Tuple[str, Dict[str, Any]]:
    context_text = _build_critic_context(context_documents, max_chars=12000)
    if not context_text:
        return answer, {"enabled": True, "applied": False, "reason": "empty_context"}

    critic_prompt = (
        "You are an answer quality critic for RAG.\n"
        "Given user question, draft answer, and evidence context, evaluate factual support.\n"
        "Return STRICT JSON object with fields:\n"
        "supported: boolean,\n"
        "issues: array of short strings,\n"
        "missing_points: array of short strings,\n"
        "refined_answer: string,\n"
        "confidence: number (0..1).\n"
        "Do not return markdown.\n\n"
        f"Question:\n{query}\n\n"
        f"Draft answer:\n{answer}\n\n"
        f"Evidence context:\n{context_text}\n\n"
        "JSON:"
    )

    try:
        critic = await llm_manager.generate_response(
            prompt=critic_prompt,
            model_source=model_source,
            model_name=model_name,
            temperature=0.0,
            max_tokens=1200,
            conversation_history=None,
        )
    except Exception as e:
        logger.warning("Critic step failed: %s", e)
        return answer, {"enabled": True, "applied": False, "reason": "critic_call_failed"}

    parsed = _extract_json_object(critic.get("response", ""))
    if not parsed:
        return answer, {"enabled": True, "applied": False, "reason": "critic_parse_failed"}

    supported = bool(parsed.get("supported", True))
    refined = (parsed.get("refined_answer") or "").strip()
    confidence = parsed.get("confidence")
    issues = parsed.get("issues") if isinstance(parsed.get("issues"), list) else []
    missing = parsed.get("missing_points") if isinstance(parsed.get("missing_points"), list) else []

    apply_refine = (not supported and bool(refined)) or (bool(refined) and refined != answer and len(refined) > 20)
    final = refined if apply_refine else answer

    return final, {
        "enabled": True,
        "applied": bool(apply_refine),
        "supported": supported,
        "confidence": confidence,
        "issues_count": len(issues),
        "missing_points_count": len(missing),
    }


def _batch_context_docs(
    context_documents: List[Dict[str, Any]],
    max_docs: int = 12,
    max_chars: int = 7000,
) -> List[List[Dict[str, Any]]]:
    batches: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    chars = 0

    for d in context_documents:
        content = (d.get("content") or "").strip()
        if not content:
            continue
        add = len(content)

        if current and (len(current) >= max_docs or (chars + add) > max_chars):
            batches.append(current)
            current = []
            chars = 0

        current.append(d)
        chars += add

    if current:
        batches.append(current)

    return batches


def _chunk_text_blocks(blocks: List[str], max_chars: int) -> List[List[str]]:
    groups: List[List[str]] = []
    current: List[str] = []
    used = 0
    separator_size = len("\n\n=====\n\n")

    for block in blocks:
        add = len(block) + (separator_size if current else 0)
        if current and (used + add) > max_chars:
            groups.append(current)
            current = []
            used = 0
            add = len(block)

        current.append(block)
        used += add

    if current:
        groups.append(current)

    return groups


async def _hierarchical_reduce_partials(
    *,
    query: str,
    partials: List[str],
    model_source: Optional[str],
    model_name: Optional[str],
    prompt_max_chars: Optional[int],
) -> Tuple[str, Dict[str, Any]]:
    target_chars = int(settings.FULL_FILE_REDUCE_CONTEXT_MAX_CHARS)
    target_groups = int(settings.FULL_FILE_REDUCE_TARGET_GROUPS)
    max_rounds = int(settings.FULL_FILE_REDUCE_MAX_ROUNDS)

    working = [(p or "").strip() for p in partials if (p or "").strip()]
    if not working:
        return "", {"rounds": 0, "truncated": False, "input_partials": 0, "output_partials": 0}

    rounds = 0
    truncated = False

    while rounds < max_rounds:
        combined_len = len("\n\n=====\n\n".join(working))
        if len(working) == 1 and combined_len <= target_chars:
            break

        groups_count = min(max(1, target_groups), len(working))
        group_size = max(1, (len(working) + groups_count - 1) // groups_count)
        next_round: List[str] = []

        for start in range(0, len(working), group_size):
            group = working[start:start + group_size]
            reduce_prompt = (
                "You are compressing partial summaries for full-file analysis.\n"
                "Preserve critical numeric facts, entities, constraints and caveats.\n"
                "Do not invent facts. Keep output dense and factual.\n\n"
                f"User question:\n{query}\n\n"
                "Part summaries:\n"
                + "\n\n-----\n\n".join(group)
                + "\n\nCompressed summary:"
            )
            try:
                reduced = await llm_manager.generate_response(
                    prompt=reduce_prompt,
                    model_source=model_source,
                    model_name=model_name,
                    temperature=0.0,
                    max_tokens=1100,
                    conversation_history=None,
                    prompt_max_chars=prompt_max_chars,
                )
                text = (reduced.get("response") or "").strip()
                if text:
                    next_round.append(text)
            except Exception:
                logger.warning("Hierarchical reduce step failed (round=%d)", rounds + 1, exc_info=True)

        if not next_round:
            truncated = True
            break

        working = next_round
        rounds += 1

    if len(working) > 1:
        final_groups = _chunk_text_blocks(working, max_chars=target_chars)
        if len(final_groups) > 1:
            truncated = True
        context = "\n\n=====\n\n".join(final_groups[0])
    else:
        context = (working[0] if working else "")[:target_chars]
        if working and len(working[0]) > target_chars:
            truncated = True

    return context, {
        "rounds": rounds,
        "truncated": truncated,
        "input_partials": len(partials),
        "output_partials": len(working),
        "target_chars": target_chars,
    }


async def _build_full_file_map_reduce_prompt(
    *,
    query: str,
    context_documents: List[Dict[str, Any]],
    model_source: Optional[str],
    model_name: Optional[str],
    prompt_max_chars: Optional[int] = None,
) -> Tuple[str, Dict[str, Any]]:
    max_docs = int(settings.FULL_FILE_MAP_BATCH_MAX_DOCS)
    max_chars = int(settings.FULL_FILE_MAP_BATCH_MAX_CHARS)
    max_batches = int(settings.FULL_FILE_MAP_MAX_BATCHES)
    batches = _batch_context_docs(context_documents, max_docs=max_docs, max_chars=max_chars)
    logger.info("RAG full_file map-reduce: docs=%d batches=%d", len(context_documents), len(batches))

    if not batches:
        return query, {
            "enabled": True,
            "total_batches": 0,
            "processed_batches": 0,
            "max_batches": max_batches,
            "truncated_batches": False,
            "partials_count": 0,
            "fallback_to_query": True,
        }

    partials: List[str] = []
    processed_batches = 0
    covered_chunks = 0
    for i, batch in enumerate(batches[:max_batches], start=1):
        chunk_lines: List[str] = []
        for j, d in enumerate(batch, start=1):
            meta = d.get("metadata") or {}
            filename = meta.get("filename") or meta.get("source") or "unknown"
            chunk_index = meta.get("chunk_index", "?")
            sheet_name = meta.get("sheet_name")
            row_start = meta.get("row_start")
            row_end = meta.get("row_end")
            total_rows = meta.get("total_rows")
            content = (d.get("content") or "").strip()
            if not content:
                continue
            label_parts = [f"file={filename}", f"chunk={chunk_index}"]
            if sheet_name:
                label_parts.append(f"sheet={sheet_name}")
            if row_start is not None and row_end is not None:
                rows = f"{row_start}-{row_end}"
                if total_rows is not None:
                    rows = f"{rows}/{total_rows}"
                label_parts.append(f"rows={rows}")
            chunk_lines.append(f"[{j}] " + " ".join(label_parts) + f"\n{content}")

        if not chunk_lines:
            continue
        processed_batches += 1
        covered_chunks += len(chunk_lines)

        map_prompt = (
            "You are summarizing one batch of a large document for full-file analysis.\n"
            "Extract key facts relevant to the user question and preserve important numeric values.\n"
            "For tabular data, explicitly include row ranges and notable outliers/trends in this batch.\n"
            "If the batch has no relevant facts, explicitly say so.\n"
            "Be concise and factual.\n\n"
            f"User question:\n{query}\n\n"
            f"Batch content ({i}/{min(len(batches), max_batches)}):\n"
            + "\n\n---\n\n".join(chunk_lines)
            + "\n\nBatch summary:"
        )

        try:
            map_result = await llm_manager.generate_response(
                prompt=map_prompt,
                model_source=model_source,
                model_name=model_name,
                temperature=0.1,
                max_tokens=900,
                conversation_history=None,
                prompt_max_chars=prompt_max_chars,
            )
            partial_text = (map_result.get("response") or "").strip()
            if partial_text:
                partials.append(f"[PART {i}]\n{partial_text}")
        except Exception:
            logger.warning("Map step failed for batch %d", i, exc_info=True)

    truncated_batches = len(batches) > max_batches
    if not partials:
        logger.warning("RAG full_file map-reduce: no partial summaries produced")
        return query, {
            "enabled": True,
            "total_batches": len(batches),
            "processed_batches": processed_batches,
            "max_batches": max_batches,
            "truncated_batches": truncated_batches,
            "partials_count": 0,
            "fallback_to_query": True,
        }

    reduce_context, reduce_meta = await _hierarchical_reduce_partials(
        query=query,
        partials=partials,
        model_source=model_source,
        model_name=model_name,
        prompt_max_chars=prompt_max_chars,
    )
    if not reduce_context:
        logger.warning("RAG full_file map-reduce: empty reduce context")
        return query, {
            "enabled": True,
            "total_batches": len(batches),
            "processed_batches": processed_batches,
            "max_batches": max_batches,
            "truncated_batches": True,
            "partials_count": len(partials),
            "fallback_to_query": True,
            "hierarchical_reduce": reduce_meta,
        }

    final_prompt = (
        "You are a document analyst. You received summaries of all document parts.\n"
        "Produce a complete, consistent and detailed answer to the user question.\n"
        "Rules:\n"
        "1) Do not invent facts outside provided context.\n"
        "2) Explicitly mention missing information when needed.\n"
        "3) Return sections in order: Answer, Limitations/Missing data, Sources.\n"
        "4) Use all available summaries, not a single fragment.\n"
        "5) Keep concrete values and units from evidence.\n\n"
        f"User question:\n{query}\n\n"
        f"All partial summaries:\n{reduce_context}\n\n"
        "Final answer:"
    )
    return final_prompt, {
        "enabled": True,
        "total_batches": len(batches),
        "processed_batches": processed_batches,
        "max_batches": max_batches,
        "truncated_batches": bool(truncated_batches or reduce_meta.get("truncated")),
        "partials_count": len(partials),
        "covered_chunks": covered_chunks,
        "fallback_to_query": False,
        "hierarchical_reduce": reduce_meta,
    }

async def _try_build_rag_prompt(
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
):
    final_prompt = query
    rag_used = False
    rag_debug = None
    context_docs: List[Dict[str, Any]] = []
    rag_caveats: List[str] = []
    rag_sources: List[str] = []

    if not user_id:
        return final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources

    try:
        files = await crud_file.get_conversation_files(db, conversation_id=conversation_id, user_id=user_id)
        logger.info("Conversation files (completed): %d", len(files))
    except Exception as e:
        logger.warning("Could not fetch conversation files: %s", e)
        return final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources

    if file_ids:
        allowed_ids = {str(x) for x in file_ids}
        files = [f for f in files if str(f.id) in allowed_ids]
        logger.info("Conversation files filtered by payload file_ids: %d", len(files))

    if not files:
        return final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources

    rag_file_ids = [str(f.id) for f in files]
    groups = _group_files_by_embedding_config(files, model_source)
    embedding_mode, embedding_model = _resolve_rag_embedding_config(files, model_source)

    try:
        rag_results: List[Dict[str, Any]] = []
        if len(groups) == 1:
            rag_result = await rag_retriever.query_rag(
                query,
                top_k=top_k,
                user_id=str(user_id),
                conversation_id=str(conversation_id),
                file_ids=rag_file_ids,
                embedding_mode=embedding_mode,
                embedding_model=embedding_model,
                rag_mode=rag_mode,
                debug_return=True,
            )
            if isinstance(rag_result, dict):
                rag_results.append(rag_result)
        else:
            logger.info("RAG mixed embeddings: groups=%d", len(groups))
            group_tasks = []
            for (group_mode, group_model), group_file_ids in groups.items():
                group_tasks.append(
                    rag_retriever.query_rag(
                        query,
                        top_k=max(top_k, 4),
                        user_id=str(user_id),
                        conversation_id=str(conversation_id),
                        file_ids=group_file_ids,
                        embedding_mode=group_mode,
                        embedding_model=group_model,
                        rag_mode=rag_mode,
                        debug_return=True,
                    )
                )
            group_results = await asyncio.gather(*group_tasks, return_exceptions=True)
            for gr in group_results:
                if isinstance(gr, Exception):
                    logger.warning("RAG group retrieval failed: %s", gr)
                    continue
                if isinstance(gr, dict):
                    rag_results.append(gr)

        collected_docs: List[Dict[str, Any]] = []
        debug_groups: List[Dict[str, Any]] = []
        for rr in rag_results:
            docs = rr.get("docs") or []
            dbg = rr.get("debug") if isinstance(rr.get("debug"), dict) else {}
            collected_docs.extend(docs)
            debug_groups.append(dbg)

        is_full_file_mode = any(
            isinstance(dbg, dict) and (
                dbg.get("retrieval_mode") == "full_file"
                or dbg.get("intent") == "analyze_full_file"
            )
            for dbg in debug_groups
        )
        max_docs = int(settings.RAG_FULL_FILE_MAX_CHUNKS) if is_full_file_mode else max(top_k * 4, 32)
        context_docs = _merge_context_docs(
            collected_docs,
            max_docs=max_docs,
            sort_by_score=not is_full_file_mode,
        )
        rag_debug = (debug_groups[0] if debug_groups else {}) if isinstance(debug_groups, list) else {}
        if not isinstance(rag_debug, dict):
            rag_debug = {}
        else:
            rag_debug = deepcopy(rag_debug)
        rag_debug["embedding_mode"] = embedding_mode
        rag_debug["embedding_model"] = embedding_model
        rag_debug["file_ids"] = rag_file_ids
        rag_debug["rag_mode"] = rag_mode or "auto"
        rag_debug["mixed_embedding_groups"] = [
            {"mode": mode, "model": model, "file_count": len(ids)}
            for (mode, model), ids in groups.items()
        ]
        rag_debug["mixed_embeddings"] = len(groups) > 1
        rag_debug["group_count"] = len(groups)
        # Keep debug payload JSON-serializable and avoid self-references.
        rag_debug["group_debug"] = [deepcopy(d) if isinstance(d, dict) else d for d in debug_groups]
        expected_chunks_total = sum(int(getattr(f, "chunks_count", 0) or 0) for f in files)
        retrieved_chunks_total = len(context_docs)
        coverage_ratio = (float(retrieved_chunks_total / expected_chunks_total) if expected_chunks_total > 0 else 0.0)
        rag_debug["retrieved_chunks_total"] = retrieved_chunks_total
        rag_debug["coverage"] = {
            "expected_chunks": expected_chunks_total,
            "retrieved_chunks": retrieved_chunks_total,
            "ratio": coverage_ratio,
            "complete": bool(expected_chunks_total == 0 or retrieved_chunks_total >= expected_chunks_total),
        }

        if context_docs:
            retrieval_mode = (rag_debug or {}).get("retrieval_mode") if isinstance(rag_debug, dict) else None
            intent = (rag_debug or {}).get("intent") if isinstance(rag_debug, dict) else None

            if retrieval_mode == "full_file" or intent == "analyze_full_file":
                final_prompt, map_reduce_meta = await _build_full_file_map_reduce_prompt(
                    query=query,
                    context_documents=context_docs,
                    model_source=model_source,
                    model_name=model_name,
                    prompt_max_chars=prompt_max_chars,
                )
                rag_debug["full_file_map_reduce"] = map_reduce_meta
                rag_debug["truncated"] = bool(
                    map_reduce_meta.get("truncated_batches")
                    or rag_debug.get("full_file_limit_hit")
                )
                if not rag_debug.get("coverage", {}).get("complete", True):
                    rag_debug["truncated"] = True
            else:
                final_prompt = rag_retriever.build_context_prompt(query=query, context_documents=context_docs)

            rag_used = True
            rag_sources = _build_sources_list(context_docs, max_items=12)
            rag_caveats = _build_rag_caveats(files=files, context_documents=context_docs, rag_debug=rag_debug)
            logger.info(
                "RAG enabled: docs=%d mode=%s model=%s retrieval_mode=%s",
                len(context_docs),
                embedding_mode,
                embedding_model,
                retrieval_mode,
            )
        else:
            logger.info("RAG: no relevant chunks")

    except TypeError:
        context_docs = await rag_retriever.query_rag(
            query,
            top_k=top_k,
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            debug_return=True,
        )
        if isinstance(context_docs, dict) and "docs" in context_docs:
            context_docs_list = context_docs.get("docs") or []
            rag_debug = context_docs.get("debug")
            if context_docs_list:
                final_prompt = rag_retriever.build_context_prompt(query=query, context_documents=context_docs_list)
                rag_used = True
                rag_sources = _build_sources_list(context_docs_list, max_items=12)
                rag_caveats = _build_rag_caveats(files=files, context_documents=context_docs_list, rag_debug=rag_debug)

    except Exception as e:
        logger.warning("RAG retrieval failed: %s", e)

    return final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources


class ChatOrchestrator:
    async def _get_or_create_conversation(
        self,
        *,
        db: AsyncSession,
        chat_data: ChatMessage,
        user_id: Optional[uuid.UUID],
    ):
        if chat_data.conversation_id:
            conversation = await crud_conversation.get(db, id=chat_data.conversation_id)
            if not conversation:
                raise HTTPException(status_code=404, detail="Conversation not found")
            if conversation.user_id != user_id:
                raise HTTPException(status_code=403, detail="Access denied")
            return conversation

        from app.schemas.conversation import ConversationCreate

        conv_data = ConversationCreate(
            title=chat_data.message[:100] if len(chat_data.message) <= 100 else chat_data.message[:97] + "...",
            model_source=chat_data.model_source or "ollama",
            model_name=chat_data.model_name or llm_manager.ollama_model,
        )
        return await crud_conversation.create_for_user(db=db, obj_in=conv_data, user_id=user_id)

    async def chat_stream(
        self,
        *,
        chat_data: ChatMessage,
        db: AsyncSession,
        current_user: Optional[User],
    ) -> StreamingResponse:
        user_id = current_user.id if current_user else None
        username = current_user.username if current_user else "anonymous"
        logger.info("Chat(stream) from %s", username)

        conversation = await self._get_or_create_conversation(db=db, chat_data=chat_data, user_id=user_id)
        conversation_id = conversation.id

        await crud_message.create_message(db=db, conversation_id=conversation_id, role="user", content=chat_data.message)
        messages = await crud_message.get_last_messages(
            db,
            conversation_id=conversation_id,
            count=settings.CHAT_HISTORY_MAX_MESSAGES,
        )
        conversation_history = _build_conversation_history(messages)

        final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = await _try_build_rag_prompt(
            db=db,
            user_id=user_id,
            conversation_id=conversation_id,
            query=chat_data.message,
            top_k=8,
            file_ids=chat_data.file_ids,
            model_source=chat_data.model_source,
            model_name=chat_data.model_name,
            rag_mode=chat_data.rag_mode,
            prompt_max_chars=chat_data.prompt_max_chars,
        )

        assistant_message_id = uuid.uuid4()

        async def event_stream():
            full_response = ""
            start_time = datetime.utcnow()
            summary_text: Optional[str] = None

            try:
                start_payload = {
                    "type": "start",
                    "conversation_id": str(conversation_id),
                    "message_id": str(assistant_message_id),
                    "rag_enabled": rag_used,
                    "rag_debug": rag_debug,
                }
                if chat_data.rag_debug:
                    debug_max_items = 64 if isinstance(rag_debug, dict) and rag_debug.get("retrieval_mode") == "full_file" else 8
                    start_payload["rag_debug"] = _build_standard_rag_debug_payload(
                        rag_debug=rag_debug,
                        context_docs=context_docs,
                        rag_sources=rag_sources,
                        llm_tokens_used=None,
                        max_items=debug_max_items,
                    )
                try:
                    start_payload_json = json.dumps(start_payload)
                except ValueError:
                    logger.warning("RAG start payload is not JSON-serializable; sending reduced debug payload", exc_info=True)
                    start_payload["rag_debug"] = {"serialization_error": True}
                    start_payload_json = json.dumps(start_payload)
                yield f"data: {start_payload_json}\n\n"

                history_for_generation = conversation_history
                if rag_used:
                    history_for_generation = _build_rag_conversation_memory(conversation_history, max_messages=6)

                async for chunk in llm_manager.generate_response_stream(
                    prompt=final_prompt,
                    model_source=chat_data.model_source,
                    model_name=chat_data.model_name,
                    temperature=chat_data.temperature or 0.7,
                    max_tokens=chat_data.max_tokens or 2000,
                    conversation_history=history_for_generation,
                    prompt_max_chars=chat_data.prompt_max_chars,
                ):
                    full_response += chunk
                    yield f"data: {json.dumps({'type': 'chunk', 'content': chunk})}\n\n"

                generation_time = (datetime.utcnow() - start_time).total_seconds()

                if rag_used:
                    full_response = _append_caveats_and_sources(full_response, rag_caveats, rag_sources)

                if chat_data.summarize and rag_used and context_docs and settings.ENABLE_POST_ANSWER_SUMMARIZE:
                    summarized_response, critic_meta = await _run_answer_critic(
                        query=chat_data.message,
                        answer=full_response,
                        context_documents=context_docs,
                        model_source=chat_data.model_source,
                        model_name=chat_data.model_name,
                    )
                    if summarized_response and summarized_response != full_response:
                        summary_text = summarized_response
                        yield f"data: {json.dumps({'type': 'summary', 'content': summary_text, 'critic': critic_meta})}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'critic', 'critic': critic_meta})}\n\n"

                await crud_message.create_message(
                    db=db,
                    conversation_id=conversation_id,
                    role="assistant",
                    content=full_response,
                    model_name=chat_data.model_name or llm_manager.ollama_model,
                    temperature=chat_data.temperature,
                    max_tokens=chat_data.max_tokens,
                    generation_time=generation_time,
                )

                yield f"data: {json.dumps({'type': 'done', 'generation_time': generation_time, 'rag_used': rag_used, 'summary_available': bool(summary_text), 'caveats': rag_caveats, 'sources': rag_sources})}\n\n"

            except Exception as e:
                logger.error("Streaming error: %s", e, exc_info=True)
                yield f"data: {json.dumps({'type': 'error', 'message': str(e), 'error_type': type(e).__name__})}\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
        )

    async def chat(
        self,
        *,
        chat_data: ChatMessage,
        db: AsyncSession,
        current_user: Optional[User],
    ) -> ChatResponse:
        user_id = current_user.id if current_user else None
        username = current_user.username if current_user else "anonymous"
        logger.info("Chat(non-stream) from %s", username)

        conversation = await self._get_or_create_conversation(db=db, chat_data=chat_data, user_id=user_id)
        conversation_id = conversation.id

        await crud_message.create_message(db=db, conversation_id=conversation_id, role="user", content=chat_data.message)
        messages = await crud_message.get_last_messages(
            db,
            conversation_id=conversation_id,
            count=settings.CHAT_HISTORY_MAX_MESSAGES,
        )
        conversation_history = _build_conversation_history(messages)

        final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = await _try_build_rag_prompt(
            db=db,
            user_id=user_id,
            conversation_id=conversation_id,
            query=chat_data.message,
            top_k=8,
            file_ids=chat_data.file_ids,
            model_source=chat_data.model_source,
            model_name=chat_data.model_name,
            rag_mode=chat_data.rag_mode,
            prompt_max_chars=chat_data.prompt_max_chars,
        )

        history_for_generation = conversation_history
        if rag_used:
            history_for_generation = _build_rag_conversation_memory(conversation_history, max_messages=6)

        start_time = datetime.utcnow()
        result = await llm_manager.generate_response(
            prompt=final_prompt,
            model_source=chat_data.model_source,
            model_name=chat_data.model_name,
            temperature=chat_data.temperature or 0.7,
            max_tokens=chat_data.max_tokens or 2000,
            conversation_history=history_for_generation,
            prompt_max_chars=chat_data.prompt_max_chars,
        )

        answer_text = result.get("response", "")
        if rag_used:
            answer_text = _append_caveats_and_sources(answer_text, rag_caveats, rag_sources)
            result["response"] = answer_text

        summary_text: Optional[str] = None
        if chat_data.summarize and rag_used and context_docs and settings.ENABLE_POST_ANSWER_SUMMARIZE:
            summarized_answer, critic_meta = await _run_answer_critic(
                query=chat_data.message,
                answer=answer_text,
                context_documents=context_docs,
                model_source=chat_data.model_source,
                model_name=chat_data.model_name,
            )
            if summarized_answer and summarized_answer != answer_text:
                summary_text = summarized_answer
            logger.info("RAG critic(non-stream, summarize=%s): %s", chat_data.summarize, critic_meta)

        generation_time = (datetime.utcnow() - start_time).total_seconds()

        assistant_message = await crud_message.create_message(
            db=db,
            conversation_id=conversation_id,
            role="assistant",
            content=result["response"],
            model_name=result["model"],
            temperature=chat_data.temperature,
            max_tokens=chat_data.max_tokens,
            tokens_used=result.get("tokens_used"),
            generation_time=generation_time,
        )
        rag_debug_payload = None
        if chat_data.rag_debug:
            debug_max_items = 64 if isinstance(rag_debug, dict) and rag_debug.get("retrieval_mode") == "full_file" else 8
            rag_debug_payload = _build_standard_rag_debug_payload(
                rag_debug=rag_debug,
                context_docs=context_docs,
                rag_sources=rag_sources,
                llm_tokens_used=result.get("tokens_used"),
                max_items=debug_max_items,
            )

        return ChatResponse(
            response=result["response"],
            conversation_id=conversation_id,
            message_id=assistant_message.id,
            model_used=result["model"],
            tokens_used=result.get("tokens_used"),
            generation_time=generation_time,
            summary=summary_text,
            caveats=rag_caveats,
            sources=rag_sources,
            rag_debug=rag_debug_payload,
        )


chat_orchestrator = ChatOrchestrator()



from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from app.core.config import settings
from app.services.chat.language import build_language_policy_instruction, apply_language_policy_to_prompt
from app.services.chat.sources_debug import build_context_coverage_summary
from app.services.llm.manager import llm_manager

logger = logging.getLogger(__name__)


def batch_context_docs(
    context_documents: List[Dict[str, Any]],
    max_docs: int = 12,
    max_chars: int = 7000,
) -> List[List[Dict[str, Any]]]:
    batches: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    chars = 0

    for doc in context_documents:
        content = (doc.get("content") or "").strip()
        if not content:
            continue
        add = len(content)

        if current and (len(current) >= max_docs or (chars + add) > max_chars):
            batches.append(current)
            current = []
            chars = 0

        current.append(doc)
        chars += add

    if current:
        batches.append(current)

    return batches


def chunk_text_blocks(blocks: List[str], max_chars: int) -> List[List[str]]:
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


def _build_direct_full_file_prompt(
    *,
    query: str,
    context_documents: List[Dict[str, Any]],
    preferred_lang: str,
) -> str:
    lines: List[str] = []
    for i, doc in enumerate(context_documents, start=1):
        meta = doc.get("metadata") or {}
        filename = meta.get("filename") or meta.get("source") or "unknown"
        chunk_index = meta.get("chunk_index", "?")
        sheet_name = meta.get("sheet_name")
        row_start = meta.get("row_start")
        row_end = meta.get("row_end")
        total_rows = meta.get("total_rows")
        content = (doc.get("content") or "").strip()
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
        lines.append(f"[{i}] {' '.join(label_parts)}\n{content}")

    coverage_summary = build_context_coverage_summary(context_documents, max_items=12)
    return apply_language_policy_to_prompt(
        preferred_lang=preferred_lang,
        prompt=(
            "You are a document analyst.\n"
            "Use ALL provided chunks as the source of truth and do not ignore any chunk.\n"
            "For spreadsheet data, preserve numeric values and refer to row ranges.\n"
            "Do not invent facts outside context.\n"
            "Return sections in order: Answer, Limitations/Missing data, Sources.\n\n"
            "Retrieved coverage summary:\n"
            f"{coverage_summary}\n\n"
            f"User question:\n{query}\n\n"
            "Full retrieved context:\n"
            + "\n\n---\n\n".join(lines)
            + "\n\nFinal answer:"
        ),
    )


async def hierarchical_reduce_partials(
    *,
    query: str,
    partials: List[str],
    preferred_lang: str,
    model_source: Optional[str],
    model_name: Optional[str],
    prompt_max_chars: Optional[int],
) -> Tuple[str, Dict[str, Any]]:
    target_chars = int(settings.FULL_FILE_REDUCE_CONTEXT_MAX_CHARS)
    target_groups = int(settings.FULL_FILE_REDUCE_TARGET_GROUPS)
    max_rounds = int(settings.FULL_FILE_REDUCE_MAX_ROUNDS)

    working = [(part or "").strip() for part in partials if (part or "").strip()]
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
                + build_language_policy_instruction(preferred_lang)
                + "\n"
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
        final_groups = chunk_text_blocks(working, max_chars=target_chars)
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


async def build_full_file_map_reduce_prompt(
    *,
    query: str,
    context_documents: List[Dict[str, Any]],
    preferred_lang: str,
    model_source: Optional[str],
    model_name: Optional[str],
    prompt_max_chars: Optional[int] = None,
) -> Tuple[str, Dict[str, Any]]:
    direct_max_chunks = int(settings.FULL_FILE_DIRECT_CONTEXT_MAX_CHUNKS)
    direct_max_chars = int(settings.FULL_FILE_DIRECT_CONTEXT_MAX_CHARS)
    docs_chars = sum(len((doc.get("content") or "").strip()) for doc in context_documents)
    if context_documents and len(context_documents) <= direct_max_chunks and docs_chars <= direct_max_chars:
        logger.info(
            "RAG full_file direct-context: docs=%d chars=%d limits=(chunks=%d chars=%d)",
            len(context_documents),
            docs_chars,
            direct_max_chunks,
            direct_max_chars,
        )
        return _build_direct_full_file_prompt(
            query=query,
            context_documents=context_documents,
            preferred_lang=preferred_lang,
        ), {
            "enabled": True,
            "strategy": "direct_context",
            "total_batches": 1,
            "processed_batches": 1,
            "max_batches": 1,
            "truncated_batches": False,
            "partials_count": 0,
            "covered_chunks": len(context_documents),
            "fallback_to_query": False,
            "direct_context_total_chars": docs_chars,
            "direct_context_max_chars": direct_max_chars,
            "direct_context_max_chunks": direct_max_chunks,
        }

    max_docs = int(settings.FULL_FILE_MAP_BATCH_MAX_DOCS)
    max_chars = int(settings.FULL_FILE_MAP_BATCH_MAX_CHARS)
    max_batches = int(settings.FULL_FILE_MAP_MAX_BATCHES)
    batches = batch_context_docs(context_documents, max_docs=max_docs, max_chars=max_chars)
    logger.info("RAG full_file map-reduce: docs=%d batches=%d", len(context_documents), len(batches))

    if not batches:
        return apply_language_policy_to_prompt(prompt=query, preferred_lang=preferred_lang), {
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
        for j, doc in enumerate(batch, start=1):
            meta = doc.get("metadata") or {}
            filename = meta.get("filename") or meta.get("source") or "unknown"
            chunk_index = meta.get("chunk_index", "?")
            sheet_name = meta.get("sheet_name")
            row_start = meta.get("row_start")
            row_end = meta.get("row_end")
            total_rows = meta.get("total_rows")
            content = (doc.get("content") or "").strip()
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
            + build_language_policy_instruction(preferred_lang)
            + "\n"
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

    reduce_context, reduce_meta = await hierarchical_reduce_partials(
        query=query,
        partials=partials,
        preferred_lang=preferred_lang,
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

    coverage_summary = build_context_coverage_summary(context_documents, max_items=12)
    final_prompt = apply_language_policy_to_prompt(
        preferred_lang=preferred_lang,
        prompt=(
            "You are a document analyst. You received summaries of all document parts.\n"
            "Produce a complete, consistent and detailed answer to the user question.\n"
            "Rules:\n"
            "1) Do not invent facts outside provided context.\n"
            "2) Explicitly mention missing information when needed.\n"
            "3) Return sections in order: Answer, Limitations/Missing data, Sources.\n"
            "4) Use all available summaries, not a single fragment.\n"
            "5) Keep concrete values and units from evidence.\n\n"
            "Retrieved coverage summary:\n"
            f"{coverage_summary}\n\n"
            "If coverage summary spans the whole document, do not claim that only the last rows were provided.\n\n"
            f"User question:\n{query}\n\n"
            f"All partial summaries:\n{reduce_context}\n\n"
            "Final answer:"
        ),
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

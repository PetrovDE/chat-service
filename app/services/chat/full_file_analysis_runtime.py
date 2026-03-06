from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from app.services.chat.language import build_language_policy_instruction, apply_language_policy_to_prompt
from app.services.chat.sources_debug import build_context_coverage_summary

from .full_file_analysis_helpers import (
    _build_direct_full_file_prompt,
    _build_structured_partial,
    _count_rows,
    _extract_doc_row_range,
    _merge_grouped_row_ranges,
    _merge_structured_partials,
    _row_ranges_debug,
    batch_context_docs,
)


async def build_full_file_map_reduce_prompt_runtime(
    *,
    query: str,
    context_documents: List[Dict[str, Any]],
    preferred_lang: str,
    model_source: Optional[str],
    model_name: Optional[str],
    settings_obj: Any,
    llm_client: Any,
    logger_obj: Any,
    prompt_max_chars: Optional[int] = None,
) -> Tuple[str, Dict[str, Any]]:
    direct_max_chunks = int(settings_obj.FULL_FILE_DIRECT_CONTEXT_MAX_CHUNKS)
    direct_max_chars = int(settings_obj.FULL_FILE_DIRECT_CONTEXT_MAX_CHARS)
    docs_chars = sum(len((doc.get("content") or "").strip()) for doc in context_documents)
    direct_ranges: Dict[Tuple[str, str], List[Tuple[int, int]]] = {}
    for doc in context_documents:
        row = _extract_doc_row_range(doc)
        if row is None:
            continue
        key = (row[0], row[1])
        direct_ranges.setdefault(key, []).append((row[2], row[3]))
    merged_direct_ranges = _merge_grouped_row_ranges(direct_ranges)
    direct_rows_total = _count_rows(merged_direct_ranges)

    if context_documents and len(context_documents) <= direct_max_chunks and docs_chars <= direct_max_chars:
        logger_obj.info(
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
            "rows_used_map_total": direct_rows_total,
            "rows_used_reduce_total": direct_rows_total,
            "batch_diagnostics": [
                {
                    "batch_index": 1,
                    "batch_rows_start_end": _row_ranges_debug(merged_direct_ranges),
                    "batch_input_chars": docs_chars,
                    "batch_output_chars": docs_chars,
                }
            ],
        }

    max_docs = int(settings_obj.FULL_FILE_MAP_BATCH_MAX_DOCS)
    max_chars = int(settings_obj.FULL_FILE_MAP_BATCH_MAX_CHARS)
    max_batches = int(settings_obj.FULL_FILE_MAP_MAX_BATCHES)
    batches = batch_context_docs(context_documents, max_docs=max_docs, max_chars=max_chars)
    logger_obj.info("RAG full_file map-reduce: docs=%d batches=%d", len(context_documents), len(batches))

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

    structured_partials: List[Dict[str, Any]] = []
    processed_batches = 0
    covered_chunks = 0
    batch_diagnostics: List[Dict[str, Any]] = []
    map_ranges: Dict[Tuple[str, str], List[Tuple[int, int]]] = {}
    map_max_tokens = int(getattr(settings_obj, "FULL_FILE_MAP_MAX_TOKENS", 900) or 900)
    for i, batch in enumerate(batches[:max_batches], start=1):
        chunk_lines: List[str] = []
        batch_ranges: Dict[Tuple[str, str], List[Tuple[int, int]]] = {}
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
            row = _extract_doc_row_range(doc)
            if row is not None:
                key = (row[0], row[1])
                batch_ranges.setdefault(key, []).append((row[2], row[3]))
                map_ranges.setdefault(key, []).append((row[2], row[3]))

        if not chunk_lines:
            continue
        processed_batches += 1
        covered_chunks += len(chunk_lines)
        batch_rows_merged = _merge_grouped_row_ranges(batch_ranges)
        batch_rows_debug = _row_ranges_debug(batch_rows_merged)
        batch_input_chars = len("\n\n---\n\n".join(chunk_lines))
        batch_output_chars = 0
        partial_text = ""

        map_prompt = (
            "You are extracting structured evidence from one batch of a large document.\n"
            "Return STRICT JSON object with fields:\n"
            "facts: string[]\n"
            "aggregates: string[]\n"
            "row_ranges_covered: [{file_key:string,sheet_name:string,row_start:int,row_end:int}]\n"
            "missing_data: string[]\n"
            "Do not return markdown.\n\n"
            + build_language_policy_instruction(preferred_lang)
            + "\n"
            f"User question:\n{query}\n\n"
            f"Batch content ({i}/{min(len(batches), max_batches)}):\n"
            + "\n\n---\n\n".join(chunk_lines)
            + "\n\nJSON:"
        )

        try:
            map_result = await llm_client.generate_response(
                prompt=map_prompt,
                model_source=model_source,
                model_name=model_name,
                temperature=0.1,
                max_tokens=map_max_tokens,
                conversation_history=None,
                prompt_max_chars=prompt_max_chars,
            )
            partial_text = (map_result.get("response") or "").strip()
            batch_output_chars = len(partial_text)
        except Exception:
            logger_obj.warning("Map step failed for batch %d", i, exc_info=True)
        finally:
            structured_partials.append(
                _build_structured_partial(
                    map_output_text=partial_text,
                    fallback_ranges=batch_rows_debug,
                )
            )
            batch_diagnostics.append(
                {
                    "batch_index": i,
                    "batch_rows_start_end": batch_rows_debug,
                    "batch_input_chars": int(batch_input_chars),
                    "batch_output_chars": int(batch_output_chars),
                }
            )

    truncated_batches = len(batches) > max_batches
    if not structured_partials:
        logger_obj.warning("RAG full_file map-reduce: no partial summaries produced")
        return query, {
            "enabled": True,
            "total_batches": len(batches),
            "processed_batches": processed_batches,
            "max_batches": max_batches,
            "truncated_batches": truncated_batches,
            "partials_count": 0,
            "fallback_to_query": True,
        }

    reduce_context, reduce_meta = _merge_structured_partials(
        structured_partials,
        max_chars=int(settings_obj.FULL_FILE_REDUCE_CONTEXT_MAX_CHARS),
    )
    if not reduce_context:
        logger_obj.warning("RAG full_file map-reduce: empty reduce context")
        return query, {
            "enabled": True,
            "total_batches": len(batches),
            "processed_batches": processed_batches,
            "max_batches": max_batches,
            "truncated_batches": True,
            "partials_count": len(structured_partials),
            "fallback_to_query": True,
            "structured_reduce": reduce_meta,
        }

    coverage_summary = build_context_coverage_summary(context_documents, max_items=12)
    final_prompt = apply_language_policy_to_prompt(
        preferred_lang=preferred_lang,
        prompt=(
            "You are a document analyst. You received structured evidence from all document parts.\n"
            "Produce a complete, consistent and detailed answer to the user question.\n"
            "Rules:\n"
            "1) Do not invent facts outside provided context.\n"
            "2) Explicitly mention missing information when needed.\n"
            "3) Return sections in order: Answer, Limitations/Missing data, Sources.\n"
            "4) Use all structured evidence, not a single fragment.\n"
            "5) Keep concrete values and units from evidence.\n\n"
            "Retrieved coverage summary:\n"
            f"{coverage_summary}\n\n"
            "If coverage summary spans the whole document, do not claim that only the last rows were provided.\n\n"
            f"User question:\n{query}\n\n"
            f"Structured reduce JSON:\n{reduce_context}\n\n"
            "Final answer:"
        ),
    )
    merged_map_ranges = _merge_grouped_row_ranges(map_ranges)
    rows_used_map_total = _count_rows(merged_map_ranges)
    rows_used_reduce_total = int(reduce_meta.get("rows_used_reduce_total", rows_used_map_total) or 0)

    return final_prompt, {
        "enabled": True,
        "strategy": "structured_map_reduce",
        "total_batches": len(batches),
        "processed_batches": processed_batches,
        "max_batches": max_batches,
        "truncated_batches": bool(truncated_batches or reduce_meta.get("truncated")),
        "partials_count": len(structured_partials),
        "covered_chunks": covered_chunks,
        "fallback_to_query": False,
        "structured_reduce": reduce_meta,
        "rows_used_map_total": rows_used_map_total,
        "rows_used_reduce_total": rows_used_reduce_total,
        "batch_diagnostics": batch_diagnostics,
    }

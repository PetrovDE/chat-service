from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from app.core.config import settings
from app.services.chat.language import build_language_policy_instruction, apply_language_policy_to_prompt
from app.services.chat.sources_debug import build_context_coverage_summary
from app.services.llm.manager import llm_manager

logger = logging.getLogger(__name__)


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _merge_ranges(ranges: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    if not ranges:
        return []
    ranges = sorted(ranges, key=lambda item: (item[0], item[1]))
    merged = [ranges[0]]
    for start, end in ranges[1:]:
        cur_start, cur_end = merged[-1]
        if start <= cur_end + 1:
            merged[-1] = (cur_start, max(cur_end, end))
        else:
            merged.append((start, end))
    return merged


def _extract_doc_row_range(doc: Dict[str, Any]) -> Optional[Tuple[str, str, int, int]]:
    meta = doc.get("metadata") or {}
    row_start = _to_int(meta.get("row_start"))
    row_end = _to_int(meta.get("row_end"))
    if row_start is None or row_end is None:
        return None
    file_key = str(meta.get("file_id") or meta.get("filename") or meta.get("source") or "unknown")
    sheet_key = str(meta.get("sheet_name") or "")
    return file_key, sheet_key, min(row_start, row_end), max(row_start, row_end)


def _merge_grouped_row_ranges(
    grouped_ranges: Dict[Tuple[str, str], List[Tuple[int, int]]],
) -> Dict[Tuple[str, str], List[Tuple[int, int]]]:
    return {key: _merge_ranges(value) for key, value in grouped_ranges.items() if value}


def _count_rows(grouped_ranges: Dict[Tuple[str, str], List[Tuple[int, int]]]) -> int:
    total = 0
    for ranges in grouped_ranges.values():
        total += sum((end - start + 1) for start, end in ranges)
    return int(total)


def _row_ranges_debug(
    grouped_ranges: Dict[Tuple[str, str], List[Tuple[int, int]]],
    max_items: int = 24,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for (file_key, sheet_key), ranges in sorted(grouped_ranges.items(), key=lambda item: (item[0][0], item[0][1])):
        for start, end in ranges:
            rows.append(
                {
                    "file_key": file_key,
                    "sheet_name": sheet_key or None,
                    "row_start": int(start),
                    "row_end": int(end),
                }
            )
            if len(rows) >= max_items:
                return rows
    return rows


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


def _parse_json_object(text: str) -> Optional[Dict[str, Any]]:
    raw = (text or "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        parsed = json.loads(raw[start:end + 1])
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _normalize_string_list(value: Any, max_items: int = 40) -> List[str]:
    if not isinstance(value, list):
        return []
    out: List[str] = []
    for item in value:
        text = str(item or "").strip()
        if not text:
            continue
        if text not in out:
            out.append(text)
        if len(out) >= max_items:
            break
    return out


def _normalize_row_ranges_payload(
    value: Any,
    fallback_ranges: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return list(fallback_ranges)
    normalized: List[Dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        row_start = _to_int(item.get("row_start"))
        row_end = _to_int(item.get("row_end"))
        if row_start is None or row_end is None:
            continue
        normalized.append(
            {
                "file_key": str(item.get("file_key") or "unknown"),
                "sheet_name": str(item.get("sheet_name") or ""),
                "row_start": min(row_start, row_end),
                "row_end": max(row_start, row_end),
            }
        )
    return normalized if normalized else list(fallback_ranges)


def _build_structured_partial(
    *,
    map_output_text: str,
    fallback_ranges: List[Dict[str, Any]],
) -> Dict[str, Any]:
    parsed = _parse_json_object(map_output_text)
    if not parsed:
        return {
            "facts": [map_output_text.strip()] if map_output_text.strip() else [],
            "aggregates": [],
            "row_ranges_covered": list(fallback_ranges),
            "missing_data": [],
        }
    return {
        "facts": _normalize_string_list(parsed.get("facts"), max_items=32),
        "aggregates": _normalize_string_list(parsed.get("aggregates"), max_items=24),
        "row_ranges_covered": _normalize_row_ranges_payload(parsed.get("row_ranges_covered"), fallback_ranges),
        "missing_data": _normalize_string_list(parsed.get("missing_data"), max_items=24),
    }


def _merge_structured_partials(
    partials: List[Dict[str, Any]],
    *,
    max_chars: int,
) -> Tuple[str, Dict[str, Any]]:
    facts: List[str] = []
    aggregates: List[str] = []
    missing_data: List[str] = []
    grouped_ranges: Dict[Tuple[str, str], List[Tuple[int, int]]] = {}

    for part in partials:
        for item in _normalize_string_list(part.get("facts"), max_items=200):
            if item not in facts:
                facts.append(item)
        for item in _normalize_string_list(part.get("aggregates"), max_items=200):
            if item not in aggregates:
                aggregates.append(item)
        for item in _normalize_string_list(part.get("missing_data"), max_items=200):
            if item not in missing_data:
                missing_data.append(item)

        ranges = part.get("row_ranges_covered")
        if not isinstance(ranges, list):
            continue
        for row in ranges:
            if not isinstance(row, dict):
                continue
            row_start = _to_int(row.get("row_start"))
            row_end = _to_int(row.get("row_end"))
            if row_start is None or row_end is None:
                continue
            key = (str(row.get("file_key") or "unknown"), str(row.get("sheet_name") or ""))
            grouped_ranges.setdefault(key, []).append((min(row_start, row_end), max(row_start, row_end)))

    merged_ranges = _merge_grouped_row_ranges(grouped_ranges)
    merged_ranges_debug = _row_ranges_debug(merged_ranges, max_items=500)
    rows_used_map_total = _count_rows(merged_ranges)

    merged = {
        "facts": facts,
        "aggregates": aggregates,
        "row_ranges_covered": merged_ranges_debug,
        "missing_data": missing_data,
    }
    context_text = json.dumps(merged, ensure_ascii=False, indent=2)
    truncated = False
    rows_used_reduce_total = rows_used_map_total

    if len(context_text) > max_chars:
        truncated = True
        trimmed = dict(merged)
        while len(json.dumps(trimmed, ensure_ascii=False, indent=2)) > max_chars and trimmed["facts"]:
            trimmed["facts"] = trimmed["facts"][:-1]
        while len(json.dumps(trimmed, ensure_ascii=False, indent=2)) > max_chars and trimmed["missing_data"]:
            trimmed["missing_data"] = trimmed["missing_data"][:-1]
        while len(json.dumps(trimmed, ensure_ascii=False, indent=2)) > max_chars and trimmed["aggregates"]:
            trimmed["aggregates"] = trimmed["aggregates"][:-1]
        while len(json.dumps(trimmed, ensure_ascii=False, indent=2)) > max_chars and trimmed["row_ranges_covered"]:
            trimmed["row_ranges_covered"] = trimmed["row_ranges_covered"][:-1]
        context_text = json.dumps(trimmed, ensure_ascii=False, indent=2)
        rows_used_reduce_total = 0
        for row in trimmed.get("row_ranges_covered", []):
            if not isinstance(row, dict):
                continue
            row_start = _to_int(row.get("row_start"))
            row_end = _to_int(row.get("row_end"))
            if row_start is None or row_end is None:
                continue
            rows_used_reduce_total += max(0, row_end - row_start + 1)
        merged_ranges_debug = trimmed.get("row_ranges_covered", [])

    return context_text, {
        "truncated": truncated,
        "input_partials": len(partials),
        "output_partials": 1 if context_text else 0,
        "target_chars": int(max_chars),
        "rows_used_map_total": int(rows_used_map_total),
        "rows_used_reduce_total": int(rows_used_reduce_total),
        "row_ranges_covered": merged_ranges_debug,
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

    structured_partials: List[Dict[str, Any]] = []
    processed_batches = 0
    covered_chunks = 0
    batch_diagnostics: List[Dict[str, Any]] = []
    map_ranges: Dict[Tuple[str, str], List[Tuple[int, int]]] = {}
    map_max_tokens = int(getattr(settings, "FULL_FILE_MAP_MAX_TOKENS", 900) or 900)
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
            map_result = await llm_manager.generate_response(
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
            logger.warning("Map step failed for batch %d", i, exc_info=True)
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

    reduce_context, reduce_meta = _merge_structured_partials(
        structured_partials,
        max_chars=int(settings.FULL_FILE_REDUCE_CONTEXT_MAX_CHARS),
    )
    if not reduce_context:
        logger.warning("RAG full_file map-reduce: empty reduce context")
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

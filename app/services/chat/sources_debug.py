from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def merge_ranges(ranges: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    if not ranges:
        return []
    sorted_ranges = sorted(ranges, key=lambda x: (x[0], x[1]))
    merged: List[Tuple[int, int]] = [sorted_ranges[0]]
    for start, end in sorted_ranges[1:]:
        cur_start, cur_end = merged[-1]
        if start <= cur_end + 1:
            merged[-1] = (cur_start, max(cur_end, end))
        else:
            merged.append((start, end))
    return merged


def build_coverage_sources(context_documents: List[Dict[str, Any]], max_items: int = 8) -> List[str]:
    groups: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for doc in context_documents:
        meta = doc.get("metadata") or {}
        filename = str(meta.get("filename") or meta.get("source") or "unknown")
        sheet = str(meta.get("sheet_name") or "")
        key = (filename, sheet)
        item = groups.setdefault(key, {"ranges": [], "chunk_indices": set()})

        row_start = to_int(meta.get("row_start"))
        row_end = to_int(meta.get("row_end"))
        if row_start is not None and row_end is not None:
            item["ranges"].append((min(row_start, row_end), max(row_start, row_end)))

        chunk_idx = to_int(meta.get("chunk_index"))
        if chunk_idx is not None:
            item["chunk_indices"].add(chunk_idx)

    if not groups:
        return []

    out: List[str] = []
    for (filename, sheet), item in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1])):
        ranges = merge_ranges(item["ranges"])
        if ranges:
            ranges_text = ", ".join([f"{start}-{end}" for start, end in ranges[:4]])
            if len(ranges) > 4:
                ranges_text += ", ..."
            if sheet:
                line = f"{filename} | sheet={sheet} | rows={ranges_text}"
            else:
                line = f"{filename} | rows={ranges_text}"
        else:
            chunk_indices = sorted(item["chunk_indices"])
            if chunk_indices:
                chunk_range = f"{chunk_indices[0]}-{chunk_indices[-1]}"
                if sheet:
                    line = f"{filename} | sheet={sheet} | chunk_range={chunk_range}"
                else:
                    line = f"{filename} | chunk_range={chunk_range}"
            else:
                line = f"{filename} | coverage=unknown"

        out.append(line)
        if len(out) >= max_items:
            break

    return out


def build_sources_list_with_mode(
    *,
    context_documents: List[Dict[str, Any]],
    max_items: int = 8,
    aggregate_ranges: bool = False,
) -> List[str]:
    if aggregate_ranges:
        aggregated = build_coverage_sources(context_documents=context_documents, max_items=max_items)
        if aggregated:
            return aggregated

    out: List[str] = []
    seen = set()
    for doc in context_documents:
        meta = doc.get("metadata") or {}
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


def build_sources_list(context_documents: List[Dict[str, Any]], max_items: int = 8) -> List[str]:
    return build_sources_list_with_mode(
        context_documents=context_documents,
        max_items=max_items,
        aggregate_ranges=False,
    )


def build_top_chunks_debug(context_documents: List[Dict[str, Any]], max_items: int = 8) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    docs = context_documents if (max_items is None or max_items <= 0) else context_documents[:max_items]
    for doc in docs:
        meta = doc.get("metadata") or {}
        file_id = str(meta.get("file_id") or "")
        chunk_index = meta.get("chunk_index")
        doc_id = str(meta.get("doc_id") or "").strip()
        chunk_id = str(meta.get("chunk_id") or "").strip()
        if not doc_id and file_id:
            doc_id = file_id
        if not chunk_id and file_id and chunk_index is not None:
            chunk_id = f"{file_id}_{chunk_index}"
        rows.append(
            {
                "score": float(doc.get("similarity_score", meta.get("similarity_score", 0.0)) or 0.0),
                "file_id": file_id,
                "doc_id": doc_id or None,
                "chunk_id": chunk_id or None,
                "filename": str(meta.get("filename") or meta.get("source") or "unknown"),
                "sheet_name": meta.get("sheet_name"),
                "chunk_type": meta.get("chunk_type"),
                "chunk_index": chunk_index,
                "row_start": meta.get("row_start"),
                "row_end": meta.get("row_end"),
                "total_rows": meta.get("total_rows"),
                "collection": meta.get("collection"),
                "preview": (doc.get("content") or "")[:220],
            }
        )
    return rows


def _row_range_key(meta: Dict[str, Any]) -> Tuple[str, str]:
    file_id = str(meta.get("file_id") or meta.get("source") or meta.get("filename") or "unknown")
    sheet = str(meta.get("sheet_name") or "")
    return file_id, sheet


def build_row_coverage_stats(context_documents: List[Dict[str, Any]]) -> Dict[str, Any]:
    expected_per_key: Dict[Tuple[str, str], int] = {}
    ranges_per_key: Dict[Tuple[str, str], List[Tuple[int, int]]] = {}

    for doc in context_documents:
        meta = doc.get("metadata") or {}
        key = _row_range_key(meta)

        row_start = to_int(meta.get("row_start"))
        row_end = to_int(meta.get("row_end"))
        total_rows = to_int(meta.get("total_rows"))

        if row_start is not None and row_end is not None:
            start = min(row_start, row_end)
            end = max(row_start, row_end)
            ranges_per_key.setdefault(key, []).append((start, end))
            expected_per_key[key] = max(expected_per_key.get(key, 0), end)

        if total_rows is not None and total_rows > 0:
            expected_per_key[key] = max(expected_per_key.get(key, 0), total_rows)

    rows_expected_total = sum(max(0, v) for v in expected_per_key.values())
    rows_retrieved_total = 0
    merged_ranges: Dict[str, List[Tuple[int, int]]] = {}

    for key, ranges in ranges_per_key.items():
        merged = merge_ranges(ranges)
        rows_retrieved_total += sum((end - start + 1) for start, end in merged)
        key_str = f"{key[0]}::{key[1]}"
        merged_ranges[key_str] = merged

    ratio = float(rows_retrieved_total / rows_expected_total) if rows_expected_total > 0 else 0.0
    return {
        "rows_expected_total": int(rows_expected_total),
        "rows_retrieved_total": int(rows_retrieved_total),
        "row_coverage_ratio": ratio,
        "row_ranges_merged": merged_ranges,
    }


def estimate_text_tokens(text: str) -> int:
    # Fast, provider-agnostic approximation for observability/debug.
    if not text:
        return 0
    return max(1, int(len(text) / 4))


def build_standard_rag_debug_payload(
    *,
    rag_debug: Optional[Dict[str, Any]],
    context_docs: List[Dict[str, Any]],
    rag_sources: List[str],
    llm_tokens_used: Optional[int],
    provider_debug: Optional[Dict[str, Any]] = None,
    max_items: int = 8,
) -> Dict[str, Any]:
    payload = dict(rag_debug or {})
    top_chunks = build_top_chunks_debug(context_docs, max_items=max_items)
    avg_score = (
        sum(float(item.get("score", 0.0) or 0.0) for item in top_chunks) / len(top_chunks)
        if top_chunks
        else 0.0
    )
    context_tokens = sum(estimate_text_tokens((doc.get("content") or "")) for doc in context_docs)
    payload["filters"] = payload.get("filters") or payload.get("where")
    payload["applied_filters"] = payload["filters"]
    payload["route"] = payload.get("execution_route") or payload.get("route") or "unknown"
    payload["strategy_mode"] = payload.get("strategy_mode") or (
        payload.get("planner_decision", {}).get("strategy_mode")
        if isinstance(payload.get("planner_decision"), dict)
        else None
    )
    payload["analytical_mode_used"] = bool(payload.get("analytical_mode_used", False))
    payload["top_chunks"] = top_chunks
    payload["top_chunks_limit"] = max_items
    payload["top_chunks_total"] = len(context_docs)
    payload["sources"] = rag_sources
    payload["retrieval_hits"] = int(payload.get("returned_count", len(context_docs)) or 0)
    payload["retrieved_chunks_total"] = len(context_docs)
    payload["avg_score"] = float(avg_score)
    payload["avg_similarity"] = float(avg_score)
    payload["top_similarity_scores"] = [float(item.get("score", 0.0) or 0.0) for item in top_chunks[:10]]
    payload["context_tokens"] = int(context_tokens)
    payload["llm_tokens_used"] = llm_tokens_used
    retrieval_mode = str(payload.get("retrieval_mode") or "")
    structured_modes = {"tabular_sql", "tabular_combined", "complex_analytics"}
    payload["retrieval_path"] = "structured" if retrieval_mode.startswith("tabular_sql") or retrieval_mode in structured_modes else "vector"
    payload["structured_path_used"] = bool(payload["retrieval_path"] == "structured")
    filters = payload.get("filters")
    if isinstance(filters, dict):
        processing_filter = filters.get("processing_id")
        if isinstance(processing_filter, dict) and isinstance(processing_filter.get("$in"), list):
            payload["active_processing_ids"] = [str(x) for x in processing_filter.get("$in")]

    row_stats = build_row_coverage_stats(context_docs)
    rows_expected = payload.get("rows_expected_total", row_stats["rows_expected_total"])
    rows_retrieved = payload.get("rows_retrieved_total", row_stats["rows_retrieved_total"])
    rows_used_map = payload.get("rows_used_map_total", rows_retrieved)
    rows_used_reduce = payload.get("rows_used_reduce_total", rows_used_map)
    payload["rows_expected_total"] = int(rows_expected or 0)
    payload["rows_retrieved_total"] = int(rows_retrieved or 0)
    payload["rows_used_map_total"] = int(rows_used_map or 0)
    payload["rows_used_reduce_total"] = int(rows_used_reduce or 0)
    if payload.get("rows_expected_total", 0):
        payload["row_coverage_ratio"] = float(
            payload.get("rows_used_reduce_total", 0) / max(1, payload.get("rows_expected_total", 0))
        )
    else:
        payload["row_coverage_ratio"] = float(row_stats["row_coverage_ratio"])

    debug_info = provider_debug if isinstance(provider_debug, dict) else payload.get("provider_debug")
    if isinstance(debug_info, dict):
        payload["provider_debug"] = debug_info
        payload["prompt_chars_before"] = int(debug_info.get("prompt_chars_before", 0) or 0)
        payload["prompt_chars_after"] = int(debug_info.get("prompt_chars_after", 0) or 0)
        payload["prompt_truncated"] = bool(debug_info.get("prompt_truncated", False))
        if "prompt_chars_requested" in debug_info:
            payload["prompt_chars_requested"] = int(debug_info.get("prompt_chars_requested", 0) or 0)
        if "prompt_chars_configured" in debug_info:
            payload["prompt_chars_configured"] = int(debug_info.get("prompt_chars_configured", 0) or 0)
        if "prompt_chars_limit" in debug_info:
            payload["prompt_chars_limit"] = int(debug_info.get("prompt_chars_limit", 0) or 0)
    return payload


def build_context_coverage_summary(context_documents: List[Dict[str, Any]], max_items: int = 12) -> str:
    lines = build_coverage_sources(context_documents=context_documents, max_items=max_items)
    if not lines:
        return "- coverage details are unavailable"
    return "\n".join([f"- {line}" for line in lines])

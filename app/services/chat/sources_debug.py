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
                "chunk_index": chunk_index,
                "row_start": meta.get("row_start"),
                "row_end": meta.get("row_end"),
                "total_rows": meta.get("total_rows"),
                "preview": (doc.get("content") or "")[:220],
            }
        )
    return rows


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


def build_context_coverage_summary(context_documents: List[Dict[str, Any]], max_items: int = 12) -> str:
    lines = build_coverage_sources(context_documents=context_documents, max_items=max_items)
    if not lines:
        return "- coverage details are unavailable"
    return "\n".join([f"- {line}" for line in lines])

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import UUID

from app.core.config import settings


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class DerivedArtifactsResult:
    manifest_path: Path
    summary: Dict[str, Any]


def _doc_preview(text: str, *, max_chars: int = 360) -> str:
    value = str(text or "").strip()
    if len(value) <= max_chars:
        return value
    return value[:max_chars] + "..."


def _extract_tabular_summary(docs: List[Any]) -> Dict[str, Any]:
    file_summary = None
    sheet_summaries: List[Dict[str, Any]] = []
    row_windows: List[Dict[str, Any]] = []
    schema_snapshot: Dict[str, Any] = {}
    for doc in docs:
        meta = dict(getattr(doc, "metadata", {}) or {})
        chunk_type = str(meta.get("chunk_type") or "").lower()
        if chunk_type == "file_summary" and file_summary is None:
            file_summary = {
                "content_preview": _doc_preview(getattr(doc, "page_content", "")),
                "metadata": meta,
            }
            schema_meta = meta.get("schema_snapshot")
            if isinstance(schema_meta, dict):
                schema_snapshot = dict(schema_meta)
        elif chunk_type == "sheet_summary":
            sheet_summaries.append(
                {
                    "sheet_name": str(meta.get("sheet_name") or ""),
                    "total_rows": int(meta.get("total_rows", 0) or 0),
                    "columns": list(meta.get("columns") or []),
                    "inferred_types": dict(meta.get("inferred_types") or {}),
                    "preview_rows": list(meta.get("preview_rows") or []),
                }
            )
        elif chunk_type == "row_group":
            row_windows.append(
                {
                    "sheet_name": str(meta.get("sheet_name") or ""),
                    "row_start": int(meta.get("row_start", 0) or 0),
                    "row_end": int(meta.get("row_end", 0) or 0),
                    "total_rows": int(meta.get("total_rows", 0) or 0),
                    "columns": list(meta.get("columns") or []),
                }
            )

    csv_parse = None
    if file_summary and isinstance(file_summary.get("metadata"), dict):
        maybe_csv = file_summary["metadata"].get("csv_parse")
        if isinstance(maybe_csv, dict):
            csv_parse = dict(maybe_csv)

    workbook_summary = {
        "sheets_total": len(sheet_summaries),
        "sheet_names": [str(item.get("sheet_name") or "") for item in sheet_summaries],
    }
    return {
        "file_summary": file_summary,
        "workbook_summary": workbook_summary,
        "sheet_summaries": sheet_summaries,
        "row_windows": row_windows,
        "schema_snapshot": schema_snapshot,
        "csv_parse": csv_parse,
    }


def _extract_document_summary(docs: List[Any]) -> Dict[str, Any]:
    extracted_chars = 0
    extracted_preview_parts: List[str] = []
    for doc in docs:
        content = str(getattr(doc, "page_content", "") or "").strip()
        if not content:
            continue
        extracted_chars += len(content)
        if len(extracted_preview_parts) < 3:
            extracted_preview_parts.append(_doc_preview(content, max_chars=240))
    return {
        "extracted_text": {
            "chars": extracted_chars,
            "preview": "\n\n".join(extracted_preview_parts),
        }
    }


def persist_derived_artifacts(
    *,
    file_id: UUID,
    processing_id: Optional[UUID],
    file_path: Path,
    docs: List[Any],
    pipeline_version: Optional[str],
    parser_version: Optional[str],
    artifact_version: Optional[str],
    owner_user_id: Optional[UUID],
) -> DerivedArtifactsResult:
    processing_segment = str(processing_id) if processing_id is not None else "no-processing"
    output_dir = settings.get_file_artifacts_dir() / str(file_id) / processing_segment
    output_dir.mkdir(parents=True, exist_ok=True)

    chunk_type_counter = Counter()
    is_tabular = False
    for doc in docs:
        meta = dict(getattr(doc, "metadata", {}) or {})
        chunk_type = str(meta.get("chunk_type") or "unknown")
        source_type = str(meta.get("source_type") or "")
        if source_type == "tabular" or str(meta.get("file_type") or "").lower() in {"xlsx", "xls", "csv", "tsv"}:
            is_tabular = True
        chunk_type_counter[chunk_type] += 1

    raw_stats = file_path.stat() if file_path.exists() else None
    manifest: Dict[str, Any] = {
        "file_id": str(file_id),
        "processing_id": str(processing_id) if processing_id is not None else None,
        "owner_user_id": str(owner_user_id) if owner_user_id is not None else None,
        "raw_file": {
            "path": str(file_path),
            "exists": bool(file_path.exists()),
            "size_bytes": int(raw_stats.st_size) if raw_stats is not None else 0,
            "extension": str(file_path.suffix.lower().lstrip(".")),
        },
        "pipeline": {
            "pipeline_version": str(pipeline_version or ""),
            "parser_version": str(parser_version or ""),
            "artifact_version": str(artifact_version or ""),
        },
        "derived_at": _utc_now_iso(),
        "artifact_counts": dict(chunk_type_counter),
        "total_artifacts": int(sum(chunk_type_counter.values())),
        "source_type": "tabular" if is_tabular else "document",
    }
    manifest.update(_extract_document_summary(docs))
    if is_tabular:
        manifest["tabular"] = _extract_tabular_summary(docs)

    artifacts_index: List[Dict[str, Any]] = []
    for idx, doc in enumerate(docs):
        meta = dict(getattr(doc, "metadata", {}) or {})
        artifacts_index.append(
            {
                "artifact_index": idx,
                "artifact_type": str(meta.get("artifact_type") or meta.get("chunk_type") or "unknown"),
                "chunk_type": str(meta.get("chunk_type") or "unknown"),
                "sheet_name": meta.get("sheet_name"),
                "chunk_index": meta.get("chunk_index"),
                "row_start": meta.get("row_start"),
                "row_end": meta.get("row_end"),
                "metadata": meta,
                "content_preview": _doc_preview(str(getattr(doc, "page_content", "") or "")),
            }
        )
    manifest["artifacts_index"] = artifacts_index

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    summary = {
        "source_type": manifest["source_type"],
        "manifest_path": str(manifest_path),
        "artifact_counts": dict(chunk_type_counter),
        "total_artifacts": int(manifest["total_artifacts"]),
    }
    if is_tabular:
        tabular = manifest.get("tabular") if isinstance(manifest.get("tabular"), dict) else {}
        summary["sheet_count"] = int(len(tabular.get("sheet_summaries") or []))
        summary["row_windows"] = int(len(tabular.get("row_windows") or []))
    return DerivedArtifactsResult(manifest_path=manifest_path, summary=summary)

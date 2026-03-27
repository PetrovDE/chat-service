"""
Document loader based on LangChain loaders.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from langchain_core.documents import Document

from app.core.config import settings
from app.services.tabular.parsing import (
    dataframe_preview_rows,
    infer_column_types,
    read_csv_with_detection,
    read_excel_sheets,
)

logger = logging.getLogger(__name__)


class DocumentLoader:
    def __init__(self) -> None:
        self.supported_loaders = {
            ".pdf": self.load_pdf,
            ".docx": self.load_docx,
            ".txt": self.load_text,
            ".csv": self.load_csv,
            ".tsv": self.load_tsv,
            ".xlsx": self.load_excel,
            ".xls": self.load_excel,
            ".json": self.load_json,
            ".md": self.load_markdown,
        }
        logger.info("DocumentLoader initialized")

    @staticmethod
    def _normalize_cell(value: Any, *, max_len: int = 0) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        if max_len and max_len > 0 and len(text) > max_len:
            return text[:max_len] + "..."
        return text

    def _normalize_tabular_dataframe(self, df):
        import pandas as pd

        if df is None:
            return pd.DataFrame()

        work = df.copy()
        work.columns = [str(col or "").strip() or f"col_{idx + 1}" for idx, col in enumerate(work.columns)]
        work = work.fillna("")

        for col in work.columns:
            work[col] = work[col].astype(str).map(lambda value: value.strip())

        if not work.empty:
            row_non_empty = (work != "").any(axis=1)
            work = work.loc[row_non_empty]
        if not work.empty:
            non_empty_cols = [col for col in work.columns if bool((work[col] != "").any())]
            work = work[non_empty_cols]

        return work.reset_index(drop=True)

    @staticmethod
    def _infer_column_kind(series) -> str:
        # Keep backward-compatible method while delegating to shared parsing logic.
        from app.services.tabular.parsing import infer_series_kind

        return infer_series_kind(series)

    def _row_group_size(self, columns_count: int) -> int:
        medium_threshold = int(getattr(settings, "TABULAR_ROW_GROUP_MEDIUM_COLUMNS_THRESHOLD", 12) or 12)
        wide_threshold = int(getattr(settings, "TABULAR_ROW_GROUP_WIDE_COLUMNS_THRESHOLD", 40) or 40)
        rows_narrow = int(getattr(settings, "TABULAR_ROW_GROUP_ROWS_NARROW", 200) or 200)
        rows_medium = int(getattr(settings, "TABULAR_ROW_GROUP_ROWS_MEDIUM", 100) or 100)
        rows_wide = int(getattr(settings, "TABULAR_ROW_GROUP_ROWS_WIDE", 50) or 50)

        if columns_count >= wide_threshold:
            return max(10, rows_wide)
        if columns_count >= medium_threshold:
            return max(20, rows_medium)
        return max(20, rows_narrow)

    def _build_file_summary_doc(
        self,
        *,
        filepath: str,
        file_type: str,
        metadata: Optional[Dict[str, Any]],
        sheet_profiles: List[Dict[str, Any]],
        csv_parse: Optional[Dict[str, Any]] = None,
        workbook_parse: Optional[Dict[str, Any]] = None,
    ) -> Optional[Document]:
        if not sheet_profiles:
            return None

        total_rows = sum(int(item.get("rows", 0) or 0) for item in sheet_profiles)
        total_cols_unique = sorted(
            {
                str(col)
                for item in sheet_profiles
                for col in (item.get("columns") or [])
                if str(col).strip()
            }
        )
        lines = [
            f"Table file summary: type={file_type} sheets={len(sheet_profiles)} total_rows={total_rows} unique_columns={len(total_cols_unique)}"
        ]
        for profile in sheet_profiles[:20]:
            sheet_name = str(profile.get("sheet_name") or "Sheet")
            rows = int(profile.get("rows", 0) or 0)
            cols = int(profile.get("cols", 0) or 0)
            lines.append(f"- sheet={sheet_name} rows={rows} cols={cols}")

        top_columns = int(getattr(settings, "TABULAR_SUMMARY_TOP_COLUMNS", 12) or 12)
        if total_cols_unique:
            lines.append("Top columns: " + ", ".join(total_cols_unique[:top_columns]))
        if csv_parse:
            lines.append(
                "CSV parse: "
                f"encoding={csv_parse.get('encoding')} delimiter={csv_parse.get('delimiter')} "
                f"header_detected={csv_parse.get('header_detected')}"
            )
        if workbook_parse:
            lines.append(
                "Workbook parse: "
                f"sheets_total={workbook_parse.get('sheets_total')} "
                f"source_type={workbook_parse.get('source_type')}"
            )

        doc_meta: Dict[str, Any] = {
            "source": filepath,
            "file_type": file_type,
            "source_type": "tabular",
            "artifact_type": "file_summary",
            "chunk_type": "file_summary",
            "sheets_count": len(sheet_profiles),
            "total_rows": total_rows,
            "columns_total_unique": len(total_cols_unique),
            "table_aware_version": 2,
            "schema_snapshot": {
                str(item.get("sheet_name") or "Sheet"): {
                    "rows": int(item.get("rows", 0) or 0),
                    "cols": int(item.get("cols", 0) or 0),
                    "columns": list(item.get("columns") or []),
                    "inferred_types": dict(item.get("inferred_types") or {}),
                }
                for item in sheet_profiles
            },
        }
        if csv_parse:
            doc_meta["csv_parse"] = dict(csv_parse)
        if workbook_parse:
            doc_meta["workbook_parse"] = dict(workbook_parse)
        if metadata:
            doc_meta.update(metadata)
        return Document(page_content="\n".join(lines), metadata=doc_meta)

    def _build_column_summary_docs(
        self,
        *,
        filepath: str,
        file_type: str,
        metadata: Optional[Dict[str, Any]],
        sheet_name: str,
        column_types: Dict[str, str],
        preview_rows: List[Dict[str, str]],
    ) -> List[Document]:
        if not bool(getattr(settings, "TABULAR_COLUMN_SUMMARY_ENABLED", True)):
            return []
        max_columns = int(getattr(settings, "TABULAR_COLUMN_SUMMARY_MAX_COLUMNS", 6) or 6)
        out: List[Document] = []
        for col_name, col_kind in list(column_types.items())[:max_columns]:
            examples: List[str] = []
            for row in preview_rows:
                value = str(row.get(col_name, "") or "").strip()
                if value and value not in examples:
                    examples.append(value)
                if len(examples) >= 3:
                    break
            text = (
                f"Column summary: sheet={sheet_name} column={col_name} inferred_type={col_kind}\n"
                f"Examples: {', '.join(examples) if examples else 'n/a'}"
            )
            item_meta: Dict[str, Any] = {
                "source": filepath,
                "file_type": file_type,
                "source_type": "tabular",
                "artifact_type": "column_summary",
                "chunk_type": "column_summary",
                "sheet_name": sheet_name,
                "column_name": col_name,
                "inferred_type": col_kind,
                "table_aware_version": 2,
            }
            if metadata:
                item_meta.update(metadata)
            out.append(Document(page_content=text, metadata=item_meta))
        return out

    def _cap_tabular_docs(self, docs: List[Document]) -> List[Document]:
        max_docs = int(getattr(settings, "TABULAR_MAX_EMBEDDING_DOCS", 320) or 320)
        if len(docs) <= max_docs:
            return docs

        summary_types = {"file_summary", "sheet_summary", "column_summary"}
        summaries = [doc for doc in docs if str((doc.metadata or {}).get("chunk_type") or "") in summary_types]
        row_groups = [doc for doc in docs if str((doc.metadata or {}).get("chunk_type") or "") == "row_group"]
        others = [
            doc
            for doc in docs
            if doc not in summaries and doc not in row_groups
        ]

        protected = summaries + others
        if len(protected) >= max_docs:
            return protected[:max_docs]

        allowed_row_groups = max_docs - len(protected)
        if len(row_groups) <= allowed_row_groups:
            return protected + row_groups
        if allowed_row_groups <= 0:
            return protected

        if allowed_row_groups == 1:
            picked_indices = [0]
        else:
            step = float(len(row_groups) - 1) / float(allowed_row_groups - 1)
            picked_indices = [int(round(step * i)) for i in range(allowed_row_groups)]

        selected: List[Document] = []
        seen = set()
        for idx in picked_indices:
            idx = max(0, min(idx, len(row_groups) - 1))
            if idx in seen:
                continue
            seen.add(idx)
            selected.append(row_groups[idx])

        if row_groups and row_groups[-1] not in selected and len(selected) < allowed_row_groups:
            selected.append(row_groups[-1])

        logger.warning(
            "Tabular doc cap applied: before=%d after=%d summaries=%d row_groups_before=%d row_groups_after=%d",
            len(docs),
            len(protected) + len(selected),
            len(summaries),
            len(row_groups),
            len(selected),
        )
        return protected + selected

    def _pick_columns_for_chunk(self, df, columns: List[str]) -> Tuple[List[str], List[str]]:
        max_cols = int(getattr(settings, "XLSX_MAX_COLUMNS_PER_CHUNK", 0) or 0)
        if max_cols <= 0 or len(columns) <= max_cols:
            return columns, []

        density: List[Tuple[int, int, str]] = []
        for idx, col in enumerate(columns):
            try:
                non_empty = int((df[col].astype(str).str.strip() != "").sum())
            except Exception:
                non_empty = 0
            density.append((non_empty, -idx, col))

        density.sort(reverse=True)
        picked = [col for non_empty, _neg_idx, col in density if non_empty > 0][:max_cols]
        if not picked:
            picked = columns[:max_cols]

        picked_set = set(picked)
        selected = [col for col in columns if col in picked_set]
        pruned = [col for col in columns if col not in picked_set]
        return selected, pruned

    def _build_tabular_docs(
        self,
        *,
        df,
        filepath: str,
        file_type: str,
        metadata: Optional[Dict[str, Any]],
        sheet_name: Optional[str] = None,
        sheet_count: int = 1,
    ) -> List[Document]:
        df = self._normalize_tabular_dataframe(df)
        if df.empty:
            return []

        max_chars = int(getattr(settings, "XLSX_CHUNK_MAX_CHARS", 9000) or 9000)
        max_rows_hard = int(getattr(settings, "XLSX_CHUNK_MAX_ROWS", 40) or 40)
        configured_cell_chars = int(getattr(settings, "XLSX_CELL_MAX_CHARS", 0) or 0)
        hard_cell_chars = int(getattr(settings, "TABULAR_WIDE_CELL_HARD_LIMIT", 2000) or 2000)
        max_cell_chars = configured_cell_chars if configured_cell_chars > 0 else hard_cell_chars
        max_chars = max(1000, max_chars)
        max_rows_hard = max(1, max_rows_hard)

        columns_all = [str(c) for c in df.columns.tolist()]
        selected_columns, pruned_columns = self._pick_columns_for_chunk(df, columns_all)
        total_rows = len(df)
        sheet_label = str(sheet_name or "CSV")
        row_group_size = min(max_rows_hard, self._row_group_size(len(selected_columns)))
        row_group_size = max(1, row_group_size)
        column_types = infer_column_types(df[selected_columns]) if selected_columns else {}
        preview_rows = dataframe_preview_rows(df[selected_columns], max_rows=5) if selected_columns else []

        docs: List[Document] = []
        top_columns = int(getattr(settings, "TABULAR_SUMMARY_TOP_COLUMNS", 12) or 12)
        summary_lines = [
            (
                f"Sheet summary: sheet={sheet_label} total_rows={total_rows} selected_columns={len(selected_columns)} "
                f"all_columns={len(columns_all)} row_group_size={row_group_size}"
            ),
            "Columns: " + ", ".join(selected_columns[:top_columns]),
        ]
        if pruned_columns:
            summary_lines.append(
                f"Pruned columns for embeddings: {len(pruned_columns)} (examples: {', '.join(pruned_columns[:min(8, len(pruned_columns))])})"
            )
        typed_cols = [f"{col}:{column_types.get(col, 'text')}" for col in selected_columns[:top_columns]]
        if typed_cols:
            summary_lines.append("Inferred column types: " + ", ".join(typed_cols))
        docs.append(
            Document(
                page_content="\n".join(summary_lines),
                metadata={
                    "source": filepath,
                    "file_type": file_type,
                    "source_type": "tabular",
                    "artifact_type": "sheet_summary",
                    "sheet_name": sheet_label,
                    "chunk_type": "sheet_summary",
                    "total_rows": total_rows,
                    "columns": selected_columns,
                    "columns_all": columns_all,
                    "columns_pruned": bool(pruned_columns),
                    "pruned_columns": pruned_columns,
                    "sheet_count": sheet_count,
                    "row_group_size": row_group_size,
                    "inferred_types": column_types,
                    "preview_rows": preview_rows,
                    "table_aware_version": 2,
                    **(metadata or {}),
                },
            )
        )
        docs.extend(
            self._build_column_summary_docs(
                filepath=filepath,
                file_type=file_type,
                metadata=metadata,
                sheet_name=sheet_label,
                column_types=column_types,
                preview_rows=preview_rows,
            )
        )

        chunk_lines: List[str] = []
        chunk_row_start = 0
        chunk_chars = 0
        chunk_source_rows = 0
        row_group_index = 0

        def build_row_lines(row_number: int, row_values: List[str]) -> List[str]:
            prefix = f"Row {row_number}: "
            cont_prefix = f"Row {row_number} (cont): "
            max_payload = max(120, max_chars - len(cont_prefix) - 8)
            if not row_values:
                return [prefix + "<empty>"]

            lines: List[str] = []
            current = ""
            for part in row_values:
                candidate = part if not current else f"{current} | {part}"
                head = prefix if not lines else cont_prefix
                if len(head) + len(candidate) <= max_chars:
                    current = candidate
                    continue
                if current:
                    lines.append(head + current)
                remain = part
                while len(cont_prefix) + len(remain) > max_chars:
                    chunk = remain[:max_payload]
                    lines.append((prefix if not lines else cont_prefix) + chunk)
                    remain = remain[max_payload:]
                current = remain
            if current:
                lines.append((prefix if not lines else cont_prefix) + current)
            return lines

        def flush_chunk(row_end_idx: int) -> None:
            nonlocal chunk_lines, chunk_row_start, chunk_chars, chunk_source_rows, row_group_index
            if not chunk_lines:
                return
            row_start = chunk_row_start + 1
            row_end = row_end_idx + 1
            header = f"sheet={sheet_label} rows={row_start}-{row_end}/{total_rows}"
            page = (header + "\n" + "\n".join(chunk_lines)).strip()
            if not page:
                chunk_lines = []
                chunk_chars = 0
                return

            doc_meta: Dict[str, Any] = {
                "source": filepath,
                "file_type": file_type,
                "source_type": "tabular",
                "artifact_type": "row_group",
                "sheet_name": sheet_label,
                "row_start": row_start,
                "row_end": row_end,
                "total_rows": total_rows,
                "columns": selected_columns,
                "columns_all": columns_all,
                "columns_pruned": bool(pruned_columns),
                "pruned_columns": pruned_columns,
                "sheet_count": sheet_count,
                "chunk_type": "row_group",
                "row_group_index": row_group_index,
                "row_group_size": row_group_size,
                "inferred_types": column_types,
                "table_aware_version": 2,
            }
            if metadata:
                doc_meta.update(metadata)

            docs.append(Document(page_content=page, metadata=doc_meta))
            row_group_index += 1
            chunk_lines = []
            chunk_chars = 0
            chunk_source_rows = 0

        for ridx in range(total_rows):
            row = df.iloc[ridx]
            values: List[str] = []
            for col in selected_columns:
                cell = self._normalize_cell(row.get(col), max_len=max_cell_chars)
                if cell:
                    values.append(f"{col}: {cell}")
            row_lines = build_row_lines(ridx + 1, values)
            row_chars = sum(len(line) for line in row_lines) + max(0, len(row_lines) - 1)

            if chunk_lines and ((chunk_source_rows + 1) > row_group_size or (chunk_chars + row_chars) > max_chars):
                flush_chunk(ridx - 1)
                chunk_row_start = ridx

            if not chunk_lines:
                chunk_row_start = ridx
            for line in row_lines:
                if chunk_lines and (chunk_chars + len(line) + 1) > max_chars:
                    flush_chunk(ridx - 1)
                    chunk_row_start = ridx
                if not chunk_lines:
                    chunk_row_start = ridx
                chunk_lines.append(line)
                chunk_chars += len(line) + (1 if len(chunk_lines) > 1 else 0)
            chunk_source_rows += 1

        flush_chunk(total_rows - 1)
        return docs

    async def load_file(self, filepath: str, metadata: Optional[Dict[str, Any]] = None) -> List[Document]:
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        file_size_mb = path.stat().st_size / (1024 * 1024)
        if file_size_mb > settings.MAX_FILESIZE_MB:
            raise ValueError(
                f"File too large: {file_size_mb:.2f} MB > {settings.MAX_FILESIZE_MB} MB"
            )

        ext = path.suffix.lower()
        if not settings.is_file_supported(filepath) or ext not in self.supported_loaders:
            raise ValueError(f"Filetype {ext} not supported")

        logger.info("Loading file: %s (%.2f MB)", path.name, file_size_mb)

        docs = await self.supported_loaders[ext](filepath, metadata)

        for d in docs:
            d.metadata = d.metadata or {}
            d.metadata.setdefault("source", filepath)
            d.metadata.setdefault("file_type", ext.lstrip("."))
            file_type = str(d.metadata.get("file_type") or ext.lstrip(".")).lower()
            is_tabular = file_type in {"xlsx", "xls", "csv", "tsv"}
            d.metadata.setdefault("source_type", "tabular" if is_tabular else "document")
            chunk_type = str(d.metadata.get("chunk_type") or "").strip().lower()
            if not chunk_type:
                chunk_type = "extracted_text"
                d.metadata["chunk_type"] = chunk_type
            d.metadata.setdefault("artifact_type", chunk_type)

        if not docs or not any((d.page_content or "").strip() for d in docs):
            raise ValueError(f"No readable content extracted from file: {filepath}")

        return docs

    async def load_pdf(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import PyPDFLoader

        loader = PyPDFLoader(filepath)
        primary_docs = loader.load()
        if metadata:
            for d in primary_docs:
                d.metadata.update(metadata)

        if not self._is_near_empty_pdf_extraction(primary_docs):
            return primary_docs

        logger.warning("Primary PDF extraction is near-empty, trying fallback parser: %s", filepath)
        fallback_docs = self._load_pdf_fallback(filepath=filepath, metadata=metadata)
        if not fallback_docs:
            return primary_docs

        primary_chars = self._pdf_non_whitespace_chars(primary_docs)
        fallback_chars = self._pdf_non_whitespace_chars(fallback_docs)
        if fallback_chars > primary_chars:
            logger.info(
                "PDF fallback extraction selected: file=%s primary_non_ws=%d fallback_non_ws=%d",
                Path(filepath).name,
                primary_chars,
                fallback_chars,
            )
            return fallback_docs
        return primary_docs

    @staticmethod
    def _pdf_non_whitespace_chars(docs: List[Document]) -> int:
        total = 0
        for doc in docs:
            total += len(re.sub(r"\s+", "", str(doc.page_content or "")))
        return total

    @classmethod
    def _is_near_empty_pdf_extraction(cls, docs: List[Document]) -> bool:
        if not docs:
            return True
        non_empty_pages = 0
        meaningful_pages = 0
        for doc in docs:
            non_ws_chars = len(re.sub(r"\s+", "", str(doc.page_content or "")))
            if non_ws_chars <= 0:
                continue
            non_empty_pages += 1
            if non_ws_chars >= 24:
                meaningful_pages += 1
        if non_empty_pages <= 0:
            return True
        if meaningful_pages > 0:
            return False
        return cls._pdf_non_whitespace_chars(docs) < max(24, non_empty_pages * 16)

    def _load_pdf_fallback(self, *, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        try:
            from pypdf import PdfReader
        except Exception:
            logger.warning("PDF fallback parser unavailable (pypdf import failed): %s", filepath, exc_info=True)
            return []

        try:
            reader = PdfReader(filepath)
        except Exception:
            logger.warning("PDF fallback parser failed to open file: %s", filepath, exc_info=True)
            return []

        docs: List[Document] = []
        for index, page in enumerate(reader.pages):
            try:
                text = str(page.extract_text() or "").strip()
            except Exception:
                text = ""
            if not text:
                continue
            page_meta: Dict[str, Any] = {
                "source": filepath,
                "page": index,
                "page_label": str(index + 1),
            }
            if metadata:
                page_meta.update(metadata)
            docs.append(Document(page_content=text, metadata=page_meta))
        return docs

    async def load_docx(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import Docx2txtLoader

        loader = Docx2txtLoader(filepath)
        docs = loader.load()
        if metadata:
            for d in docs:
                d.metadata.update(metadata)
        return docs

    async def load_text(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import TextLoader
        docs = self._textloader_try_encodings(filepath, TextLoader, ["utf-8", "utf-8-sig", "cp1251"])
        if metadata:
            for d in docs:
                d.metadata.update(metadata)
        return docs

    async def load_csv(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        """
        CSV -> split into row blocks so RAG can target specific rows and columns.
        """
        df, parse_meta = read_csv_with_detection(Path(filepath), forced_delimiter=None)
        if df.empty:
            raise ValueError(f"No readable data found in CSV file: {filepath}")

        tabular_metadata = dict(metadata or {})
        tabular_metadata["csv_parse"] = {
            "encoding": parse_meta.encoding,
            "delimiter": parse_meta.delimiter,
            "header_detected": parse_meta.header_detected,
        }
        docs = self._build_tabular_docs(
            df=df,
            filepath=filepath,
            file_type="csv",
            metadata=tabular_metadata,
            sheet_name="CSV",
            sheet_count=1,
        )
        profile = [
            {
                "sheet_name": "CSV",
                "rows": int(len(df)),
                "cols": int(len(df.columns)),
                "columns": [str(c) for c in df.columns],
                "inferred_types": dict(parse_meta.inferred_types),
                "preview_rows": list(parse_meta.preview_rows),
            }
        ]
        file_summary = self._build_file_summary_doc(
            filepath=filepath,
            file_type="csv",
            metadata=metadata,
            sheet_profiles=profile,
            csv_parse={
                "encoding": parse_meta.encoding,
                "delimiter": parse_meta.delimiter,
                "header_detected": parse_meta.header_detected,
            },
            workbook_parse={
                "source_type": "csv",
                "sheets_total": 1,
            },
        )
        if file_summary is not None:
            docs.insert(0, file_summary)
        return self._cap_tabular_docs(docs)

    async def load_tsv(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        df, parse_meta = read_csv_with_detection(Path(filepath), forced_delimiter="\t")
        if df.empty:
            raise ValueError(f"No readable data found in TSV file: {filepath}")

        tabular_metadata = dict(metadata or {})
        tabular_metadata["csv_parse"] = {
            "encoding": parse_meta.encoding,
            "delimiter": parse_meta.delimiter,
            "header_detected": parse_meta.header_detected,
        }
        docs = self._build_tabular_docs(
            df=df,
            filepath=filepath,
            file_type="tsv",
            metadata=tabular_metadata,
            sheet_name="TSV",
            sheet_count=1,
        )
        profile = [
            {
                "sheet_name": "TSV",
                "rows": int(len(df)),
                "cols": int(len(df.columns)),
                "columns": [str(c) for c in df.columns],
                "inferred_types": dict(parse_meta.inferred_types),
                "preview_rows": list(parse_meta.preview_rows),
            }
        ]
        file_summary = self._build_file_summary_doc(
            filepath=filepath,
            file_type="tsv",
            metadata=metadata,
            sheet_profiles=profile,
            csv_parse={
                "encoding": parse_meta.encoding,
                "delimiter": parse_meta.delimiter,
                "header_detected": parse_meta.header_detected,
            },
            workbook_parse={
                "source_type": "tsv",
                "sheets_total": 1,
            },
        )
        if file_summary is not None:
            docs.insert(0, file_summary)
        return self._cap_tabular_docs(docs)

    async def load_excel(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        """
        Excel parsing strategy:
        - read each sheet separately
        - split by row blocks (for example 30-50)
        - create a separate Document per block with metadata:
          sheet_name, row_start/row_end, columns
        """
        try:
            parsed_sheets = read_excel_sheets(Path(filepath))
        except Exception as e:
            raise ValueError(f"Failed to read Excel file: {filepath}. Error: {e}")

        sheet_names = [sheet_name for (sheet_name, _df) in parsed_sheets]
        logger.info("Processing Excel: sheets=%d", len(sheet_names))

        docs: List[Document] = []
        sheet_profiles: List[Dict[str, Any]] = []
        sheet_docs: List[Document] = []

        for sheet_name, df in parsed_sheets:
            df = self._normalize_tabular_dataframe(df)
            if df is None or df.empty:
                continue

            file_type = Path(filepath).suffix.lower().lstrip(".") or "xlsx"
            inferred_types = infer_column_types(df)
            preview_rows = dataframe_preview_rows(df, max_rows=5)
            sheet_profiles.append(
                {
                    "sheet_name": str(sheet_name),
                    "rows": int(len(df)),
                    "cols": int(len(df.columns)),
                    "columns": [str(col) for col in df.columns],
                    "inferred_types": inferred_types,
                    "preview_rows": preview_rows,
                }
            )
            sheet_docs.extend(
                self._build_tabular_docs(
                    df=df,
                    filepath=filepath,
                    file_type=file_type,
                    metadata=metadata,
                    sheet_name=str(sheet_name),
                    sheet_count=len(sheet_names),
                )
            )

        if sheet_profiles:
            file_summary = self._build_file_summary_doc(
                filepath=filepath,
                file_type=(Path(filepath).suffix.lower().lstrip(".") or "xlsx"),
                metadata=metadata,
                sheet_profiles=sheet_profiles,
                workbook_parse={
                    "source_type": "workbook",
                    "sheets_total": len(sheet_names),
                },
            )
            if file_summary is not None:
                docs.append(file_summary)
        docs.extend(sheet_docs)

        if not docs:
            raise ValueError(f"No readable data found in Excel file: {filepath}")

        return self._cap_tabular_docs(docs)

    async def load_json(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import JSONLoader

        loader = JSONLoader(filepath, jq_schema=".", text_content=False)
        docs = loader.load()
        if metadata:
            for d in docs:
                d.metadata.update(metadata)
        return docs

    async def load_markdown(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        try:
            from langchain_community.document_loaders import UnstructuredMarkdownLoader

            loader = UnstructuredMarkdownLoader(filepath)
            docs = loader.load()
            if docs and any((d.page_content or "").strip() for d in docs):
                if metadata:
                    for d in docs:
                        d.metadata.update(metadata)
                return docs
        except Exception:
            logger.warning("UnstructuredMarkdownLoader failed, fallback to TextLoader", exc_info=True)

        from langchain_community.document_loaders import TextLoader
        docs = self._textloader_try_encodings(filepath, TextLoader, ["utf-8", "utf-8-sig", "cp1251"])
        if metadata:
            for d in docs:
                d.metadata.update(metadata)
        return docs

    def _textloader_try_encodings(self, filepath: str, loader_cls: Any, encodings: List[str]) -> List[Document]:
        last_err: Optional[Exception] = None
        for enc in encodings:
            try:
                loader = loader_cls(filepath, encoding=enc)
                docs = loader.load()
                if docs and any((d.page_content or "").strip() for d in docs):
                    return docs
            except Exception as e:
                last_err = e
                continue
        raise ValueError(f"Failed to read text file with encodings {encodings}: {filepath}. Last error: {last_err}")

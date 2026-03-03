"""
Document loader based on LangChain loaders.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from langchain_core.documents import Document

from app.core.config import settings

logger = logging.getLogger(__name__)


class DocumentLoader:
    def __init__(self) -> None:
        self.supported_loaders = {
            ".pdf": self.load_pdf,
            ".docx": self.load_docx,
            ".txt": self.load_text,
            ".csv": self.load_csv,
            ".xlsx": self.load_excel,
            ".xls": self.load_excel,
            ".json": self.load_json,
            ".md": self.load_markdown,
        }
        logger.info("DocumentLoader initialized")

    @staticmethod
    def _normalize_cell(value: Any, *, max_len: int = 200) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        return text if len(text) <= max_len else (text[:max_len] + "...")

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
        max_chars = int(getattr(settings, "XLSX_CHUNK_MAX_CHARS", 9000) or 9000)
        max_rows = int(getattr(settings, "XLSX_CHUNK_MAX_ROWS", 40) or 40)
        max_chars = max(1000, max_chars)
        max_rows = max(1, max_rows)

        columns_all = [str(c) for c in df.columns.tolist()]
        selected_columns, pruned_columns = self._pick_columns_for_chunk(df, columns_all)
        total_rows = len(df)
        sheet_label = str(sheet_name or "CSV")

        docs: List[Document] = []
        chunk_lines: List[str] = []
        chunk_row_start = 0
        chunk_chars = 0

        def flush_chunk(row_end_idx: int) -> None:
            nonlocal chunk_lines, chunk_row_start, chunk_chars
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
                "sheet_name": sheet_label,
                "row_start": row_start,
                "row_end": row_end,
                "total_rows": total_rows,
                "columns": selected_columns,
                "columns_all": columns_all,
                "columns_pruned": bool(pruned_columns),
                "pruned_columns": pruned_columns,
                "sheet_count": sheet_count,
            }
            if metadata:
                doc_meta.update(metadata)

            docs.append(Document(page_content=page, metadata=doc_meta))
            chunk_lines = []
            chunk_chars = 0

        for ridx in range(total_rows):
            row = df.iloc[ridx]
            values: List[str] = []
            for col in selected_columns:
                cell = self._normalize_cell(row.get(col))
                if cell:
                    values.append(f"{col}: {cell}")
            row_line = f"Row {ridx + 1}: " + (" | ".join(values) if values else "<empty>")
            row_line = row_line[: max_chars - 120] if len(row_line) > max_chars else row_line
            projected_rows = len(chunk_lines) + 1
            projected_chars = chunk_chars + len(row_line) + (1 if chunk_lines else 0)

            if chunk_lines and (projected_rows > max_rows or projected_chars > max_chars):
                flush_chunk(ridx - 1)
                chunk_row_start = ridx

            if not chunk_lines:
                chunk_row_start = ridx
            chunk_lines.append(row_line)
            chunk_chars += len(row_line) + (1 if len(chunk_lines) > 1 else 0)

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

        if not docs or not any((d.page_content or "").strip() for d in docs):
            raise ValueError(f"No readable content extracted from file: {filepath}")

        return docs

    async def load_pdf(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import PyPDFLoader

        loader = PyPDFLoader(filepath)
        docs = loader.load()

        if metadata:
            for d in docs:
                d.metadata.update(metadata)
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
        import pandas as pd

        csv_attempts = [
            {"encoding": "utf-8"},
            {"encoding": "utf-8-sig"},
            {"encoding": "cp1251"},
            {"encoding": "latin-1"},
        ]

        df = None
        last_error: Optional[Exception] = None
        for attempt in csv_attempts:
            try:
                df = pd.read_csv(
                    filepath,
                    dtype=str,
                    keep_default_na=False,
                    engine="python",
                    sep=None,
                    on_bad_lines="skip",
                    **attempt,
                )
                break
            except Exception as e:
                last_error = e
                continue

        if df is None:
            raise ValueError(f"Failed to read CSV file: {filepath}. Last error: {last_error}")
        if df.empty:
            raise ValueError(f"No readable data found in CSV file: {filepath}")

        return self._build_tabular_docs(
            df=df,
            filepath=filepath,
            file_type="csv",
            metadata=metadata,
            sheet_name="CSV",
            sheet_count=1,
        )

    async def load_excel(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        """
        Excel parsing strategy:
        - read each sheet separately
        - split by row blocks (for example 30-50)
        - create a separate Document per block with metadata:
          sheet_name, row_start/row_end, columns
        """
        import pandas as pd

        try:
            excel_file = pd.ExcelFile(filepath)
        except Exception as e:
            raise ValueError(f"Failed to read Excel file: {filepath}. Error: {e}")

        sheet_names = excel_file.sheet_names
        logger.info("Processing Excel: sheets=%d", len(sheet_names))

        docs: List[Document] = []

        for sheet_name in sheet_names:
            try:
                df = pd.read_excel(filepath, sheet_name=sheet_name, dtype=str, keep_default_na=False)
            except Exception:
                logger.warning("Failed to read sheet '%s'", sheet_name, exc_info=True)
                continue

            if df is None or df.empty:
                continue

            file_type = Path(filepath).suffix.lower().lstrip(".") or "xlsx"
            docs.extend(
                self._build_tabular_docs(
                    df=df,
                    filepath=filepath,
                    file_type=file_type,
                    metadata=metadata,
                    sheet_name=str(sheet_name),
                    sheet_count=len(sheet_names),
                )
            )

        if not docs:
            raise ValueError(f"No readable data found in Excel file: {filepath}")

        return docs

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

"""
Document loader based on LangChain loaders.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

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

        # гарантируем source/file_type в metadata
        for d in docs:
            d.metadata = d.metadata or {}
            d.metadata.setdefault("source", filepath)
            d.metadata.setdefault("file_type", ext.lstrip("."))

        # минимальная защита от пустоты
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
        CSV -> делаем блоки строками, чтобы RAG мог попадать в нужные строки/колонки.
        """
        import pandas as pd

        # FIX: dtype=str, keep_default_na=False чтобы не превращать всё в NaN/float
        df = pd.read_csv(filepath, dtype=str, keep_default_na=False, encoding="utf-8", engine="python")
        if df.empty:
            raise ValueError(f"No readable data found in CSV file: {filepath}")

        max_rows_per_doc = 50  # можно вынести в settings
        docs: List[Document] = []

        cols = [str(c) for c in df.columns.tolist()]
        total_rows = len(df)

        for start in range(0, total_rows, max_rows_per_doc):
            end = min(start + max_rows_per_doc, total_rows)
            block = df.iloc[start:end]

            lines: List[str] = []
            lines.append("=" * 60)
            lines.append("CSV")
            lines.append("=" * 60)
            lines.append(f"Колонки: {', '.join(cols)}")
            lines.append(f"Строки: {start + 1}-{end} / {total_rows}")
            lines.append("-" * 60)

            for ridx in range(len(block)):
                row = block.iloc[ridx]
                parts = []
                for col in cols:
                    val = (row.get(col) or "").strip()
                    if val:
                        if len(val) > 200:
                            val = val[:200] + "..."
                        parts.append(f"{col}: {val}")
                if parts:
                    lines.append(f"Row {start + ridx + 1}: " + " | ".join(parts))

            page = "\n".join(lines).strip()
            if not page:
                continue

            meta = {
                "source": filepath,
                "file_type": "csv",
                "row_start": start + 1,
                "row_end": end,
                "total_rows": total_rows,
                "columns": cols,
            }
            if metadata:
                meta.update(metadata)

            docs.append(Document(page_content=page, metadata=meta))

        return docs

    async def load_excel(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        """
        FIX: Excel больше НЕ собираем в один огромный документ.

        Теперь стратегия:
        - читаем каждый лист
        - режем по блокам строк (например по 30–50)
        - каждый блок превращаем в отдельный Document с metadata: sheet_name, row_start/row_end, columns

        Это резко повышает точность RAG (и уменьшает "слипание" контекста).
        """
        import pandas as pd

        try:
            excel_file = pd.ExcelFile(filepath)
        except Exception as e:
            raise ValueError(f"Failed to read Excel file: {filepath}. Error: {e}")

        sheet_names = excel_file.sheet_names
        logger.info("Processing Excel: sheets=%d", len(sheet_names))

        max_rows_per_doc = 40  # можно вынести в settings
        docs: List[Document] = []

        for sheet_name in sheet_names:
            try:
                # FIX: dtype=str + keep_default_na=False чтобы не ломать текстовые значения
                df = pd.read_excel(filepath, sheet_name=sheet_name, dtype=str, keep_default_na=False)
            except Exception:
                logger.warning(f"⚠️ Failed to read sheet '{sheet_name}'", exc_info=True)
                continue

            if df is None or df.empty:
                continue

            cols = [str(c) for c in df.columns.tolist()]
            total_rows = len(df)

            for start in range(0, total_rows, max_rows_per_doc):
                end = min(start + max_rows_per_doc, total_rows)
                block = df.iloc[start:end]

                lines: List[str] = []
                lines.append("=" * 70)
                lines.append(f"EXCEL | ЛИСТ: {sheet_name}")
                lines.append("=" * 70)
                lines.append(f"Колонки: {', '.join(cols)}")
                lines.append(f"Строки: {start + 1}-{end} / {total_rows}")
                lines.append("-" * 70)

                # компактная сериализация строк
                for ridx in range(len(block)):
                    row = block.iloc[ridx]
                    parts = []
                    for col in cols:
                        val = (row.get(col) or "").strip()
                        if val:
                            if len(val) > 200:
                                val = val[:200] + "..."
                            parts.append(f"{col}: {val}")
                    if parts:
                        lines.append(f"Row {start + ridx + 1}: " + " | ".join(parts))

                page = "\n".join(lines).strip()
                if not page:
                    continue

                doc_meta = {
                    "source": filepath,
                    "file_type": "xlsx",
                    "sheet_name": str(sheet_name),
                    "row_start": start + 1,
                    "row_end": end,
                    "total_rows": total_rows,
                    "columns": cols,
                    "sheet_count": len(sheet_names),
                }
                if metadata:
                    doc_meta.update(metadata)

                docs.append(Document(page_content=page, metadata=doc_meta))

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
        # Сначала пробуем Markdown loader, потом fallback в TextLoader с энкодингами
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

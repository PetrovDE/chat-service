# app/rag/document_loader.py

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from langchain_core.documents import Document
from app.core.config import settings

logger = logging.getLogger(__name__)

class DocumentLoader:
    def __init__(self):
        # Получаем поддерживаемые типы из конфигурации
        self.supported_loaders = {
            ".pdf": self.load_pdf,
            ".docx": self.load_docx,
            ".txt": self.load_text,
            ".csv": self.load_csv,
            ".xlsx": self.load_excel,
            ".json": self.load_json,
            ".md": self.load_markdown,
        }
        logger.info("DocumentLoader initialized")

    async def load_file(self, filepath: str, metadata: Optional[Dict[str, Any]] = None) -> List[Document]:
        path = Path(filepath)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        ext = path.suffix.lower()
        if not settings.is_file_supported(filepath):
            raise ValueError(f"Filetype {ext} not supported")
        if path.stat().st_size > settings.MAX_FILESIZE_MB * 1024 * 1024:
            raise ValueError(f"File {filepath} exceeds max allowed size ({settings.MAX_FILESIZE_MB} MB)")

        return await self.supported_loaders[ext](filepath, metadata)

    async def load_pdf(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import PyPDFLoader
        loader = PyPDFLoader(filepath)
        return loader.load()

    async def load_docx(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import Docx2txtLoader
        loader = Docx2txtLoader(filepath)
        return loader.load()

    async def load_text(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import TextLoader
        loader = TextLoader(filepath)
        return loader.load()

    async def load_csv(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import CSVLoader
        loader = CSVLoader(filepath)
        return loader.load()

    async def load_excel(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        """
        Быстрое и безопасное чтение Excel (xlsx) файлов с помощью pandas.
        Решает проблему с undefined entity (&copy; и т.д.), подходит для многопользовательского режима.
        """
        try:
            import pandas as pd
            documents = []
            excel_file = pd.ExcelFile(filepath)
            for sheet_name in excel_file.sheet_names:
                try:
                    df = pd.read_excel(filepath, sheet_name=sheet_name)
                    text_content = []
                    headers = df.columns.tolist()
                    text_content.append("Columns: " + ", ".join(str(h) for h in headers))
                    text_content.append("\n")
                    for idx, row in df.iterrows():
                        row_text = []
                        for col in df.columns:
                            value = row[col]
                            if pd.notna(value):
                                row_text.append(f"{col}: {value}")
                        if row_text:
                            text_content.append(" | ".join(row_text))
                    doc_metadata = {
                        "source": filepath,
                        "sheet_name": sheet_name,
                        "row_count": len(df),
                        "column_count": len(df.columns),
                    }
                    if metadata:
                        doc_metadata.update(metadata)
                    doc = Document(
                        page_content="\n".join(text_content),
                        metadata=doc_metadata
                    )
                    documents.append(doc)
                except Exception as e:
                    logger.warning(f"Error reading sheet '{sheet_name}' from {filepath}: {str(e)}")
                    continue
            if not documents:
                raise ValueError(f"No readable sheets found in Excel file: {filepath}")
            logger.info(f"Successfully loaded {len(documents)} sheets from {filepath}")
            return documents
        except ImportError:
            logger.error("pandas library is required for Excel file loading")
            raise ImportError("Please install pandas: pip install pandas openpyxl")
        except Exception as e:
            logger.error(f"Error loading Excel file {filepath}: {str(e)}")
            raise

    async def load_json(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import JSONLoader
        loader = JSONLoader(filepath)
        return loader.load()

    async def load_markdown(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import UnstructuredMarkdownLoader
        loader = UnstructuredMarkdownLoader(filepath)
        return loader.load()

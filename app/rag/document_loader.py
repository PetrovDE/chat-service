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

        # Вызов нужного загрузчика
        return await self.supported_loaders[ext](filepath, metadata)

    async def load_pdf(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        # Импорт нужной библиотеки (например langchain PDFLoader)
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
        from langchain_community.document_loaders import UnstructuredExcelLoader
        loader = UnstructuredExcelLoader(filepath)
        return loader.load()

    async def load_json(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import JSONLoader
        loader = JSONLoader(filepath)
        return loader.load()

    async def load_markdown(self, filepath: str, metadata: Optional[Dict[str, Any]]) -> List[Document]:
        from langchain_community.document_loaders import UnstructuredMarkdownLoader
        loader = UnstructuredMarkdownLoader(filepath)
        return loader.load()

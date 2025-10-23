# app/rag/async_processor.py
# ⭐ ИСПРАВЛЕННЫЙ И ПРОВЕРЕННЫЙ ФАЙЛ ⭐

import asyncio
import logging
from typing import Optional, Dict, Any
from datetime import datetime

from langchain_community.document_loaders import (
    PyPDFLoader, TextLoader, CSVLoader, JSONLoader,
    UnstructuredExcelLoader, UnstructuredMarkdownLoader,
    UnstructuredPowerPointLoader
)
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.embeddings import OllamaEmbeddings

from app.rag.vector_store import VectorStoreManager

logger = logging.getLogger(__name__)

# Глобальный статус обработки файлов
PROCESSING_STATUS = {}


class RAGAsyncProcessor:
    """
    ✨ Асинхронный RAG процессор

    Использует асинхронные методы для параллельной обработки документов.
    Импорты ТОЛЬКО необходимые, без лишних зависимостей.
    """

    def __init__(self, max_workers: int = 4):
        self.vector_store = VectorStoreManager()
        self.embeddings = OllamaEmbeddings(model="llama3.1:8b")
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
            separators=["\n\n", "\n", ". ", " ", ""]
        )
        self.semaphore = asyncio.Semaphore(max_workers)
        self.loader_map = self._create_loader_map()

    def _create_loader_map(self) -> Dict:
        """Маппинг расширений файлов к загрузчикам"""
        return {
            '.pdf': PyPDFLoader,
            '.txt': TextLoader,
            '.csv': CSVLoader,
            '.json': JSONLoader,
            '.xlsx': UnstructuredExcelLoader,
            '.xls': UnstructuredExcelLoader,
            '.md': UnstructuredMarkdownLoader,
            '.pptx': UnstructuredPowerPointLoader,
            '.ppt': UnstructuredPowerPointLoader,
        }

    async def process_file_async(
            self,
            file_id: str,
            file_path: str,
            user_id: Optional[str] = None,
            db_session=None
    ) -> Dict[str, Any]:
        """
        ⚡ Основной метод фоновой обработки файла
        Выполняется асинхронно в фоне
        """

        async with self.semaphore:
            try:
                # === ИНИЦИАЛИЗАЦИЯ СТАТУСА ===
                PROCESSING_STATUS[file_id] = {
                    "status": "processing",
                    "progress": 0,
                    "started_at": datetime.now().isoformat(),
                    "error": None
                }
                logger.info(f"[{file_id}] 📄 Starting async processing...")

                # === ЭТАП 1: ЗАГРУЗКА ДОКУМЕНТА ===
                PROCESSING_STATUS[file_id]["progress"] = 15
                logger.info(f"[{file_id}] 📥 Loading document...")

                documents = await asyncio.to_thread(
                    self._load_document,
                    file_path,
                    file_id,
                    user_id
                )

                if not documents:
                    raise ValueError("No documents loaded from file")

                logger.info(f"[{file_id}] ✅ Loaded {len(documents)} documents")

                # === ЭТАП 2: РАЗБИВКА НА CHUNKS ===
                PROCESSING_STATUS[file_id]["progress"] = 30
                logger.info(f"[{file_id}] ✂️ Splitting text...")

                chunks = await asyncio.to_thread(
                    self.text_splitter.split_documents,
                    documents
                )

                if not chunks:
                    raise ValueError("No chunks created")

                logger.info(f"[{file_id}] ✅ Created {len(chunks)} chunks")

                # === ЭТАП 3: EMBEDDINGS В BATCH ===
                PROCESSING_STATUS[file_id]["progress"] = 50
                logger.info(f"[{file_id}] 🧠 Creating embeddings (batch processing)...")

                batch_size = 16
                total_chunk_ids = []

                for i in range(0, len(chunks), batch_size):
                    batch = chunks[i:i + batch_size]

                    batch_ids = await asyncio.to_thread(
                        self.vector_store.add_documents,
                        batch
                    )
                    total_chunk_ids.extend(batch_ids)

                    progress = 50 + int((i / len(chunks)) * 40)
                    PROCESSING_STATUS[file_id]["progress"] = min(progress, 85)

                logger.info(f"[{file_id}] ✅ Embeddings created and stored")

                # === ЗАВЕРШЕНИЕ ===
                PROCESSING_STATUS[file_id]["status"] = "completed"
                PROCESSING_STATUS[file_id]["progress"] = 100
                PROCESSING_STATUS[file_id]["completed_at"] = datetime.now().isoformat()
                PROCESSING_STATUS[file_id]["stats"] = {
                    "documents_loaded": len(documents),
                    "chunks_created": len(chunks),
                    "embeddings_stored": len(total_chunk_ids)
                }

                logger.info(
                    f"[{file_id}] ✅ Processing completed:\n"
                    f"  Documents: {len(documents)}\n"
                    f"  Chunks: {len(chunks)}\n"
                    f"  Embeddings: {len(total_chunk_ids)}"
                )

                return {
                    "status": "success",
                    "file_id": file_id,
                    "stats": PROCESSING_STATUS[file_id]["stats"]
                }

            except Exception as e:
                logger.error(f"[{file_id}] ❌ Error: {e}")
                PROCESSING_STATUS[file_id]["status"] = "failed"
                PROCESSING_STATUS[file_id]["error"] = str(e)
                PROCESSING_STATUS[file_id]["completed_at"] = datetime.now().isoformat()

                return {
                    "status": "error",
                    "file_id": file_id,
                    "error": str(e)
                }

    def _load_document(
            self,
            file_path: str,
            file_id: str,
            user_id: Optional[str]
    ) -> list:
        """
        Загрузка документа с правильным loader
        Выполняется в отдельном потоке (to_thread)
        """
        from pathlib import Path

        path = Path(file_path)
        extension = path.suffix.lower()

        # Метаданные для всех документов
        metadata = {
            "file_id": file_id,
            "user_id": user_id,
            "file_name": path.name,
            "source": file_path
        }

        if extension not in self.loader_map:
            logger.warning(f"[{file_id}] Unknown extension {extension}, using TextLoader")
            loader = TextLoader(file_path, encoding="utf-8")
        else:
            loader_class = self.loader_map[extension]

            if extension == '.pdf':
                loader = PyPDFLoader(file_path)
            elif extension == '.csv':
                loader = CSVLoader(file_path, encoding="utf-8")
            elif extension in ['.xlsx', '.xls']:
                loader = UnstructuredExcelLoader(file_path)
            elif extension in ['.pptx', '.ppt']:
                loader = UnstructuredPowerPointLoader(file_path)
            else:
                loader = loader_class(file_path)

        try:
            documents = loader.load()
        except Exception as e:
            logger.error(f"[{file_id}] Failed to load with {loader.__class__.__name__}: {e}")
            try:
                loader = TextLoader(file_path, encoding="utf-8")
                documents = loader.load()
            except:
                return []

        # Добавляем метаданные ко всем документам
        for doc in documents:
            doc.metadata.update(metadata)

        return documents

    async def get_status(self, file_id: str) -> Dict[str, Any]:
        """Получить статус обработки файла"""
        return PROCESSING_STATUS.get(file_id, {
            "status": "not_found",
            "error": "File not in processing queue"
        })

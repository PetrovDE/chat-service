# app/async_processor.py

import logging
from typing import List, Dict, Any, Optional
from app.rag.retriever import rag_retriever
from app.rag.vector_store import vectorstore_manager
from app.rag.embeddings import embeddings_manager

logger = logging.getLogger(__name__)

class AsyncProcessor:
    def __init__(self):
        logger.info("AsyncProcessor initialized")

    async def batch_index_files(self, filepaths: List[str], user_id: str) -> Dict[str, Any]:
        results = []
        for filepath in filepaths:
            try:
                result = await rag_retriever.process_and_store_file(filepath, metadata={"user_id": user_id})
                results.append({"filepath": filepath, "status": "ok", "result": result})
            except Exception as e:
                logger.error(f"Async file processing error: {filepath}: {e}")
                results.append({"filepath": filepath, "status": "error", "error": str(e)})
        return {"batch_processed": results}

    async def reindex_collection(self, collection_name: Optional[str] = None):
        # Очистка коллекции + пересоздание на той же или новой модели
        vectorstore_manager.clear_collection()
        logger.info(f"Collection {collection_name or vectorstore_manager.collection_name} cleared for reindex")
        # TODO: тут можно реализовать массовое добавление — перебор всего upload_dir, запуск для каждого файла

    async def regenerate_embeddings(self, texts: List[str]) -> List[List[float]]:
        # Например — пересоздать эмбеддинги всем документам на новую модель
        logger.info("Batch embeddings generation started")
        return embeddings_manager.embedd_documents(texts)

async_processor = AsyncProcessor()

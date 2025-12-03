# app/rag/vector_store.py
import logging
from typing import List, Dict, Any
from app.core.config import settings

try:
    from chromadb import PersistentClient
except ImportError:
    PersistentClient = None

logger = logging.getLogger(__name__)


class VectorStoreManager:
    def __init__(self, collection_name: str = None, persist_directory: str = None):
        self.collection_name = collection_name or settings.COLLECTION_NAME
        self.persist_directory = persist_directory or str(settings.get_vectordb_path())

        if not PersistentClient:
            raise ImportError("chromadb library not installed")

        self.client = PersistentClient(path=self.persist_directory)
        self.collection = self.client.get_or_create_collection(self.collection_name)
        logger.info(f"VectorStoreManager — {self.collection_name} — {self.persist_directory}")

    def add_document(self, doc_id: str, embedding: list, metadata: dict):
        content = metadata.get('content', '')

        # ✅ ДОБАВЛЕНО: Логирование metadata для отладки
        logger.info(f"Добавление документа {doc_id[:50]}... в {self.collection_name}")
        logger.debug(f"Metadata keys: {list(metadata.keys())}")
        logger.debug(f"conversation_id: {metadata.get('conversation_id')}")
        logger.debug(f"user_id: {metadata.get('user_id')}")
        logger.debug(f"file_id: {metadata.get('file_id')}")

        self.collection.add(
            ids=[doc_id],
            embeddings=[embedding],
            metadatas=[metadata],
            documents=[content]
        )

        # ✅ ДОБАВЛЕНО: Проверка что сохранилось
        saved = self.collection.get(ids=[doc_id])
        if saved and saved['metadatas']:
            saved_metadata = saved['metadatas'][0]
            logger.info(f"✅ Документ сохранен. Saved conversation_id: {saved_metadata.get('conversation_id')}")
        else:
            logger.warning(f"⚠️ Не удалось проверить сохранение документа {doc_id}")

    def query(self, embedding_query: list, top_k: int = None) -> List[Dict[str, Any]]:
        topk = top_k or 5
        logger.info(f"Query top-{topk} {self.collection_name}")

        results = self.collection.query(
            query_embeddings=[embedding_query],
            n_results=topk
        )

        parsed_results = []
        if results and 'ids' in results and len(results['ids']) > 0:
            ids = results['ids'][0]
            metadatas = results.get('metadatas', [[]])[0]
            documents = results.get('documents', [[]])[0]
            distances = results.get('distances', [[]])[0]

            for i, doc_id in enumerate(ids):
                # Get content from documents field first, fallback to metadata
                content = None
                if i < len(documents):
                    content = documents[i]
                if not content and i < len(metadatas):
                    content = metadatas[i].get('content', '')

                current_metadata = metadatas[i] if i < len(metadatas) else {}

                # ✅ ДОБАВЛЕНО: Логирование для отладки
                logger.debug(
                    f"Result {i}: conv_id={current_metadata.get('conversation_id')}, "
                    f"user_id={current_metadata.get('user_id')}"
                )

                parsed_results.append({
                    'id': doc_id,
                    'metadata': current_metadata,
                    'content': content or '',
                    'distance': distances[i] if i < len(distances) else 0.0
                })

        logger.info(f"✅ Found {len(parsed_results)} documents")
        return parsed_results

    def clear_collection(self):
        logger.info(f"Очистка коллекции {self.collection_name}")
        # ✅ ИСПРАВЛЕНО: Правильное удаление всех документов
        try:
            # Получаем все ID
            all_docs = self.collection.get()
            if all_docs and 'ids' in all_docs and all_docs['ids']:
                self.collection.delete(ids=all_docs['ids'])
                logger.info(f"✅ Удалено {len(all_docs['ids'])} документов")
            else:
                logger.info("Коллекция уже пуста")
        except Exception as e:
            logger.error(f"Ошибка очистки коллекции: {e}")

    def recreate_collection(self, new_name: str = None):
        if new_name:
            self.collection_name = new_name
        self.client.delete_collection(self.collection_name)
        self.collection = self.client.get_or_create_collection(self.collection_name)
        logger.info(f"Recreated collection {self.collection_name}")


vectorstore_manager = VectorStoreManager()

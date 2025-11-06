import logging
from app.core.config import settings

try:
    from chromadb import PersistentClient
except ImportError:
    PersistentClient = None  # Для валидных ошибок при отсутствии библиотеки

logger = logging.getLogger(__name__)

class VectorStoreManager:
    def __init__(self,
                 collection_name: str = None,
                 persist_directory: str = None):
        self.collection_name = collection_name or settings.COLLECTION_NAME
        self.persist_directory = persist_directory or str(settings.get_vectordb_path())
        if not PersistentClient:
            raise ImportError("chromadb library not installed")
        # Используем PersistentClient вместо Client с параметром path
        self.client = PersistentClient(path=self.persist_directory)
        self.collection = self.client.get_or_create_collection(self.collection_name)
        logger.info(f"VectorStoreManager — {self.collection_name} — {self.persist_directory}")

    def add_document(self, doc_id: str, embedding: list, metadata: dict):
        logger.info(f"Добавление документа {doc_id} в {self.collection_name}")
        self.collection.add(
            ids=[doc_id],
            embeddings=[embedding],
            metadatas=[metadata]
        )

    def query(self, embedding_query: list, top_k: int = None):
        topk = top_k or settings.CHUNK_SIZE
        logger.info(f"Query top-{topk} {self.collection_name}")
        return self.collection.query(
            query_embeddings=[embedding_query],
            n_results=topk
        )

    def clear_collection(self):
        logger.info(f"Очистка коллекции {self.collection_name}")
        self.collection.delete()

    def recreate_collection(self, new_name: str = None):
        # Вызов для смены модели или структуры хранения
        if new_name:
            self.collection_name = new_name
        self.client.delete_collection(self.collection_name)
        self.collection = self.client.get_or_create_collection(self.collection_name)
        logger.info(f"Recreated collection {self.collection_name}")

vectorstore_manager = VectorStoreManager()

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
        logger.info(f"Добавление документа {doc_id} в {self.collection_name}")
        content = metadata.get('content', '')
        self.collection.add(
            ids=[doc_id],
            embeddings=[embedding],
            metadatas=[metadata],
            documents=[content]
        )

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
                parsed_results.append({
                    'id': doc_id,
                    'metadata': metadatas[i] if i < len(metadatas) else {},
                    'content': documents[i] if i < len(documents) else metadatas[i].get('content', ''),
                    'distance': distances[i] if i < len(distances) else 0.0
                })
        logger.info(f"✅ Found {len(parsed_results)} documents")
        return parsed_results

    def clear_collection(self):
        logger.info(f"Очистка коллекции {self.collection_name}")
        self.collection.delete()

    def recreate_collection(self, new_name: str = None):
        if new_name:
            self.collection_name = new_name
        self.client.delete_collection(self.collection_name)
        self.collection = self.client.get_or_create_collection(self.collection_name)
        logger.info(f"Recreated collection {self.collection_name}")

vectorstore_manager = VectorStoreManager()

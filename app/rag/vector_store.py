# app/rag/vector_store.py
import logging
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path
from langchain_core.documents import Document
from langchain_chroma import Chroma
import chromadb
from chromadb.config import Settings
from app.rag.embeddings import OllamaEmbeddingsManager
from app.rag.config import rag_config

logger = logging.getLogger(__name__)


class VectorStoreManager:
    """
    Менеджер для работы с ChromaDB векторной базой данных
    Хранит документы и их embeddings, выполняет similarity search
    """

    def __init__(
            self,
            collection_name: str = None,
            embeddings_manager: OllamaEmbeddingsManager = None,
            persist_directory: str = None
    ):
        """
        Инициализация vector store

        Args:
            collection_name: Название коллекции в ChromaDB
            embeddings_manager: Менеджер для создания embeddings
            persist_directory: Директория для сохранения БД
        """
        self.collection_name = collection_name or rag_config.collection_name
        self.persist_directory = persist_directory or str(rag_config.get_vector_db_path())

        # Создать embeddings manager если не передан
        self.embeddings_manager = embeddings_manager or OllamaEmbeddingsManager()

        # Инициализировать ChromaDB
        try:
            logger.info(f"🗄️ Initializing ChromaDB at {self.persist_directory}")

            # Создать директорию если не существует
            Path(self.persist_directory).mkdir(parents=True, exist_ok=True)

            # Настройки ChromaDB
            chroma_settings = Settings(
                persist_directory=self.persist_directory,
                anonymized_telemetry=False
            )

            # Создать ChromaDB client
            self.chroma_client = chromadb.Client(chroma_settings)

            # Создать/загрузить Chroma vector store через LangChain
            self.vector_store = Chroma(
                collection_name=self.collection_name,
                embedding_function=self.embeddings_manager.embeddings,
                persist_directory=self.persist_directory,
                client=self.chroma_client
            )

            logger.info(f"✅ ChromaDB initialized: collection='{self.collection_name}'")

        except Exception as e:
            logger.error(f"❌ Failed to initialize ChromaDB: {e}")
            raise

    def add_documents(
            self,
            documents: List[Document],
            ids: Optional[List[str]] = None
    ) -> List[str]:
        """
        Добавить документы в vector store

        Args:
            documents: Список Document объектов для добавления
            ids: Опциональные ID для документов (генерируются автоматически если None)

        Returns:
            Список ID добавленных документов
        """
        try:
            if not documents:
                logger.warning("⚠️ No documents to add")
                return []

            logger.info(f"➕ Adding {len(documents)} documents to vector store...")

            # Добавить документы в Chroma
            doc_ids = self.vector_store.add_documents(
                documents=documents,
                ids=ids
            )

            logger.info(f"✅ Successfully added {len(doc_ids)} documents")
            return doc_ids

        except Exception as e:
            logger.error(f"❌ Error adding documents: {e}")
            raise

    def add_texts(
            self,
            texts: List[str],
            metadatas: Optional[List[Dict[str, Any]]] = None,
            ids: Optional[List[str]] = None
    ) -> List[str]:
        """
        Добавить тексты напрямую (без создания Document объектов)

        Args:
            texts: Список текстов для добавления
            metadatas: Опциональные метаданные для каждого текста
            ids: Опциональные ID

        Returns:
            Список ID добавленных текстов
        """
        try:
            if not texts:
                logger.warning("⚠️ No texts to add")
                return []

            logger.info(f"➕ Adding {len(texts)} texts to vector store...")

            # Добавить тексты в Chroma
            text_ids = self.vector_store.add_texts(
                texts=texts,
                metadatas=metadatas,
                ids=ids
            )

            logger.info(f"✅ Successfully added {len(text_ids)} texts")
            return text_ids

        except Exception as e:
            logger.error(f"❌ Error adding texts: {e}")
            raise

    def similarity_search(
            self,
            query: str,
            k: int = None,
            filter: Optional[Dict[str, Any]] = None
    ) -> List[Document]:
        """
        Поиск наиболее похожих документов

        Args:
            query: Поисковый запрос
            k: Количество результатов (из config по умолчанию)
            filter: Фильтр по метаданным (например, {'file_type': '.pdf'})

        Returns:
            Список наиболее релевантных документов
        """
        try:
            k = k or rag_config.top_k

            logger.info(f"🔍 Searching for top {k} similar documents...")
            logger.debug(f"Query: {query[:100]}...")

            # Выполнить similarity search
            results = self.vector_store.similarity_search(
                query=query,
                k=k,
                filter=filter
            )

            logger.info(f"✅ Found {len(results)} similar documents")
            return results

        except Exception as e:
            logger.error(f"❌ Error during similarity search: {e}")
            raise

    def similarity_search_with_score(
            self,
            query: str,
            k: int = None,
            filter: Optional[Dict[str, Any]] = None
    ) -> List[Tuple[Document, float]]:
        """
        Поиск с возвратом similarity scores

        Args:
            query: Поисковый запрос
            k: Количество результатов
            filter: Фильтр по метаданным

        Returns:
            Список кортежей (Document, similarity_score)
        """
        try:
            k = k or rag_config.top_k

            logger.info(f"🔍 Searching with scores for top {k} documents...")

            # Выполнить поиск с scores
            results = self.vector_store.similarity_search_with_score(
                query=query,
                k=k,
                filter=filter
            )

            # Фильтрация по порогу similarity
            filtered_results = [
                (doc, score) for doc, score in results
                if score >= rag_config.similarity_threshold
            ]

            logger.info(
                f"✅ Found {len(results)} documents, "
                f"{len(filtered_results)} above threshold ({rag_config.similarity_threshold})"
            )

            return filtered_results

        except Exception as e:
            logger.error(f"❌ Error during similarity search with score: {e}")
            raise

    def get_documents_by_ids(self, ids: List[str]) -> List[Document]:
        """
        Получить документы по ID

        Args:
            ids: Список ID документов

        Returns:
            Список Document объектов
        """
        try:
            logger.info(f"📄 Retrieving {len(ids)} documents by IDs...")

            # Получить документы из коллекции
            collection = self.chroma_client.get_collection(self.collection_name)
            results = collection.get(ids=ids, include=['documents', 'metadatas'])

            # Преобразовать в Document объекты
            documents = []
            for doc_text, metadata in zip(results['documents'], results['metadatas']):
                doc = Document(
                    page_content=doc_text,
                    metadata=metadata or {}
                )
                documents.append(doc)

            logger.info(f"✅ Retrieved {len(documents)} documents")
            return documents

        except Exception as e:
            logger.error(f"❌ Error retrieving documents by IDs: {e}")
            raise

    def delete_documents(self, ids: List[str]) -> bool:
        """
        Удалить документы по ID

        Args:
            ids: Список ID документов для удаления

        Returns:
            True если успешно
        """
        try:
            if not ids:
                logger.warning("⚠️ No IDs provided for deletion")
                return False

            logger.info(f"🗑️ Deleting {len(ids)} documents...")

            # Удалить из коллекции
            collection = self.chroma_client.get_collection(self.collection_name)
            collection.delete(ids=ids)

            logger.info(f"✅ Successfully deleted {len(ids)} documents")
            return True

        except Exception as e:
            logger.error(f"❌ Error deleting documents: {e}")
            raise

    def delete_by_filter(self, filter: Dict[str, Any]) -> int:
        """
        Удалить документы по фильтру метаданных

        Args:
            filter: Фильтр для удаления (например, {'file_id': 'xxx'})

        Returns:
            Количество удаленных документов
        """
        try:
            logger.info(f"🗑️ Deleting documents by filter: {filter}")

            # Получить коллекцию
            collection = self.chroma_client.get_collection(self.collection_name)

            # Найти документы по фильтру
            results = collection.get(where=filter, include=['documents'])
            doc_count = len(results['ids'])

            if doc_count > 0:
                # Удалить найденные документы
                collection.delete(ids=results['ids'])
                logger.info(f"✅ Deleted {doc_count} documents")
            else:
                logger.info("ℹ️ No documents found matching filter")

            return doc_count

        except Exception as e:
            logger.error(f"❌ Error deleting by filter: {e}")
            raise

    def clear_collection(self) -> bool:
        """
        Очистить всю коллекцию (удалить все документы)

        Returns:
            True если успешно
        """
        try:
            logger.warning("⚠️ Clearing entire collection...")

            # Удалить коллекцию
            self.chroma_client.delete_collection(self.collection_name)

            # Создать заново
            self.vector_store = Chroma(
                collection_name=self.collection_name,
                embedding_function=self.embeddings_manager.embeddings,
                persist_directory=self.persist_directory,
                client=self.chroma_client
            )

            logger.info(f"✅ Collection '{self.collection_name}' cleared and recreated")
            return True

        except Exception as e:
            logger.error(f"❌ Error clearing collection: {e}")
            raise

    def get_collection_stats(self) -> Dict[str, Any]:
        """
        Получить статистику коллекции

        Returns:
            Словарь со статистикой
        """
        try:
            collection = self.chroma_client.get_collection(self.collection_name)
            count = collection.count()

            stats = {
                'collection_name': self.collection_name,
                'total_documents': count,
                'persist_directory': self.persist_directory
            }

            logger.info(f"📊 Collection stats: {stats}")
            return stats

        except Exception as e:
            logger.error(f"❌ Error getting collection stats: {e}")
            return {
                'collection_name': self.collection_name,
                'total_documents': 0,
                'error': str(e)
            }

    def collection_exists(self) -> bool:
        """
        Проверить существование коллекции

        Returns:
            True если коллекция существует
        """
        try:
            collections = self.chroma_client.list_collections()
            exists = any(c.name == self.collection_name for c in collections)
            logger.debug(f"Collection '{self.collection_name}' exists: {exists}")
            return exists
        except Exception as e:
            logger.error(f"❌ Error checking collection existence: {e}")
            return False

    def as_retriever(self, search_kwargs: Optional[Dict[str, Any]] = None):
        """
        Получить retriever объект для использования в chains

        Args:
            search_kwargs: Параметры поиска (k, filter, etc.)

        Returns:
            VectorStoreRetriever объект
        """
        search_kwargs = search_kwargs or {'k': rag_config.top_k}
        return self.vector_store.as_retriever(search_kwargs=search_kwargs)
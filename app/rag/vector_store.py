# app/rag/vector_store.py
import logging
from typing import List, Dict, Any, Optional
from app.core.config import settings

try:
    from chromadb import PersistentClient
except ImportError:
    PersistentClient = None

logger = logging.getLogger(__name__)


class VectorStoreManager:
    """
    Менеджер векторного хранилища с динамическим управлением размерностями эмбеддингов.

    Автоматически создает и переключается между коллекциями в зависимости от размерности
    входящих векторов. Поддерживает несколько коллекций одновременно.
    """

    def __init__(
            self,
            base_collection_name: str = None,
            persist_directory: str = None
    ):
        """
        Args:
            base_collection_name: Базовое имя для коллекций (без суффикса размерности)
            persist_directory: Директория для хранения данных ChromaDB
        """
        self.base_collection_name = base_collection_name or settings.COLLECTION_NAME
        self.persist_directory = persist_directory or str(settings.get_vectordb_path())

        if not PersistentClient:
            raise ImportError("chromadb library not installed")

        self.client = PersistentClient(path=self.persist_directory)

        # Кеш активных коллекций: {dimension: collection_object}
        self._collections_cache: Dict[int, Any] = {}

        # Текущая активная размерность и коллекция
        self._current_dimension: Optional[int] = None
        self._current_collection: Optional[Any] = None

        logger.info(
            f"✅ VectorStoreManager initialized (dynamic mode)\n"
            f"   Base name: {self.base_collection_name}\n"
            f"   Directory: {self.persist_directory}"
        )

    def _get_collection_name(self, dimension: int) -> str:
        """
        Генерирует имя коллекции на основе размерности.

        Args:
            dimension: Размерность эмбеддингов

        Returns:
            Имя коллекции вида "base_name_<dimension>d"
        """
        return f"{self.base_collection_name}_{dimension}d"

    def _get_or_create_collection(self, dimension: int):
        """
        Получает или создает коллекцию для указанной размерности.
        Использует кеширование для оптимизации.

        Args:
            dimension: Размерность эмбеддингов

        Returns:
            Объект коллекции ChromaDB
        """
        # Проверяем кеш
        if dimension in self._collections_cache:
            logger.debug(f"📦 Using cached collection for dimension {dimension}")
            return self._collections_cache[dimension]

        # Создаем/получаем коллекцию
        collection_name = self._get_collection_name(dimension)
        try:
            collection = self.client.get_or_create_collection(collection_name)
            self._collections_cache[dimension] = collection

            logger.info(
                f"📦 Collection initialized: {collection_name}\n"
                f"   Dimension: {dimension}\n"
                f"   Document count: {collection.count()}"
            )
            return collection

        except Exception as e:
            logger.error(f"❌ Failed to create collection {collection_name}: {e}")
            raise

    def _ensure_collection(self, embedding: List[float]):
        """
        Автоматически определяет размерность и переключается на нужную коллекцию.

        Args:
            embedding: Вектор эмбеддинга для определения размерности
        """
        dimension = len(embedding)

        # Если размерность изменилась или коллекция не инициализирована
        if self._current_dimension != dimension:
            old_dimension = self._current_dimension

            # Получаем/создаем нужную коллекцию
            self._current_collection = self._get_or_create_collection(dimension)
            self._current_dimension = dimension

            if old_dimension is not None and old_dimension != dimension:
                logger.warning(
                    f"🔄 Dimension changed: {old_dimension} → {dimension}\n"
                    f"   Switched to: {self._get_collection_name(dimension)}"
                )
            else:
                logger.info(f"✅ Active collection: {self._get_collection_name(dimension)} (dim: {dimension})")

    def add_document(
            self,
            doc_id: str,
            embedding: List[float],
            metadata: Dict[str, Any]
    ):
        """
        Добавляет документ в векторное хранилище.
        Автоматически выбирает коллекцию по размерности эмбеддинга.

        Args:
            doc_id: Уникальный идентификатор документа
            embedding: Вектор эмбеддинга
            metadata: Метаданные документа
        """
        # Автоматически переключаемся на нужную коллекцию
        self._ensure_collection(embedding)

        content = metadata.get('content', '')
        dimension = len(embedding)

        logger.info(
            f"📄 Adding document: {doc_id[:50]}...\n"
            f"   Collection: {self._get_collection_name(dimension)}\n"
            f"   Dimension: {dimension}\n"
            f"   Content size: {len(content)} chars"
        )

        logger.debug(
            f"   Metadata: conversation_id={metadata.get('conversation_id')}, "
            f"user_id={metadata.get('user_id')}, "
            f"file_id={metadata.get('file_id')}"
        )

        try:
            self._current_collection.add(
                ids=[doc_id],
                embeddings=[embedding],
                metadatas=[metadata],
                documents=[content]
            )

            # Проверка сохранения
            saved = self._current_collection.get(ids=[doc_id])
            if saved and saved.get('metadatas'):
                saved_metadata = saved['metadatas'][0]
                logger.info(
                    f"✅ Document saved successfully\n"
                    f"   ID: {doc_id[:50]}...\n"
                    f"   conversation_id: {saved_metadata.get('conversation_id')}"
                )
            else:
                logger.warning(f"⚠️ Could not verify document save: {doc_id}")

        except Exception as e:
            logger.error(f"❌ Failed to add document {doc_id}: {e}")
            raise

    def query(
            self,
            embedding_query: List[float],
            top_k: int = 5,
            filter_dict: Optional[Dict[str, Any]] = None,
            search_all_dimensions: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Ищет похожие документы в векторном хранилище.

        Args:
            embedding_query: Вектор запроса
            top_k: Количество результатов
            filter_dict: Фильтры для поиска (формат ChromaDB where)
            search_all_dimensions: Если True, ищет во всех коллекциях (медленнее)

        Returns:
            Список найденных документов с метаданными и расстояниями
        """
        dimension = len(embedding_query)

        if search_all_dimensions:
            # Поиск во всех коллекциях
            return self._query_all_dimensions(embedding_query, top_k, filter_dict)

        # Поиск только в коллекции с нужной размерностью
        self._ensure_collection(embedding_query)

        logger.info(
            f"🔍 Querying collection: {self._get_collection_name(dimension)}\n"
            f"   Top-K: {top_k}\n"
            f"   Dimension: {dimension}\n"
            f"   Filter: {filter_dict if filter_dict else 'None'}"
        )

        try:
            query_params = {
                "query_embeddings": [embedding_query],
                "n_results": top_k
            }

            if filter_dict:
                query_params["where"] = filter_dict

            results = self._current_collection.query(**query_params)
            parsed = self._parse_results(results)

            logger.info(f"✅ Found {len(parsed)} documents")
            return parsed

        except Exception as e:
            logger.error(f"❌ Query failed: {e}")
            return []

    def _query_all_dimensions(
            self,
            embedding_query: List[float],
            top_k: int,
            filter_dict: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Ищет во всех коллекциях разных размерностей (fallback режим).

        Args:
            embedding_query: Вектор запроса
            top_k: Количество результатов
            filter_dict: Фильтры для поиска

        Returns:
            Объединенный список результатов из всех коллекций
        """
        logger.info("🔍 Multi-dimension search across all collections")

        all_results = []
        collections = self.client.list_collections()

        for collection_obj in collections:
            # Пропускаем коллекции не нашего базового имени
            if not collection_obj.name.startswith(self.base_collection_name):
                continue

            try:
                # Извлекаем размерность из имени
                parts = collection_obj.name.split('_')
                if not parts[-1].endswith('d'):
                    continue

                coll_dimension = int(parts[-1][:-1])

                # Если размерности совпадают, делаем обычный поиск
                if coll_dimension == len(embedding_query):
                    logger.debug(f"   Searching in {collection_obj.name}")
                    query_params = {
                        "query_embeddings": [embedding_query],
                        "n_results": top_k
                    }
                    if filter_dict:
                        query_params["where"] = filter_dict

                    results = collection_obj.query(**query_params)
                    parsed = self._parse_results(results)

                    for result in parsed:
                        result['source_collection'] = collection_obj.name
                        result['dimension'] = coll_dimension

                    all_results.extend(parsed)

            except Exception as e:
                logger.warning(f"⚠️ Failed to search in {collection_obj.name}: {e}")
                continue

        # Сортируем по distance и берем top_k
        all_results.sort(key=lambda x: x.get('distance', float('inf')))
        final_results = all_results[:top_k]

        logger.info(f"✅ Multi-dimension search complete: {len(final_results)} results")
        return final_results

    def _parse_results(self, results: Dict) -> List[Dict[str, Any]]:
        """
        Парсит результаты ChromaDB в удобный формат.

        Args:
            results: Результаты от ChromaDB

        Returns:
            Список словарей с parsed результатами
        """
        parsed_results = []

        if not results or 'ids' not in results or not results['ids']:
            return parsed_results

        ids = results['ids'][0]
        metadatas = results.get('metadatas', [[]])[0]
        documents = results.get('documents', [[]])[0]
        distances = results.get('distances', [[]])[0]

        for i, doc_id in enumerate(ids):
            content = documents[i] if i < len(documents) else ''

            # Fallback на content из metadata
            if not content and i < len(metadatas):
                content = metadatas[i].get('content', '')

            current_metadata = metadatas[i] if i < len(metadatas) else {}
            distance = distances[i] if i < len(distances) else 0.0

            logger.debug(
                f"   Result {i + 1}: id={doc_id[:30]}..., "
                f"conv_id={current_metadata.get('conversation_id')}, "
                f"distance={distance:.4f}"
            )

            parsed_results.append({
                'id': doc_id,
                'metadata': current_metadata,
                'content': content,
                'distance': distance
            })

        return parsed_results

    def clear_collection(self, dimension: Optional[int] = None):
        """
        Очищает коллекцию (удаляет все документы).

        Args:
            dimension: Если указана, очищает только коллекцию этой размерности.
                      Если None, очищает текущую активную коллекцию.
        """
        if dimension:
            collection_name = self._get_collection_name(dimension)
            try:
                collection = self.client.get_collection(collection_name)
                all_docs = collection.get()
                if all_docs and all_docs.get('ids'):
                    collection.delete(ids=all_docs['ids'])
                    logger.info(f"✅ Cleared {len(all_docs['ids'])} documents from {collection_name}")
                else:
                    logger.info(f"Collection {collection_name} is already empty")
            except Exception as e:
                logger.warning(f"⚠️ Could not clear collection {collection_name}: {e}")
        else:
            if not self._current_collection:
                logger.warning("⚠️ No active collection to clear")
                return

            collection_name = self._get_collection_name(self._current_dimension)
            logger.info(f"🗑️ Clearing collection {collection_name}")

            try:
                all_docs = self._current_collection.get()
                if all_docs and all_docs.get('ids'):
                    self._current_collection.delete(ids=all_docs['ids'])
                    logger.info(f"✅ Deleted {len(all_docs['ids'])} documents")
                else:
                    logger.info("Collection is already empty")
            except Exception as e:
                logger.error(f"❌ Error clearing collection: {e}")

    def clear_all_collections(self):
        """Очищает все коллекции этого базового имени."""
        logger.info(f"🗑️ Clearing all collections with base name: {self.base_collection_name}")

        collections = self.client.list_collections()
        cleared_count = 0

        for collection_obj in collections:
            if collection_obj.name.startswith(self.base_collection_name):
                try:
                    all_docs = collection_obj.get()
                    if all_docs and all_docs.get('ids'):
                        collection_obj.delete(ids=all_docs['ids'])
                        cleared_count += len(all_docs['ids'])
                        logger.info(f"   ✅ Cleared {collection_obj.name}: {len(all_docs['ids'])} docs")
                except Exception as e:
                    logger.warning(f"   ⚠️ Failed to clear {collection_obj.name}: {e}")

        logger.info(f"✅ Cleared {cleared_count} documents total")

    def delete_collection(self, dimension: Optional[int] = None):
        """
        Удаляет коллекцию полностью.

        Args:
            dimension: Если указана, удаляет коллекцию этой размерности.
                      Если None, удаляет текущую активную коллекцию.
        """
        if dimension:
            collection_name = self._get_collection_name(dimension)
            try:
                self.client.delete_collection(collection_name)
                # Удаляем из кеша
                if dimension in self._collections_cache:
                    del self._collections_cache[dimension]
                logger.info(f"✅ Deleted collection: {collection_name}")
            except Exception as e:
                logger.warning(f"⚠️ Could not delete collection {collection_name}: {e}")
        else:
            if not self._current_dimension:
                logger.warning("⚠️ No active collection to delete")
                return

            collection_name = self._get_collection_name(self._current_dimension)
            try:
                self.client.delete_collection(collection_name)
                # Удаляем из кеша
                if self._current_dimension in self._collections_cache:
                    del self._collections_cache[self._current_dimension]
                # Сбрасываем текущую коллекцию
                self._current_collection = None
                self._current_dimension = None
                logger.info(f"✅ Deleted collection: {collection_name}")
            except Exception as e:
                logger.error(f"❌ Error deleting collection: {e}")

    def list_all_collections(self) -> List[Dict[str, Any]]:
        """
        Возвращает информацию о всех коллекциях.

        Returns:
            Список словарей с информацией о коллекциях
        """
        collections = self.client.list_collections()
        result = []

        for collection_obj in collections:
            if collection_obj.name.startswith(self.base_collection_name):
                try:
                    # Извлекаем размерность
                    parts = collection_obj.name.split('_')
                    dimension = None
                    if parts[-1].endswith('d'):
                        try:
                            dimension = int(parts[-1][:-1])
                        except ValueError:
                            pass

                    doc_count = collection_obj.count()

                    result.append({
                        'name': collection_obj.name,
                        'dimension': dimension,
                        'document_count': doc_count,
                        'is_active': dimension == self._current_dimension
                    })
                except Exception as e:
                    logger.warning(f"⚠️ Error getting info for {collection_obj.name}: {e}")

        logger.info(f"📚 Found {len(result)} collections")
        return result

    def get_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику по всем коллекциям.

        Returns:
            Словарь со статистикой
        """
        collections_info = self.list_all_collections()

        total_docs = sum(c['document_count'] for c in collections_info)
        dimensions = [c['dimension'] for c in collections_info if c['dimension']]

        stats = {
            'base_name': self.base_collection_name,
            'total_collections': len(collections_info),
            'total_documents': total_docs,
            'available_dimensions': sorted(dimensions),
            'current_dimension': self._current_dimension,
            'collections': collections_info
        }

        logger.info(
            f"📊 Stats:\n"
            f"   Collections: {stats['total_collections']}\n"
            f"   Documents: {stats['total_documents']}\n"
            f"   Dimensions: {stats['available_dimensions']}\n"
            f"   Current: {stats['current_dimension']}"
        )

        return stats


# Singleton instance
vectorstore_manager = VectorStoreManager()

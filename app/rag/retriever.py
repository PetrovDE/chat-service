# app/rag/retriever.py
import logging
from typing import List, Dict, Any, Optional, AsyncGenerator
from langchain_core.documents import Document
from app.rag.vector_store import VectorStoreManager
from app.rag.document_loader import DocumentLoader
from app.rag.text_splitter import SmartTextSplitter
from app.rag.config import rag_config
from app.llm_manager import llm_manager

logger = logging.getLogger(__name__)


class RAGRetriever:
    """
    RAG (Retrieval-Augmented Generation) Retriever
    Объединяет поиск релевантных документов с генерацией ответов LLM
    """

    def __init__(
            self,
            vector_store: VectorStoreManager = None,
            document_loader: DocumentLoader = None,
            text_splitter: SmartTextSplitter = None
    ):
        """
        Инициализация RAG retriever

        Args:
            vector_store: Менеджер векторной БД
            document_loader: Загрузчик документов
            text_splitter: Text splitter
        """
        self.vector_store = vector_store or VectorStoreManager()
        self.document_loader = document_loader or DocumentLoader()
        self.text_splitter = text_splitter or SmartTextSplitter()

        logger.info("✅ RAGRetriever initialized")

    async def process_and_store_file(
            self,
            file_path: str,
            metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Обработать файл и сохранить в vector store

        Args:
            file_path: Путь к файлу
            metadata: Дополнительные метаданные

        Returns:
            Словарь с результатами обработки
        """
        try:
            logger.info(f"📂 Processing file for RAG: {file_path}")

            # 1. Загрузить документ
            documents = self.document_loader.load_file(file_path, metadata)

            if not documents:
                raise ValueError("No documents loaded from file")

            # 2. Разбить на chunks
            chunks = self.text_splitter.split_documents(documents)

            if not chunks:
                raise ValueError("No chunks created from documents")

            # 3. Добавить в vector store
            chunk_ids = self.vector_store.add_documents(chunks)

            # 4. Получить статистику
            stats = self.text_splitter.get_chunk_stats(chunks)

            result = {
                'success': True,
                'documents_count': len(documents),
                'chunks_count': len(chunks),
                'chunk_ids': chunk_ids,
                'stats': stats
            }

            logger.info(
                f"✅ File processed: {len(documents)} docs → {len(chunks)} chunks"
            )
            return result

        except Exception as e:
            logger.error(f"❌ Error processing file: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    async def process_and_store_db_file(self, db_file) -> Dict[str, Any]:
        """
        Обработать файл из БД и сохранить в vector store

        Args:
            db_file: Объект File из БД

        Returns:
            Словарь с результатами
        """
        try:
            logger.info(f"📂 Processing DB file: {db_file.original_filename}")

            # 1. Загрузить из БД
            documents = self.document_loader.load_from_db_file(db_file)

            if not documents:
                raise ValueError("No documents loaded from DB file")

            # 2. Разбить на chunks с учетом типа файла
            file_type = db_file.file_type.replace('/', '').replace('application', '')

            # Если file_type содержит расширение, используем его
            if '.' in db_file.original_filename:
                ext = '.' + db_file.original_filename.split('.')[-1]
                chunks = self.text_splitter.split_by_file_type(
                    documents[0].page_content,
                    ext,
                    metadata=documents[0].metadata
                )
            else:
                chunks = self.text_splitter.split_documents(documents)

            # 3. Добавить file_id в метаданные для последующего удаления
            for chunk in chunks:
                chunk.metadata['file_id'] = str(db_file.id)

            # 4. Добавить в vector store
            chunk_ids = self.vector_store.add_documents(chunks)

            # 5. Статистика
            stats = self.text_splitter.get_chunk_stats(chunks)

            result = {
                'success': True,
                'file_id': str(db_file.id),
                'filename': db_file.original_filename,
                'documents_count': len(documents),
                'chunks_count': len(chunks),
                'chunk_ids': chunk_ids,
                'stats': stats
            }

            logger.info(
                f"✅ DB file processed: {db_file.original_filename} → {len(chunks)} chunks"
            )
            return result

        except Exception as e:
            logger.error(f"❌ Error processing DB file: {e}")
            return {
                'success': False,
                'file_id': str(db_file.id) if db_file else None,
                'error': str(e)
            }

    def retrieve_context(
            self,
            query: str,
            k: int = None,
            filter: Optional[Dict[str, Any]] = None
    ) -> List[Document]:
        """
        Получить релевантный контекст для запроса

        Args:
            query: Поисковый запрос
            k: Количество документов
            filter: Фильтр по метаданным

        Returns:
            Список релевантных документов
        """
        try:
            k = k or rag_config.top_k

            logger.info(f"🔍 Retrieving context for query: {query[:50]}...")

            # Поиск релевантных документов
            results = self.vector_store.similarity_search_with_score(
                query=query,
                k=k,
                filter=filter
            )

            # Извлечь только документы (без scores)
            documents = [doc for doc, score in results]

            logger.info(f"✅ Retrieved {len(documents)} relevant documents")
            return documents

        except Exception as e:
            logger.error(f"❌ Error retrieving context: {e}")
            return []

    def build_context_prompt(
            self,
            query: str,
            context_documents: List[Document],
            include_metadata: bool = None
    ) -> str:
        """
        Построить промпт с контекстом из документов

        Args:
            query: Запрос пользователя
            context_documents: Релевантные документы
            include_metadata: Включать метаданные

        Returns:
            Промпт для LLM
        """
        include_metadata = include_metadata if include_metadata is not None else rag_config.include_metadata

        if not context_documents:
            # Нет контекста - обычный запрос
            return query

        # Собрать контекст
        context_parts = []

        for idx, doc in enumerate(context_documents, 1):
            # Текст документа
            content = doc.page_content

            # Сократить если слишком длинный
            max_length = rag_config.max_context_length // len(context_documents)
            if len(content) > max_length:
                content = content[:max_length] + "..."

            # Добавить метаданные если нужно
            if include_metadata and doc.metadata:
                source = doc.metadata.get('file_name', doc.metadata.get('source', 'Unknown'))
                context_parts.append(f"[Документ {idx} из {source}]\n{content}")
            else:
                context_parts.append(f"[Документ {idx}]\n{content}")

        # Объединить контекст
        context = "\n\n---\n\n".join(context_parts)

        # Построить финальный промпт
        prompt = f"""На основе следующих документов ответь на вопрос пользователя.

ДОКУМЕНТЫ:
{context}

---

ВОПРОС: {query}

ИНСТРУКЦИЯ: Используй информацию из документов выше для ответа. Если в документах нет информации для ответа на вопрос, так и скажи. Отвечай подробно и структурированно."""

        return prompt

    async def generate_answer(
            self,
            query: str,
            filter: Optional[Dict[str, Any]] = None,
            temperature: float = 0.7,
            max_tokens: int = 2000
    ) -> Dict[str, Any]:
        """
        Сгенерировать ответ используя RAG

        Args:
            query: Запрос пользователя
            filter: Фильтр документов по метаданным
            temperature: Температура генерации
            max_tokens: Максимум токенов

        Returns:
            Словарь с ответом и метаданными
        """
        try:
            logger.info(f"🤖 Generating RAG answer for: {query[:50]}...")

            # 1. Получить релевантный контекст
            context_docs = self.retrieve_context(query, filter=filter)

            # 2. Построить промпт
            prompt = self.build_context_prompt(query, context_docs)

            # 3. Сгенерировать ответ через LLM Manager
            result = await llm_manager.generate_response(
                prompt=prompt,
                temperature=temperature,
                max_tokens=max_tokens
            )

            # 4. Добавить информацию о контексте
            result['rag_context'] = {
                'documents_used': len(context_docs),
                'sources': [
                    doc.metadata.get('file_name', doc.metadata.get('source', 'Unknown'))
                    for doc in context_docs
                ]
            }

            logger.info(
                f"✅ RAG answer generated using {len(context_docs)} documents"
            )
            return result

        except Exception as e:
            logger.error(f"❌ Error generating RAG answer: {e}")
            raise

    async def generate_answer_stream(
            self,
            query: str,
            filter: Optional[Dict[str, Any]] = None,
            temperature: float = 0.7,
            max_tokens: int = 2000
    ) -> AsyncGenerator[str, None]:
        """
        Сгенерировать ответ с streaming используя RAG

        Args:
            query: Запрос пользователя
            filter: Фильтр документов
            temperature: Температура
            max_tokens: Максимум токенов

        Yields:
            Chunks ответа
        """
        try:
            logger.info(f"🤖 Generating streaming RAG answer for: {query[:50]}...")

            # 1. Получить контекст
            context_docs = self.retrieve_context(query, filter=filter)

            # 2. Построить промпт
            prompt = self.build_context_prompt(query, context_docs)

            # 3. Streaming генерация через LLM Manager
            async for chunk in llm_manager.generate_response_stream(
                    prompt=prompt,
                    temperature=temperature,
                    max_tokens=max_tokens
            ):
                yield chunk

            logger.info(
                f"✅ RAG streaming completed using {len(context_docs)} documents"
            )

        except Exception as e:
            logger.error(f"❌ Error in RAG streaming: {e}")
            raise

    async def analyze_file(
            self,
            file_id: str,
            query: Optional[str] = None
    ) -> str:
        """
        Анализировать файл с помощью RAG

        Args:
            file_id: ID файла в векторной БД
            query: Опциональный запрос (если None - общий анализ)

        Returns:
            Анализ файла
        """
        try:
            logger.info(f"📊 Analyzing file: {file_id}")

            # Фильтр по file_id
            filter_dict = {'file_id': file_id}

            # Запрос по умолчанию
            if not query:
                query = """Проанализируй этот документ и предоставь:
1. Краткое описание содержимого
2. Основные темы и ключевые моменты
3. Структуру документа (если применимо)
4. Полезные выводы или инсайты"""

            # Получить контекст из файла
            context_docs = self.retrieve_context(query, filter=filter_dict)

            if not context_docs:
                return "Файл не найден в векторной базе или не содержит текстового контента."

            # Построить промпт для анализа
            prompt = self.build_context_prompt(query, context_docs)

            # Генерация анализа
            result = await llm_manager.generate_response(
                prompt=prompt,
                temperature=0.3,  # Низкая температура для точного анализа
                max_tokens=2000
            )

            logger.info(f"✅ File analysis completed")
            return result['response']

        except Exception as e:
            logger.error(f"❌ Error analyzing file: {e}")
            raise

    def remove_file_from_store(self, file_id: str) -> int:
        """
        Удалить все chunks файла из vector store

        Args:
            file_id: ID файла

        Returns:
            Количество удаленных chunks
        """
        try:
            logger.info(f"🗑️ Removing file from vector store: {file_id}")

            count = self.vector_store.delete_by_filter({'file_id': file_id})

            logger.info(f"✅ Removed {count} chunks for file {file_id}")
            return count

        except Exception as e:
            logger.error(f"❌ Error removing file: {e}")
            raise

    def get_stats(self) -> Dict[str, Any]:
        """
        Получить статистику RAG системы

        Returns:
            Словарь со статистикой
        """
        try:
            vector_stats = self.vector_store.get_collection_stats()

            stats = {
                'vector_store': vector_stats,
                'config': {
                    'chunk_size': rag_config.chunk_size,
                    'chunk_overlap': rag_config.chunk_overlap,
                    'top_k': rag_config.top_k,
                    'similarity_threshold': rag_config.similarity_threshold,
                    'embeddings_model': rag_config.embeddings_model
                }
            }

            logger.info(f"📊 RAG stats: {stats}")
            return stats

        except Exception as e:
            logger.error(f"❌ Error getting stats: {e}")
            return {'error': str(e)}


# Глобальный экземпляр RAG retriever
rag_retriever = RAGRetriever()
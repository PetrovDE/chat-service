# app/rag/retriever.py
import logging
from typing import List, Optional, Dict, Any
from langchain_core.documents import Document

from app.rag.vector_store import VectorStoreManager
from app.rag.document_loader import DocumentLoader
from app.rag.text_splitter import SmartTextSplitter
from app.rag.embeddings import embeddings_manager

logger = logging.getLogger(__name__)


class RAGRetriever:
    def __init__(
            self,
            vectorstore: Optional[VectorStoreManager] = None,
            documentloader: Optional[DocumentLoader] = None,
            textsplitter: Optional[SmartTextSplitter] = None
    ):
        self.vectorstore = vectorstore or VectorStoreManager()
        self.documentloader = documentloader or DocumentLoader()
        self.textsplitter = textsplitter or SmartTextSplitter()
        logger.info("✅ RAGRetriever initialized")

    async def process_and_store_file(
            self,
            filepath: str,
            metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Обрабатывает и сохраняет файл в векторное хранилище
        """
        logger.info(f"📂 Processing file: {filepath}")

        # Загружаем документ
        docs = await self.documentloader.load_file(filepath, metadata)
        logger.info(f"📄 Loaded {len(docs)} documents from file")

        # Разбиваем на чанки
        chunk_docs = self.textsplitter.split_documents(docs)
        logger.info(f"✂️ Split into {len(chunk_docs)} chunks")

        # Получаем текущий режим и модель для логирования
        current_mode = embeddings_manager.mode
        current_model = embeddings_manager.model
        expected_dim = embeddings_manager.get_embedding_dimension()

        logger.info(
            f"🔮 Generating embeddings: mode={embeddings_manager.original_mode}, "
            f"model={'arctic' if current_mode == 'aihub' else current_model}, "
            f"expected_dim={expected_dim}"
        )

        # Генерируем эмбеддинги
        embeddings = await embeddings_manager.embedd_documents_async([doc.page_content for doc in chunk_docs])

        # Сохраняем информацию о модели в метаданных для каждого чанка
        for doc, emb in zip(chunk_docs, embeddings):
            chunk_id = doc.metadata.get("id", None) or doc.page_content[:40]

            # ✅ ВАЖНО: Сохраняем информацию о модели и размерности
            doc.metadata['embedding_model'] = 'arctic' if current_mode == 'aihub' else current_model
            doc.metadata['embedding_mode'] = embeddings_manager.original_mode
            doc.metadata['embedding_dimension'] = len(emb)

            self.vectorstore.add_document(
                doc_id=chunk_id,
                embedding=emb,
                metadata=doc.metadata
            )

        logger.info(
            f"✅ Stored {len(chunk_docs)} chunks with {len(embeddings[0]) if embeddings else 0}d embeddings"
        )

        return {
            "count_stored_chunks": len(chunk_docs),
            "embedding_dimension": len(embeddings[0]) if embeddings else 0,
            "embedding_model": 'arctic' if current_mode == 'aihub' else current_model,
            "embedding_mode": embeddings_manager.original_mode
        }

    async def query_rag(
            self,
            query_content: str,
            top_k: int = 5,
            user_id: Optional[str] = None,
            conversation_id: Optional[str] = None,
            model_source: Optional[str] = None  # ✅ НОВЫЙ ПАРАМЕТР
    ) -> List[Document]:
        """
        Поиск релевантных документов через RAG

        Args:
            query_content: Текст запроса
            top_k: Количество результатов
            user_id: ID пользователя для фильтрации
            conversation_id: ID диалога для фильтрации
            model_source: Источник модели (для использования правильной размерности эмбеддингов)
        """
        # ✅ КРИТИЧЕСКИ ВАЖНО: Сохраняем текущий режим
        original_mode = embeddings_manager.original_mode
        original_model = embeddings_manager.model

        try:
            # ✅ ИСПРАВЛЕНИЕ: Переключаемся на режим, который используется для файлов
            if model_source:
                if model_source in ['corporate', 'aihub']:
                    # Для AI HUB всегда используем arctic
                    embeddings_manager.switch_mode('corporate')
                    logger.info("🔄 Switched to AI HUB mode (arctic) for query embedding")
                else:
                    embeddings_manager.switch_mode(model_source)
                    logger.info(f"🔄 Switched to {model_source} mode for query embedding")

            # Генерируем embedding запроса в правильном режиме
            expected_dim = embeddings_manager.get_embedding_dimension()
            logger.info(
                f"🔮 Generating query embedding: mode={embeddings_manager.original_mode}, "
                f"expected_dim={expected_dim}"
            )

            embedding_query = (await embeddings_manager.embedd_documents_async([query_content]))[0]
            actual_dim = len(embedding_query)

            logger.info(
                f"✅ Query embedding generated: {actual_dim}d "
                f"(expected: {expected_dim}d)"
            )

            if actual_dim != expected_dim:
                logger.warning(
                    f"⚠️ Dimension mismatch: expected {expected_dim}, got {actual_dim}"
                )

            # Выполняем поиск
            results = self.vectorstore.query(embedding_query, top_k=top_k * 2)  # Берем больше для фильтрации

            logger.info(f"🔍 Vector store returned {len(results)} raw results")

            # ✅ КРИТИЧНО: Фильтруем по conversation_id (ГЛАВНОЕ!)
            if conversation_id:
                filtered_results = [
                    r for r in results
                    if r.get('metadata', {}).get('conversation_id') == conversation_id
                ]
                logger.info(
                    f"🔍 After conversation_id filter ({conversation_id}): "
                    f"{len(filtered_results)} results"
                )
                results = filtered_results

            # Дополнительная фильтрация по user_id (для безопасности)
            if user_id:
                filtered_results = [
                    r for r in results
                    if r.get('metadata', {}).get('user_id') == user_id
                ]
                logger.info(f"🔍 After user_id filter: {len(filtered_results)} results")
                results = filtered_results

            # Ограничиваем до top_k
            results = results[:top_k]

            # Преобразуем результаты в Document
            documents = []
            for idx, result in enumerate(results):
                # Извлечение content
                content = None

                if 'content' in result and result['content']:
                    content = result['content']
                elif 'metadata' in result and 'content' in result['metadata']:
                    content = result['metadata']['content']
                elif 'metadata' in result and 'page_content' in result['metadata']:
                    content = result['metadata']['page_content']
                elif 'metadata' in result and 'text' in result['metadata']:
                    content = result['metadata']['text']

                if not content or not str(content).strip():
                    logger.warning(
                        f"⚠️ Empty content for result {idx}, id={result.get('id')}, "
                        f"metadata keys: {list(result.get('metadata', {}).keys())}"
                    )
                    continue

                metadata = result.get('metadata', {}).copy()
                metadata['result_index'] = idx
                metadata['similarity_score'] = result.get('distance', 0)

                doc = Document(
                    page_content=str(content),
                    metadata=metadata
                )
                documents.append(doc)

                logger.debug(
                    f"✅ Document {idx}: {len(content)} chars, "
                    f"file={metadata.get('filename', 'unknown')}, "
                    f"conv_id={metadata.get('conversation_id', 'none')}, "
                    f"embedding_dim={metadata.get('embedding_dimension', 'unknown')}"
                )

            logger.info(f"✅ Returning {len(documents)} valid documents for RAG")
            if not documents:
                logger.warning("⚠️ No valid documents found - RAG context will be empty!")

            return documents

        finally:
            # ✅ КРИТИЧНО: Восстанавливаем исходный режим
            embeddings_manager.switch_mode(original_mode)
            if original_model:
                embeddings_manager.switch_model(original_model)
            logger.info(f"🔄 Restored original mode: {original_mode}")

    def build_context_prompt(self, query: str, context_documents: List[Document]) -> str:
        """
        Строит промпт с контекстом из документов
        """
        if not context_documents:
            logger.warning("⚠️ No context documents provided - using query only")
            return query

        context_chunks = []
        for i, doc in enumerate(context_documents, 1):
            content = doc.page_content
            if not content or not content.strip():
                logger.warning(f"⚠️ Skipping document {i} - empty content")
                continue

            filename = doc.metadata.get('filename', 'Unknown')
            file_type = doc.metadata.get('file_type', '')
            chunk_index = doc.metadata.get('chunk_index', '')

            # Формируем заголовок документа
            doc_header = f"[Документ {i} - {filename}"
            if file_type:
                doc_header += f" ({file_type})"
            if chunk_index is not None and chunk_index != '':
                doc_header += f" - часть {chunk_index + 1}"
            doc_header += "]"

            # Ограничиваем длину контента
            max_content_length = 2000
            if len(content) > max_content_length:
                content = content[:max_content_length] + "\n[... содержимое обрезано ...]"

            context_chunks.append(f"{doc_header}\n{content}")

        if not context_chunks:
            logger.warning("⚠️ All documents were empty - using query only")
            return query

        context_text = "\n\n---\n\n".join(context_chunks)

        prompt = f"""Используя следующий контекст из загруженных файлов, ответь на вопрос пользователя.
Если в контексте нет релевантной информации, скажи об этом честно.

КОНТЕКСТ ({len(context_documents)} документов):
{context_text}

ВОПРОС:
{query}

ОТВЕТ:"""

        logger.info(
            f"📝 Built prompt: {len(context_documents)} docs, "
            f"{len(context_text)} context chars, "
            f"{len(prompt)} total chars"
        )

        return prompt


rag_retriever = RAGRetriever()

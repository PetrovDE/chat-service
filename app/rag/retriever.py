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
    def __init__(self,
                 vectorstore: Optional[VectorStoreManager] = None,
                 documentloader: Optional[DocumentLoader] = None,
                 textsplitter: Optional[SmartTextSplitter] = None):
        self.vectorstore = vectorstore or VectorStoreManager()
        self.documentloader = documentloader or DocumentLoader()
        self.textsplitter = textsplitter or SmartTextSplitter()
        logger.info("RAGRetriever initialized")

    async def process_and_store_file(self, filepath: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        docs = await self.documentloader.load_file(filepath, metadata)
        chunk_docs = self.textsplitter.split_documents(docs)
        embeddings = embeddings_manager.embedd_documents([doc.page_content for doc in chunk_docs])
        for doc, emb in zip(chunk_docs, embeddings):
            chunk_id = doc.metadata.get("id", None) or doc.page_content[:40]
            self.vectorstore.add_document(doc_id=chunk_id, embedding=emb, metadata=doc.metadata)
        return {"count_stored_chunks": len(chunk_docs)}

    def query_rag(self, query_content: str, top_k: int = 5, user_id: Optional[str] = None) -> List[Document]:
        """
        ИСПРАВЛЕНО: Улучшенный поиск с правильной обработкой content
        """
        embedding_query = embeddings_manager.embedd_documents([query_content])[0]
        results = self.vectorstore.query(embedding_query, top_k=top_k)
        logger.info(f"🔍 Vector store returned {len(results)} raw results")

        if user_id:
            filtered_results = [r for r in results if r.get('metadata', {}).get('user_id') == user_id]
            logger.info(f"🔍 After user_id filter: {len(filtered_results)} results")
            results = filtered_results

        documents = []
        for idx, result in enumerate(results):
            # ИСПРАВЛЕНИЕ 6: Улучшенная логика извлечения content
            # Проверяем несколько возможных источников контента
            content = None

            # Источник 1: прямое поле content
            if 'content' in result and result['content']:
                content = result['content']

            # Источник 2: content в metadata
            elif 'metadata' in result and 'content' in result['metadata']:
                content = result['metadata']['content']

            # Источник 3: page_content в metadata
            elif 'metadata' in result and 'page_content' in result['metadata']:
                content = result['metadata']['page_content']

            # Источник 4: text в metadata
            elif 'metadata' in result and 'text' in result['metadata']:
                content = result['metadata']['text']

            if not content or not str(content).strip():
                logger.warning(
                    f"⚠️ Empty content for result {idx}, id={result.get('id')}, "
                    f"metadata keys: {list(result.get('metadata', {}).keys())}"
                )
                continue

            # Создаем metadata без дублирования content
            metadata = result.get('metadata', {}).copy()

            # Добавляем полезную информацию для отладки
            metadata['result_index'] = idx
            metadata['similarity_score'] = result.get('distance', 0)

            doc = Document(
                page_content=str(content),
                metadata=metadata
            )
            documents.append(doc)

            logger.debug(
                f"✅ Document {idx}: {len(content)} chars, "
                f"file={metadata.get('filename', 'unknown')}"
            )

        logger.info(f"✅ Returning {len(documents)} valid documents for RAG")

        if not documents:
            logger.warning("⚠️ No valid documents found - RAG context will be empty!")

        return documents

    def build_context_prompt(self, query: str, context_documents: List[Document]) -> str:
        """
        ИСПРАВЛЕНО: Улучшенное построение промпта с проверками
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

            # ИСПРАВЛЕНИЕ 7: Ограничиваем длину контента для промпта
            # чтобы не превысить лимиты контекста модели
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

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
            self.vectorstore.add_document(
                doc_id=chunk_id,
                embedding=emb,
                metadata=doc.metadata
            )
        return {"count_stored_chunks": len(chunk_docs)}

    def query_rag(self, query_content: str, top_k: int = 5, user_id: Optional[str] = None) -> List[Document]:
        embedding_query = embeddings_manager.embedd_documents([query_content])[0]

        # Получить результаты из векторного хранилища
        results = self.vectorstore.query(embedding_query, top_k=top_k)

        # Фильтрация по user_id
        if user_id is not None:
            results = [
                doc for doc in results
                if hasattr(doc, "metadata") and (
                    doc.metadata.get("user_id") == user_id
                )
            ]
        return results

    def build_context_prompt(self, query: str, context_documents: List[Document]) -> str:
        """
        Собирает финальный prompt с добавленным контекстом RAG из chunks.
        """
        context_chunks = [doc.page_content for doc in context_documents] if context_documents else []
        context_text = "\n---\n".join(context_chunks)
        prompt = f"{query}\n\nКонтекст:\n{context_text}"
        return prompt

rag_retriever = RAGRetriever()

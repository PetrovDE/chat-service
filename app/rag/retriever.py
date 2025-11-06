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
        """Загрузить и разложить документ, сгенерировать эмбеддинги для RAG."""
        docs = await self.documentloader.load_file(filepath, metadata)
        chunks = []
        for doc in docs:
            parts = self.textsplitter.split(doc)
            chunks.extend(parts)

        # Эмбеддинги через текущий embeddings_manager — учитывается mode/model!
        embeddings = embeddings_manager.embedd_documents([chunk.content for chunk in chunks])

        # Сохранять в vectorstore, использовать DI/объекты с нужным mode!
        for chunk, emb in zip(chunks, embeddings):
            chunk_id = chunk.metadata.get("id", None) or chunk.content[:40]
            self.vectorstore.add_document(
                doc_id=chunk_id,
                embedding=emb,
                metadata=chunk.metadata
            )
        return {"count_stored_chunks": len(chunks)}

    def query_rag(self, query_content: str, top_k: int = 5) -> List[Document]:
        """Запрос к RAG через vectorstore + эмбеддинг текущей модели/режима."""
        embedding_query = embeddings_manager.embedd_documents([query_content])[0]
        results = self.vectorstore.query(embedding_query, top_k=top_k)
        return results

rag_retriever = RAGRetriever()

# Create retriever instance
rag_retriever = RAGRetriever()

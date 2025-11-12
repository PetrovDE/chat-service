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
        embedding_query = embeddings_manager.embedd_documents([query_content])[0]
        results = self.vectorstore.query(embedding_query, top_k=top_k)
        logger.info(f"🔍 Vector store returned {len(results)} results")
        if user_id:
            filtered_results = [r for r in results if r.get('metadata', {}).get('user_id') == user_id]
            logger.info(f"🔍 After user_id filter: {len(filtered_results)} results")
            results = filtered_results
        documents = []
        for result in results:
            doc = Document(page_content=result.get('content', ''), metadata=result.get('metadata', {}))
            documents.append(doc)
        logger.info(f"✅ Returning {len(documents)} documents for RAG")
        return documents

    def build_context_prompt(self, query: str, context_documents: List[Document]) -> str:
        if not context_documents:
            return query
        context_chunks = []
        for i, doc in enumerate(context_documents, 1):
            content = doc.page_content
            filename = doc.metadata.get('filename', 'Unknown')
            context_chunks.append(f"[Документ {i} - {filename}]\n{content}")
        context_text = "\n\n---\n\n".join(context_chunks)
        prompt = f"""Используя следующий контекст из загруженных файлов, ответь на вопрос пользователя.

КОНТЕКСТ:
{context_text}

ВОПРОС:
{query}

ОТВЕТ:"""
        logger.info(f"📝 Built prompt with {len(context_documents)} documents ({len(prompt)} chars)")
        return prompt

rag_retriever = RAGRetriever()

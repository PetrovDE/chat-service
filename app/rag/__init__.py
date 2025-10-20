"""
RAG (Retrieval-Augmented Generation) module
Provides document processing, embedding, and retrieval capabilities
"""

from .embeddings import OllamaEmbeddingsManager
from .vector_store import VectorStoreManager
from .document_loader import DocumentLoader
from .text_splitter import SmartTextSplitter
from .retriever import RAGRetriever
from .config import RAGConfig

__all__ = [
    'OllamaEmbeddingsManager',
    'VectorStoreManager',
    'DocumentLoader',
    'SmartTextSplitter',
    'RAGRetriever',
    'RAGConfig'
]

__version__ = '1.0.0'
# app/services/file.py
import asyncio
from pathlib import Path
from uuid import UUID
import logging

from app.db.session import AsyncSessionLocal
from app.crud import crud_file
from app.rag.document_loader import DocumentLoader
from app.rag.text_splitter import SmartTextSplitter
from app.rag.embeddings import EmbeddingsManager
from app.rag.vector_store import VectorStoreManager
from app.core.config import settings

logger = logging.getLogger(__name__)

document_loader = DocumentLoader()
text_splitter = SmartTextSplitter(
    chunk_size=settings.CHUNK_SIZE,
    chunk_overlap=settings.CHUNK_OVERLAP
)
embedding_service = EmbeddingsManager()
vector_store = VectorStoreManager()


async def process_file_async(file_id: UUID, file_path: Path) -> None:
    """Process file in background"""
    asyncio.create_task(_process_file(file_id, file_path))


async def _process_file(file_id: UUID, file_path: Path) -> None:
    """Internal file processing"""
    async with AsyncSessionLocal() as db:
        try:
            # Update status to processing
            await crud_file.update_processing_status(
                db, file_id=file_id, status="processing"
            )

            # Load document
            content = await asyncio.to_thread(
                document_loader.load, str(file_path)
            )

            # Split into chunks
            chunks = await asyncio.to_thread(
                text_splitter.split_text, content
            )

            # Generate embeddings and store
            for chunk in chunks:
                embeddings = embedding_service.embedd_documents([chunk])
                if embeddings:
                    embedding = embeddings[0]
                    file_record = await crud_file.get(db, id=file_id)
                    vector_store.add_document(
                        doc_id=f"{file_id}_{chunks.index(chunk)}",
                        embedding=embedding,
                        metadata={
                            "file_id": str(file_id),
                            "user_id": str(file_record.user_id),
                            "content": chunk
                        }
                    )

            # Update status to completed
            await crud_file.update_processing_status(
                db,
                file_id=file_id,
                status="completed",
                chunks_count=len(chunks),
                embedding_model=settings.EMBEDDINGS_MODEL
            )

            logger.info(f"File {file_id} processed: {len(chunks)} chunks")

        except Exception as e:
            logger.error(f"File processing failed: {e}")
            await crud_file.update_processing_status(
                db, file_id=file_id, status="failed"
            )

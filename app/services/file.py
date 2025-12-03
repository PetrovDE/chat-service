import asyncio
from pathlib import Path
from uuid import UUID
import logging
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.crud import crud_file
from app.rag.document_loader import DocumentLoader
from app.rag.text_splitter import SmartTextSplitter
from app.rag.embeddings import EmbeddingsManager
from app.rag.vector_store import VectorStoreManager
from app.core.config import settings
from app.db.models.conversation_file import ConversationFile  # ✅ ДОБАВЛЕНО

logger = logging.getLogger(__name__)

document_loader = DocumentLoader()
text_splitter = SmartTextSplitter(
    chunk_size=settings.CHUNK_SIZE,
    chunk_overlap=settings.CHUNK_OVERLAP
)
embedding_service = EmbeddingsManager()
vector_store = VectorStoreManager()


async def process_file_async(file_id: UUID, file_path: Path) -> None:
    asyncio.create_task(_process_file(file_id, file_path))


async def _process_file(file_id: UUID, file_path: Path) -> None:
    async with AsyncSessionLocal() as db:
        try:
            logger.info(f"🔄 Starting file processing: {file_id}")

            await crud_file.update_processing_status(
                db,
                file_id=file_id,
                status="processing"
            )

            # ✅ ИСПРАВЛЕНИЕ: Получаем conversation_id ДО обработки файла
            query = select(ConversationFile.conversation_id).where(
                ConversationFile.file_id == file_id
            )
            result = await db.execute(query)
            conversation_ids = result.scalars().all()

            conversation_id = None
            if conversation_ids:
                conversation_id = str(conversation_ids[0])
                logger.info(f"File {file_id} associated with conversation {conversation_id}")
            else:
                logger.warning(f"File {file_id} not associated with any conversation")

            logger.info(f"📂 Loading file: {file_path}")
            documents = await document_loader.load_file(str(file_path))

            if not documents:
                raise ValueError("No documents loaded from file")

            logger.info(f"✅ Loaded {len(documents)} document(s)")

            chunk_docs = text_splitter.split_documents(documents)
            if not chunk_docs:
                raise ValueError("No chunks created from documents")

            logger.info(f"✅ Created {len(chunk_docs)} chunks")

            file_record = await crud_file.get(db, id=file_id)
            if not file_record:
                raise ValueError(f"File record not found: {file_id}")

            logger.info(f"🧮 Generating embeddings and storing in vector DB...")

            for idx, chunk_doc in enumerate(chunk_docs):
                chunk_text = chunk_doc.page_content
                embeddings = embedding_service.embedd_documents([chunk_text])

                if embeddings and len(embeddings) > 0:
                    embedding = embeddings[0]

                    # ✅ ИСПРАВЛЕНИЕ: Добавляем conversation_id в метаданные!
                    metadata = {
                        "file_id": str(file_id),
                        "user_id": str(file_record.user_id),
                        "conversation_id": conversation_id,  # ✅ КРИТИЧЕСКИ ВАЖНО!
                        "chunk_index": idx,
                        "total_chunks": len(chunk_docs),
                        "filename": file_record.original_filename,
                        "file_type": file_record.file_type,
                        "content": chunk_text
                    }

                    # Добавляем метаданные из chunk_doc если есть
                    if chunk_doc.metadata:
                        metadata.update(chunk_doc.metadata)

                    vector_store.add_document(
                        doc_id=f"{file_id}_{idx}",
                        embedding=embedding,
                        metadata=metadata
                    )
                else:
                    logger.warning(f"⚠️ No embedding generated for chunk {idx}")

            logger.info(f"✅ All chunks stored in vector DB")

            await crud_file.update_processing_status(
                db,
                file_id=file_id,
                status="completed",
                chunks_count=len(chunk_docs),
                embedding_model=settings.EMBEDDINGS_MODEL
            )

            logger.info(f"✅ File {file_id} processed successfully: {len(chunk_docs)} chunks")

        except Exception as e:
            logger.error(
                f"❌ File processing failed for {file_id}: {type(e).__name__}: {str(e)}",
                exc_info=True
            )
            await crud_file.update_processing_status(
                db,
                file_id=file_id,
                status="failed"
            )

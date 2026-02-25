"""
File processing service: extraction -> chunking -> embeddings -> vector store.

Notes:
- For local/ollama embeddings use settings.OLLAMA_EMBED_MODEL first.
- embedding_model from endpoint is treated as an override, but chat-model
  names are ignored for embedding workflows.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from sqlalchemy import select

from app.core.config import settings
from app.crud import crud_file
from app.db.models.conversation_file import ConversationFile
from app.db.session import AsyncSessionLocal
from app.rag.document_loader import DocumentLoader
from app.rag.embeddings import EmbeddingsManager
from app.rag.text_splitter import SmartTextSplitter
from app.rag.vector_store import VectorStoreManager

logger = logging.getLogger(__name__)

document_loader = DocumentLoader()
text_splitter = SmartTextSplitter(chunk_size=settings.CHUNK_SIZE, chunk_overlap=settings.CHUNK_OVERLAP)
vector_store = VectorStoreManager()

EMBED_BATCH_SIZE = 32


def _batch(items: List[Any], size: int) -> List[List[Any]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _looks_like_chat_model(model_name: str) -> bool:
    m = (model_name or "").lower().strip()
    if not m:
        return False
    embed_tokens = ["embed", "embedding", "nomic", "bge", "e5", "gte", "text-embedding"]
    if any(t in m for t in embed_tokens):
        return False
    chat_tokens = ["llama", "mistral", "phi", "qwen", "gemma", "yi", "deepseek", "mixtral", "gpt", "claude"]
    return any(t in m for t in chat_tokens)


def _resolve_embedding_model(mode: str, override: Optional[str]) -> str:
    """Resolve which model should be used for embeddings."""
    mode = (mode or "local").lower().strip()
    if mode == "corporate":
        mode = "aihub"

    if mode in ("local", "ollama"):
        base = settings.OLLAMA_EMBED_MODEL or settings.EMBEDDINGS_MODEL
    elif mode == "aihub":
        base = settings.AIHUB_EMBEDDING_MODEL or settings.EMBEDDINGS_MODEL
    else:
        base = settings.EMBEDDINGS_MODEL

    if override:
        if _looks_like_chat_model(override):
            logger.warning("embedding_model override looks like chat model (%s). Using %s", override, base)
            return base
        return override

    return base


async def process_file_async(
    file_id: UUID,
    file_path: Path,
    embedding_mode: str = "local",
    embedding_model: Optional[str] = None,
) -> None:
    resolved = _resolve_embedding_model(embedding_mode, embedding_model)
    logger.info(
        "Scheduling file processing: file_id=%s mode=%s model_override=%s resolved_model=%s path=%s",
        file_id, embedding_mode, embedding_model, resolved, file_path
    )
    asyncio.create_task(_process_file(file_id, file_path, embedding_mode, resolved))


async def process_file_background(
    file_id: UUID,
    file_path: Path,
    embedding_mode: str = "local",
    embedding_model: Optional[str] = None,
) -> None:
    resolved = _resolve_embedding_model(embedding_mode, embedding_model)
    logger.info(
        "Running file processing (await): file_id=%s mode=%s model_override=%s resolved_model=%s path=%s",
        file_id, embedding_mode, embedding_model, resolved, file_path
    )
    await _process_file(file_id, file_path, embedding_mode, resolved)


async def _process_file(
    file_id: UUID,
    file_path: Path,
    embedding_mode: str,
    embedding_model: str,
) -> None:
    async with AsyncSessionLocal() as db:
        try:
            logger.info(
                "Starting file processing: file_id=%s mode=%s embedding_model=%s path=%s",
                file_id, embedding_mode, embedding_model, file_path
            )
            await crud_file.update_processing_status(db, file_id=file_id, status="processing")

            # conversation_id is used by retrieval filters
            q = select(ConversationFile.conversation_id).where(ConversationFile.file_id == file_id)
            r = await db.execute(q)
            conv_ids = r.scalars().all()
            conversation_id = str(conv_ids[0]) if conv_ids else None

            # load documents
            docs = await document_loader.load_file(str(file_path))
            if not docs:
                raise ValueError("No documents loaded from file")

            total_chars = sum(len(d.page_content or "") for d in docs)
            logger.info("Loaded documents=%d total_chars=%d", len(docs), total_chars)
            if total_chars < 50:
                raise ValueError("Extracted text is empty/too small (possible scanned PDF).")

            # split
            chunks = text_splitter.split_documents(docs)
            if not chunks:
                raise ValueError("No chunks created from documents")
            logger.info("Created chunks=%d chunk_size=%d overlap=%d", len(chunks), settings.CHUNK_SIZE, settings.CHUNK_OVERLAP)

            file_record = await crud_file.get(db, id=file_id)
            if not file_record:
                raise ValueError(f"File record not found: {file_id}")

            emb = EmbeddingsManager(mode=embedding_mode, model=embedding_model)
            logger.info("Embedding mode=%s resolved_model=%s", embedding_mode, embedding_model)

            # pre-clean by file_id
            deleted = 0
            try:
                deleted = vector_store.delete_by_metadata({"file_id": str(file_id)})
                logger.info("Pre-clean vector store by file_id=%s deleted=%d", file_id, deleted)
            except Exception:
                logger.warning("Vector pre-clean failed (continue)", exc_info=True)

            items: List[Tuple[str, Dict[str, Any], str]] = []
            for idx, cd in enumerate(chunks):
                text = (cd.page_content or "").strip()
                if not text:
                    continue
                meta: Dict[str, Any] = {
                    "file_id": str(file_id),
                    "user_id": str(file_record.user_id),
                    "conversation_id": conversation_id,
                    "chunk_index": idx,
                    "filename": file_record.original_filename,
                    "file_type": file_record.file_type,
                    "embedding_mode": embedding_mode,
                    "embedding_model": embedding_model,
                }
                if cd.metadata:
                    for k, v in cd.metadata.items():
                        if k not in meta:
                            meta[k] = v
                doc_id = f"{file_id}_{idx}"
                items.append((text, meta, doc_id))

            if not items:
                raise ValueError("All chunks are empty after split (nothing to embed).")

            stored = 0
            failed = 0
            batches = _batch(items, EMBED_BATCH_SIZE)
            logger.info("Embedding batches=%d batch_size=%d", len(batches), EMBED_BATCH_SIZE)

            for i, batch in enumerate(batches, start=1):
                texts = [t for (t, _, _) in batch]
                try:
                    vectors = await emb.embedd_documents_async(texts)
                except Exception:
                    failed += len(batch)
                    logger.warning("Embedding batch %d/%d failed (skip batch)", i, len(batches), exc_info=True)
                    continue

                if not vectors or len(vectors) != len(batch):
                    failed += len(batch)
                    logger.warning("Embedding batch %d/%d invalid vectors size", i, len(batches))
                    continue

                for vec, (text, meta, doc_id) in zip(vectors, batch):
                    try:
                        ok = vector_store.add_document(
                            doc_id=doc_id,
                            embedding=vec,
                            metadata=meta,
                            content=text,
                        )
                        if ok:
                            stored += 1
                        else:
                            failed += 1
                            logger.warning("Vector upsert returned False doc_id=%s", doc_id)
                    except Exception:
                        failed += 1
                        logger.warning("Vector upsert failed doc_id=%s", doc_id, exc_info=True)

            if stored == 0:
                raise ValueError(f"No chunks were stored to vector DB. failed_embeddings={failed} total_items={len(items)}")

            await crud_file.update_processing_status(
                db,
                file_id=file_id,
                status="completed",
                chunks_count=stored,
                embedding_model=f"{embedding_mode}:{embedding_model}",
            )

            logger.info(
                "File processed successfully: file_id=%s stored_chunks=%d failed_embeddings=%d deleted_old=%d conversation_id=%s embedding_model=%s",
                file_id, stored, failed, deleted, conversation_id, embedding_model
            )

        except Exception as e:
            logger.error("File processing failed: file_id=%s err=%s: %s", file_id, type(e).__name__, str(e), exc_info=True)
            await crud_file.update_processing_status(db, file_id=file_id, status="failed")

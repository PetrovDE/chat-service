"""
File processing service: extraction -> chunking -> embeddings -> vector store.

Notes:
- For local/ollama embeddings use settings.OLLAMA_EMBED_MODEL first.
- embedding_model from endpoint is treated as an override, but chat-model
  names are ignored for embedding workflows.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
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
from app.observability.metrics import inc_counter, observe_ms

logger = logging.getLogger(__name__)

document_loader = DocumentLoader()
text_splitter = SmartTextSplitter(chunk_size=settings.CHUNK_SIZE, chunk_overlap=settings.CHUNK_OVERLAP)
vector_store = VectorStoreManager()

EMBED_BATCH_SIZE = 32
MAX_BACKGROUND_RETRIES = 3
RETRY_DELAY_SECONDS = 2.0

_job_queue: Optional[asyncio.Queue[Tuple[UUID, Path, str, str, int]]] = None
_worker_task: Optional[asyncio.Task] = None
_worker_lock = asyncio.Lock()


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
    await _ensure_worker_started()
    assert _job_queue is not None
    await _job_queue.put((file_id, file_path, embedding_mode, resolved, 0))
    inc_counter("file_processing_enqueued_total", mode=embedding_mode)
    observe_ms("file_processing_queue_depth", float(_job_queue.qsize()))


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


async def _ensure_worker_started() -> None:
    global _job_queue, _worker_task
    if _worker_task is not None and not _worker_task.done():
        return

    async with _worker_lock:
        if _worker_task is not None and not _worker_task.done():
            return
        _job_queue = asyncio.Queue()
        _worker_task = asyncio.create_task(_file_processing_worker(), name="file-processing-worker")
        logger.info("File processing worker started")


async def _file_processing_worker() -> None:
    assert _job_queue is not None
    while True:
        try:
            file_id, file_path, embedding_mode, embedding_model, attempt = await _job_queue.get()
        except asyncio.CancelledError:
            logger.info("File processing worker cancelled")
            break
        try:
            loop = asyncio.get_running_loop()
            started = loop.time()
            ok = await _process_file(file_id, file_path, embedding_mode, embedding_model)
            if (not ok) and attempt < MAX_BACKGROUND_RETRIES:
                delay = RETRY_DELAY_SECONDS * (attempt + 1)
                inc_counter("file_processing_retry_total", mode=embedding_mode)
                logger.warning(
                    "Retrying file processing: file_id=%s attempt=%d/%d delay=%.1fs",
                    file_id,
                    attempt + 1,
                    MAX_BACKGROUND_RETRIES,
                    delay,
                )
                await asyncio.sleep(delay)
                await _job_queue.put((file_id, file_path, embedding_mode, embedding_model, attempt + 1))
            elif ok:
                inc_counter("file_processing_completed_total", mode=embedding_mode)
            else:
                inc_counter("file_processing_failed_total", mode=embedding_mode)
            observe_ms("file_processing_job_duration_ms", (loop.time() - started) * 1000.0, mode=embedding_mode)
        except Exception:
            logger.exception("Unexpected worker failure while processing file_id=%s", file_id)
            inc_counter("file_processing_worker_errors_total")
        finally:
            _job_queue.task_done()


def get_file_processing_worker_stats() -> Dict[str, Any]:
    queue_size = _job_queue.qsize() if _job_queue is not None else 0
    running = bool(_worker_task is not None and not _worker_task.done())
    return {
        "worker_running": running,
        "queue_size": queue_size,
        "max_retries": MAX_BACKGROUND_RETRIES,
        "retry_delay_seconds": RETRY_DELAY_SECONDS,
    }


async def shutdown_file_processing_worker(timeout_seconds: float = 15.0) -> None:
    global _job_queue, _worker_task
    if _worker_task is None:
        return

    queue = _job_queue
    if queue is not None:
        try:
            await asyncio.wait_for(queue.join(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            logger.warning("File worker shutdown timeout: queue still has pending tasks")

    task = _worker_task
    _worker_task = None
    _job_queue = None

    if task is not None and not task.done():
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
    logger.info("File processing worker stopped")


async def _process_file(
    file_id: UUID,
    file_path: Path,
    embedding_mode: str,
    embedding_model: str,
) -> bool:
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
            return True

        except Exception as e:
            logger.error("File processing failed: file_id=%s err=%s: %s", file_id, type(e).__name__, str(e), exc_info=True)
            await crud_file.update_processing_status(db, file_id=file_id, status="failed")
            return False

"""
File processing service: extraction -> chunking -> embeddings -> vector store.

Notes:
- For local/ollama embeddings use settings.OLLAMA_EMBED_MODEL first.
- embedding_model from endpoint is treated as an override, but chat-model
  names are ignored for embedding workflows.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID, uuid4

from sqlalchemy import select

from app.core.config import settings
from app.crud import crud_file
from app.db.models.conversation_file import ConversationFile
from app.db.models.file import File as FileModel
from app.db.session import AsyncSessionLocal
from app.rag.document_loader import DocumentLoader
from app.rag.embeddings import EmbeddingsManager
from app.rag.text_splitter import SmartTextSplitter
from app.rag.vector_store import VectorStoreManager
from app.observability.context import file_id_ctx
from app.observability.metrics import inc_counter, observe_ms
from app.observability.slo_metrics import observe_ingestion_enqueue, set_ingestion_queue_snapshot
from app.services.ingestion import (
    DurableIngestionWorker,
    IngestionJobPayload,
    IngestionWorkerConfig,
    SqliteIngestionQueueAdapter,
)
from app.services.tabular.storage_adapter import build_tabular_dataset_metadata

logger = logging.getLogger(__name__)

document_loader = DocumentLoader()
text_splitter = SmartTextSplitter(chunk_size=settings.CHUNK_SIZE, chunk_overlap=settings.CHUNK_OVERLAP)
vector_store = VectorStoreManager()

EMBED_BATCH_SIZE = 32

_ingestion_worker: Optional[DurableIngestionWorker] = None
_ingestion_worker_lock = asyncio.Lock()


def _batch(items: List[Any], size: int) -> List[List[Any]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_ratio(numerator: int, denominator: int) -> float:
    return float(numerator / denominator) if denominator > 0 else 0.0


def _parse_embedding_model_meta(value: Optional[str]) -> Tuple[str, Optional[str]]:
    raw = (value or "").strip()
    if not raw:
        return "local", None
    if ":" not in raw:
        return "local", raw
    mode_raw, model_raw = raw.split(":", 1)
    mode = (mode_raw or "local").strip().lower()
    if mode == "corporate":
        mode = "aihub"
    if mode == "ollama":
        mode = "local"
    model = (model_raw or "").strip() or None
    return mode, model


def _extract_xlsx_stats(docs: List[Any]) -> Dict[str, Any]:
    sheet_names = set()
    total_rows = 0
    for d in docs:
        meta = (d.metadata or {}) if hasattr(d, "metadata") else {}
        sheet = meta.get("sheet_name")
        if sheet:
            sheet_names.add(str(sheet))
        row_end = meta.get("row_end")
        try:
            if row_end is not None:
                total_rows = max(total_rows, int(row_end))
        except Exception:
            continue
    return {
        "xlsx_sheets_count": len(sheet_names),
        "xlsx_rows_estimate": total_rows,
    }


async def _finalize_ingestion(
    *,
    db: Any,
    file_id: UUID,
    progress: Dict[str, Any],
    embedding_mode: str,
    embedding_model: str,
    error_message: Optional[str],
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> str:
    expected = int(progress.get("total_chunks_expected", 0) or 0)
    processed = int(progress.get("chunks_processed", 0) or 0)
    failed = int(progress.get("chunks_failed", 0) or 0)
    indexed = int(progress.get("chunks_indexed", 0) or 0)

    if processed > expected:
        logger.warning(
            "Finalize normalized exceeded counters: file_id=%s expected_before=%d processed=%d",
            file_id,
            expected,
            processed,
        )
        expected = processed
        progress["total_chunks_expected"] = expected

    if expected > 0 and processed < expected:
        remainder = expected - processed
        failed += remainder
        processed = expected
        progress["chunks_failed"] = failed
        progress["chunks_processed"] = processed
        logger.warning(
            "Finalize fixed incomplete counters: file_id=%s expected=%d processed_before=%d added_failed=%d",
            file_id,
            expected,
            processed - remainder,
            remainder,
        )

    bad_ratio = _safe_ratio(failed, expected)
    threshold = float(settings.INGESTION_BAD_CHUNK_RATIO_THRESHOLD)

    if indexed <= 0:
        final_status = "failed"
    elif failed <= 0:
        final_status = "completed"
    elif bad_ratio > threshold:
        final_status = "failed"
    else:
        final_status = "partial_success"

    progress["status"] = final_status
    progress["bad_ratio"] = bad_ratio
    progress["finished_at"] = _utc_now_iso()
    progress["stage"] = "finalized"

    metadata_patch: Dict[str, Any] = {"ingestion_progress": progress}
    if error_message:
        metadata_patch["error"] = error_message
    if extra_metadata:
        metadata_patch.update(extra_metadata)

    await crud_file.update_processing_status(
        db,
        file_id=file_id,
        status=final_status,
        chunks_count=indexed,
        embedding_model=f"{embedding_mode}:{embedding_model}",
        metadata_patch=metadata_patch,
    )

    inc_counter("ingestion_chunks_total", value=expected)
    inc_counter("ingestion_chunks_ok", value=indexed)
    inc_counter("ingestion_chunks_bad", value=failed)
    inc_counter("ingestion_upserts_ok", value=indexed)
    inc_counter("ingestion_upserts_fail", value=failed)
    inc_counter("ingestion_finalize_total", status=final_status)
    logger.info(
        "Ingestion finalized: file_id=%s status=%s expected=%d processed=%d indexed=%d failed=%d bad_ratio=%.4f threshold=%.4f",
        file_id,
        final_status,
        expected,
        processed,
        indexed,
        failed,
        bad_ratio,
        threshold,
    )
    return final_status


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


def _normalize_embedding_mode(mode: str) -> str:
    normalized = (mode or "local").strip().lower()
    if normalized == "corporate":
        return "aihub"
    if normalized == "ollama":
        return "local"
    return normalized or "local"


def _build_ingestion_idempotency_key(
    *,
    file_id: UUID,
    file_path: Path,
    embedding_mode: str,
    embedding_model: str,
) -> str:
    raw = f"{file_id}|{str(file_path)}|{embedding_mode}|{embedding_model}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
    return f"ingestion:{file_id}:{digest}"


def _build_worker_config() -> IngestionWorkerConfig:
    return IngestionWorkerConfig(
        worker_id=f"api-{uuid4().hex[:8]}",
        lease_seconds=float(settings.INGESTION_WORKER_LEASE_SECONDS),
        poll_interval_seconds=float(settings.INGESTION_WORKER_POLL_INTERVAL_SECONDS),
        heartbeat_interval_seconds=float(settings.INGESTION_WORKER_HEARTBEAT_SECONDS),
        retry_base_seconds=float(settings.INGESTION_RETRY_BASE_SECONDS),
        retry_max_seconds=float(settings.INGESTION_RETRY_MAX_SECONDS),
    )


async def _process_payload_for_worker(payload: IngestionJobPayload) -> Tuple[bool, bool]:
    try:
        file_id = UUID(payload.file_id)
    except Exception:
        logger.error("Invalid ingestion payload file_id=%s", payload.file_id)
        return False, False
    return await _process_file(
        file_id=file_id,
        file_path=Path(payload.file_path),
        embedding_mode=payload.embedding_mode,
        embedding_model=payload.embedding_model,
    )


async def _ensure_worker_started() -> DurableIngestionWorker:
    global _ingestion_worker
    if _ingestion_worker is not None and bool(_ingestion_worker.snapshot().get("worker_running")):
        return _ingestion_worker

    async with _ingestion_worker_lock:
        if _ingestion_worker is not None and bool(_ingestion_worker.snapshot().get("worker_running")):
            return _ingestion_worker

        adapter = SqliteIngestionQueueAdapter(Path(settings.INGESTION_QUEUE_SQLITE_PATH))
        worker = DurableIngestionWorker(
            queue=adapter,
            processor=_process_payload_for_worker,
            config=_build_worker_config(),
        )
        await worker.start()
        _ingestion_worker = worker
        logger.info("Durable ingestion runtime started: queue=%s", settings.INGESTION_QUEUE_SQLITE_PATH)
        return worker


async def process_file_async(
    file_id: UUID,
    file_path: Path,
    embedding_mode: str = "local",
    embedding_model: Optional[str] = None,
) -> None:
    mode = _normalize_embedding_mode(embedding_mode)
    resolved = _resolve_embedding_model(mode, embedding_model)
    logger.info(
        "Scheduling file processing: file_id=%s mode=%s model_override=%s resolved_model=%s path=%s",
        file_id, mode, embedding_model, resolved, file_path
    )
    worker = await _ensure_worker_started()
    job_payload = IngestionJobPayload(
        file_id=str(file_id),
        file_path=str(file_path),
        embedding_mode=mode,
        embedding_model=resolved,
    )
    result = await worker.enqueue(
        payload=job_payload,
        idempotency_key=_build_ingestion_idempotency_key(
            file_id=file_id,
            file_path=file_path,
            embedding_mode=mode,
            embedding_model=resolved,
        ),
        max_retries=int(settings.INGESTION_MAX_RETRIES),
        allow_requeue_terminal=True,
    )
    if result.deduplicated:
        inc_counter("ingestion_jobs_deduplicated_total", mode=mode)
    else:
        inc_counter("ingestion_jobs_enqueued_total", mode=mode)
    observe_ingestion_enqueue(mode=mode, deduplicated=bool(result.deduplicated))

    stats = worker.snapshot()
    observe_ms("file_processing_queue_depth", float(stats.get("queue_size", 0)))
    set_ingestion_queue_snapshot(
        depth=float(stats.get("queue_size", 0) or 0.0),
        processing=float(stats.get("processing", 0) or 0.0),
        dead_letter_depth=float(stats.get("dead_letter", 0) or 0.0),
        lag_seconds=float(stats.get("lag_seconds", 0.0) or 0.0),
        heartbeat_age_seconds=(
            float(stats["heartbeat_age_seconds"])
            if stats.get("heartbeat_age_seconds") is not None
            else None
        ),
    )
    logger.info(
        "Ingestion job queued: file_id=%s job_id=%s deduplicated=%s status=%s",
        file_id,
        result.job_id,
        result.deduplicated,
        result.status,
    )


async def process_file_background(
    file_id: UUID,
    file_path: Path,
    embedding_mode: str = "local",
    embedding_model: Optional[str] = None,
) -> None:
    mode = _normalize_embedding_mode(embedding_mode)
    resolved = _resolve_embedding_model(mode, embedding_model)
    logger.info(
        "Running file processing (await): file_id=%s mode=%s model_override=%s resolved_model=%s path=%s",
        file_id, mode, embedding_model, resolved, file_path
    )
    await _process_file(file_id, file_path, mode, resolved)


def get_file_processing_worker_stats() -> Dict[str, Any]:
    if _ingestion_worker is None:
        return {
            "worker_running": False,
            "worker_id": None,
            "queue_size": 0,
            "processing": 0,
            "completed": 0,
            "dead_letter": 0,
            "lag_seconds": 0.0,
            "heartbeat_age_seconds": None,
            "healthy": False,
            "max_retries": int(settings.INGESTION_MAX_RETRIES),
            "retry_base_seconds": float(settings.INGESTION_RETRY_BASE_SECONDS),
            "retry_max_seconds": float(settings.INGESTION_RETRY_MAX_SECONDS),
            "retry_delay_seconds": float(settings.INGESTION_RETRY_BASE_SECONDS),
        }

    snapshot = dict(_ingestion_worker.snapshot())
    snapshot["max_retries"] = int(settings.INGESTION_MAX_RETRIES)
    snapshot["retry_delay_seconds"] = float(settings.INGESTION_RETRY_BASE_SECONDS)
    return snapshot


async def shutdown_file_processing_worker(timeout_seconds: float = 15.0) -> None:
    global _ingestion_worker
    worker = _ingestion_worker
    _ingestion_worker = None
    if worker is None:
        return

    effective_timeout = timeout_seconds
    if timeout_seconds == 15.0:
        effective_timeout = float(settings.INGESTION_WORKER_SHUTDOWN_TIMEOUT_SECONDS)
    await worker.stop(timeout_seconds=float(effective_timeout))


async def recover_pending_file_jobs(limit: int = 200) -> int:
    """
    Recover files stuck in pending/processing states after service restart.
    Best effort: enqueue existing files back to durable queue (idempotent).
    """
    await _ensure_worker_started()
    recovered = 0
    async with AsyncSessionLocal() as db:
        stmt = (
            select(FileModel)
            .where(FileModel.is_processed.in_(["pending", "processing"]))
            .order_by(FileModel.uploaded_at.asc())
            .limit(limit)
        )
        res = await db.execute(stmt)
        files = res.scalars().all()

    for file_obj in files:
        file_path = Path(file_obj.path)
        if not file_path.exists():
            logger.warning("Skip recovery: file missing on disk file_id=%s path=%s", file_obj.id, file_path)
            continue

        mode, model = _parse_embedding_model_meta(getattr(file_obj, "embedding_model", None))
        try:
            await process_file_async(
                file_id=file_obj.id,
                file_path=file_path,
                embedding_mode=mode,
                embedding_model=model,
            )
            recovered += 1
        except Exception:
            logger.warning("Failed to recover pending file job file_id=%s", file_obj.id, exc_info=True)

    if recovered:
        logger.info("Recovered pending file jobs: %d", recovered)
    return recovered


async def _process_file(
    file_id: UUID,
    file_path: Path,
    embedding_mode: str,
    embedding_model: str,
) -> Tuple[bool, bool]:
    file_ctx_token = file_id_ctx.set(str(file_id))
    started_ms = asyncio.get_running_loop().time()
    progress: Dict[str, Any] = {
        "status": "processing",
        "stage": "queued",
        "total_chunks_expected": 0,
        "chunks_processed": 0,
        "chunks_failed": 0,
        "chunks_indexed": 0,
        "started_at": _utc_now_iso(),
        "finished_at": None,
        "bad_ratio": 0.0,
    }
    error_message: Optional[str] = None
    extra_metadata: Dict[str, Any] = {}

    async with AsyncSessionLocal() as db:
        try:
            logger.info(
                "Starting file processing: file_id=%s mode=%s embedding_model=%s path=%s",
                file_id, embedding_mode, embedding_model, file_path
            )
            await crud_file.update_processing_status(
                db,
                file_id=file_id,
                status="processing",
                metadata_patch={"ingestion_progress": progress, "error": None},
            )

            # conversation_id is used by retrieval filters
            q = select(ConversationFile.conversation_id).where(ConversationFile.file_id == file_id)
            r = await db.execute(q)
            conv_ids = r.scalars().all()
            conversation_id = str(conv_ids[0]) if conv_ids else None

            # load documents
            stage_t0 = asyncio.get_running_loop().time()
            progress["stage"] = "extract"
            docs = await document_loader.load_file(str(file_path))
            if not docs:
                raise ValueError("No documents loaded from file")

            total_chars = sum(len(d.page_content or "") for d in docs)
            xlsx_stats = _extract_xlsx_stats(docs) if file_path.suffix.lower() in (".xlsx", ".xls") else {}
            logger.info(
                "Loaded documents=%d extracted_text_chars=%d file_id=%s %s",
                len(docs),
                total_chars,
                file_id,
                " ".join([f"{k}={v}" for k, v in xlsx_stats.items()]) if xlsx_stats else "",
            )
            observe_ms("ingestion_stage_ms", (asyncio.get_running_loop().time() - stage_t0) * 1000.0, stage="extract")
            if total_chars < 50:
                raise ValueError("Extracted text is empty/too small (possible scanned PDF).")

            file_record = await crud_file.get(db, id=file_id)
            if not file_record:
                raise ValueError(f"File record not found: {file_id}")

            file_ext = file_path.suffix.lower().lstrip(".")
            if file_ext in {"xlsx", "xls", "csv"}:
                try:
                    tabular_dataset = await asyncio.to_thread(
                        build_tabular_dataset_metadata,
                        file_id=file_id,
                        file_path=file_path,
                        file_type=file_ext,
                        source_filename=file_record.original_filename,
                    )
                    if tabular_dataset:
                        extra_metadata["tabular_dataset"] = tabular_dataset
                        logger.info(
                            "Tabular dataset generated: file_id=%s dataset_version=%s tables=%d engine=%s",
                            file_id,
                            tabular_dataset.get("dataset_version"),
                            len(tabular_dataset.get("tables", [])),
                            tabular_dataset.get("engine"),
                        )
                except Exception:
                    logger.warning("Tabular dataset generation failed for file_id=%s", file_id, exc_info=True)
                    extra_metadata["tabular_dataset_error"] = "generation_failed"

            # split
            stage_t0 = asyncio.get_running_loop().time()
            progress["stage"] = "chunk"
            if file_ext in ("xlsx", "xls", "csv"):
                # Spreadsheet/CSV docs are already block-structured by loader.
                chunks = docs
            else:
                chunks = text_splitter.split_documents(docs)
            if not chunks:
                raise ValueError("No chunks created from documents")
            progress["total_chunks_expected"] = len(chunks)
            logger.info(
                "Created chunks_expected=%d chunk_size=%d overlap=%d file_id=%s",
                len(chunks),
                settings.CHUNK_SIZE,
                settings.CHUNK_OVERLAP,
                file_id,
            )
            observe_ms("ingestion_stage_ms", (asyncio.get_running_loop().time() - stage_t0) * 1000.0, stage="chunk")

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
            empty_chunks = 0
            for idx, cd in enumerate(chunks):
                text = (cd.page_content or "").strip()
                if not text:
                    empty_chunks += 1
                    continue
                meta: Dict[str, Any] = {
                    "file_id": str(file_id),
                    "user_id": str(file_record.user_id),
                    "conversation_id": conversation_id,
                    "chunk_index": idx,
                    "doc_id": str(file_id),
                    "chunk_id": f"{file_id}_{idx}",
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

            progress["chunks_failed"] = int(progress["chunks_failed"]) + empty_chunks
            progress["chunks_processed"] = int(progress["chunks_processed"]) + empty_chunks
            stored = 0
            batches = _batch(items, EMBED_BATCH_SIZE)
            logger.info("Embedding batches=%d batch_size=%d", len(batches), EMBED_BATCH_SIZE)

            stage_t0 = asyncio.get_running_loop().time()
            progress["stage"] = "embed_upsert"
            for i, batch in enumerate(batches, start=1):
                texts = [t for (t, _, _) in batch]
                try:
                    vectors = await emb.embedd_documents_async(texts)
                except Exception:
                    progress["chunks_failed"] = int(progress["chunks_failed"]) + len(batch)
                    progress["chunks_processed"] = int(progress["chunks_processed"]) + len(batch)
                    logger.warning("Embedding batch %d/%d failed (skip batch)", i, len(batches), exc_info=True)
                    continue

                if not vectors or len(vectors) != len(batch):
                    progress["chunks_failed"] = int(progress["chunks_failed"]) + len(batch)
                    progress["chunks_processed"] = int(progress["chunks_processed"]) + len(batch)
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
                            progress["chunks_indexed"] = int(progress["chunks_indexed"]) + 1
                            progress["chunks_processed"] = int(progress["chunks_processed"]) + 1
                        else:
                            progress["chunks_failed"] = int(progress["chunks_failed"]) + 1
                            progress["chunks_processed"] = int(progress["chunks_processed"]) + 1
                            logger.warning("Vector upsert returned False doc_id=%s", doc_id)
                    except Exception:
                        progress["chunks_failed"] = int(progress["chunks_failed"]) + 1
                        progress["chunks_processed"] = int(progress["chunks_processed"]) + 1
                        logger.warning("Vector upsert failed doc_id=%s", doc_id, exc_info=True)

            observe_ms("ingestion_stage_ms", (asyncio.get_running_loop().time() - stage_t0) * 1000.0, stage="embed_upsert")
            logger.info(
                "File processing done before finalize: file_id=%s chunks_expected=%d chunks_processed=%d chunks_indexed=%d chunks_failed=%d empty_chunks=%d deleted_old=%d conversation_id=%s embedding_model=%s",
                file_id,
                progress["total_chunks_expected"],
                progress["chunks_processed"],
                progress["chunks_indexed"],
                progress["chunks_failed"],
                empty_chunks,
                deleted,
                conversation_id,
                embedding_model,
            )

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}"
            progress["stage"] = "failed"
            logger.error("File processing failed: file_id=%s err=%s", file_id, error_message, exc_info=True)
        finally:
            try:
                observe_ms(
                    "ingestion_total_ms",
                    (asyncio.get_running_loop().time() - started_ms) * 1000.0,
                    file_type=file_path.suffix.lower().lstrip(".") or "unknown",
                )
                final_status = await _finalize_ingestion(
                    db=db,
                    file_id=file_id,
                    progress=progress,
                    embedding_mode=embedding_mode,
                    embedding_model=embedding_model,
                    error_message=error_message,
                    extra_metadata=extra_metadata,
                )
                retryable = bool(
                    final_status == "failed"
                    and int(progress.get("total_chunks_expected", 0) or 0) == 0
                    and int(progress.get("chunks_processed", 0) or 0) == 0
                    and bool(error_message)
                )
                return (final_status in ("completed", "partial_success"), retryable)
            finally:
                file_id_ctx.reset(file_ctx_token)

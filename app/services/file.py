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
from app.services.file_pipeline import finalize_ingestion_pipeline, process_file_pipeline
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
    return await finalize_ingestion_pipeline(
        db=db,
        file_id=file_id,
        progress=progress,
        embedding_mode=embedding_mode,
        embedding_model=embedding_model,
        error_message=error_message,
        extra_metadata=extra_metadata,
        safe_ratio_fn=_safe_ratio,
        utc_now_iso_fn=_utc_now_iso,
        bad_chunk_ratio_threshold=float(settings.INGESTION_BAD_CHUNK_RATIO_THRESHOLD),
        update_processing_status_fn=crud_file.update_processing_status,
        inc_counter_fn=inc_counter,
        logger_obj=logger,
    )


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
    return await process_file_pipeline(
        file_id=file_id,
        file_path=file_path,
        embedding_mode=embedding_mode,
        embedding_model=embedding_model,
        async_session_factory=AsyncSessionLocal,
        crud_file_module=crud_file,
        conversation_file_model=ConversationFile,
        select_fn=select,
        document_loader_obj=document_loader,
        text_splitter_obj=text_splitter,
        vector_store_obj=vector_store,
        embeddings_manager_cls=EmbeddingsManager,
        build_tabular_dataset_metadata_fn=build_tabular_dataset_metadata,
        finalize_ingestion_fn=_finalize_ingestion,
        batch_fn=_batch,
        utc_now_iso_fn=_utc_now_iso,
        extract_xlsx_stats_fn=_extract_xlsx_stats,
        file_id_ctx_var=file_id_ctx,
        settings_obj=settings,
        observe_ms_fn=observe_ms,
        logger_obj=logger,
    )

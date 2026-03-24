"""
File processing service: extraction -> chunking -> embeddings -> vector store.

Notes:
- Embedding model resolution is provider-aware and capability-aware.
- chat model and embedding model are resolved independently.
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
from sqlalchemy.orm import selectinload

from app.core.config import settings
from app.crud import crud_file
from app.db.models.conversation_file import ConversationFile
from app.db.models.file import File as FileModel
from app.db.models.file_processing import FileProcessingProfile
from app.db.session import AsyncSessionLocal
from app.rag.document_loader import DocumentLoader
from app.rag.embeddings import EmbeddingsManager
from app.rag.text_splitter import SmartTextSplitter
from app.rag.vector_store import VectorStoreManager
from app.observability.context import file_id_ctx
from app.observability.context import request_id_ctx
from app.observability.file_lifecycle import log_file_lifecycle_event
from app.observability.metrics import inc_counter, observe_ms
from app.observability.slo_metrics import observe_ingestion_enqueue, set_ingestion_queue_snapshot
from app.services.file_pipeline import finalize_ingestion_pipeline, process_file_pipeline
from app.services.ingestion import (
    DurableIngestionWorker,
    IngestionJobPayload,
    IngestionWorkerConfig,
    SqliteIngestionQueueAdapter,
)
from app.services.ingestion.derived_artifacts import persist_derived_artifacts
from app.services.llm.exceptions import ProviderAuthError, ProviderConfigError, ProviderTransientError
from app.services.llm.manager import llm_manager
from app.services.tabular.storage_adapter import build_tabular_dataset_metadata

logger = logging.getLogger(__name__)

document_loader = DocumentLoader()
text_splitter = SmartTextSplitter(chunk_size=settings.CHUNK_SIZE, chunk_overlap=settings.CHUNK_OVERLAP)
vector_store = VectorStoreManager()

_ingestion_worker: Optional[DurableIngestionWorker] = None
_ingestion_worker_lock = asyncio.Lock()


async def _load_file_lifecycle_context(db: Any, *, file_id: UUID) -> Dict[str, Any]:
    file_obj = await crud_file.get(db, id=file_id)
    if file_obj is None:
        return {
            "file_id": str(file_id),
            "user_id": None,
            "chat_ids": [],
            "chat_id": None,
            "filename": None,
            "upload_id": None,
            "storage_key": None,
        }

    chats_query = select(ConversationFile.chat_id).where(ConversationFile.file_id == file_id)
    chats_result = await db.execute(chats_query)
    chat_ids = [str(chat_id) for chat_id in chats_result.scalars().all()]

    raw_custom_meta = getattr(file_obj, "custom_metadata", None)
    custom_meta = raw_custom_meta if isinstance(raw_custom_meta, dict) else {}
    upload_id_raw = custom_meta.get("upload_id")
    upload_id = str(upload_id_raw).strip() if upload_id_raw is not None else ""
    storage_key_raw = getattr(file_obj, "storage_key", None)
    storage_key = str(storage_key_raw).strip() if storage_key_raw is not None else ""
    filename_raw = getattr(file_obj, "original_filename", None)
    filename = str(filename_raw).strip() if filename_raw is not None else ""
    return {
        "file_id": str(file_id),
        "user_id": str(file_obj.user_id),
        "chat_ids": chat_ids,
        "chat_id": chat_ids[0] if chat_ids else None,
        "filename": filename or None,
        "upload_id": upload_id or None,
        "storage_key": storage_key or None,
    }


def _emit_file_lifecycle(
    event: str,
    *,
    context: Optional[Dict[str, Any]],
    file_id: UUID,
    processing_id: Optional[UUID],
    pipeline_version: Optional[str],
    status_value: Optional[str],
    embedding_provider: Optional[str] = None,
    embedding_model: Optional[str] = None,
    embedding_dimension_expected: Optional[int] = None,
    embedding_dimension_actual: Optional[int] = None,
    embedding_dimension_source: Optional[str] = None,
    collection: Optional[str] = None,
    namespace: Optional[str] = None,
    processing_stage: Optional[str] = None,
    document_ids: Optional[List[str]] = None,
    is_active_processing: Optional[bool] = None,
    error: Optional[str] = None,
    error_code: Optional[str] = None,
    extras: Optional[Dict[str, Any]] = None,
) -> None:
    resolved = context or {}
    log_file_lifecycle_event(
        logger,
        event,
        user_id=resolved.get("user_id"),
        chat_id=resolved.get("chat_id"),
        conversation_id=resolved.get("chat_id"),
        chat_ids=resolved.get("chat_ids"),
        conversation_ids=resolved.get("chat_ids"),
        file_id=file_id,
        filename=resolved.get("filename"),
        upload_id=resolved.get("upload_id"),
        processing_id=processing_id,
        document_ids=document_ids,
        pipeline_version=pipeline_version,
        processing_stage=processing_stage,
        status=status_value,
        storage_key=resolved.get("storage_key"),
        is_active_processing=is_active_processing,
        embedding_provider=embedding_provider,
        embedding_model=embedding_model,
        embedding_dimension_expected=embedding_dimension_expected,
        embedding_dimension_actual=embedding_dimension_actual,
        embedding_dimension_source=embedding_dimension_source,
        collection=collection,
        namespace=namespace,
        error=error,
        error_code=error_code,
        extras=extras,
    )


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
    processing_id: Optional[UUID] = None,
    progress: Dict[str, Any],
    embedding_mode: str,
    embedding_model: str,
    error_message: Optional[str],
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> str:
    lifecycle_context = await _load_file_lifecycle_context(db, file_id=file_id)
    metadata = extra_metadata if isinstance(extra_metadata, dict) else {}
    actual_dimension: Optional[int] = None
    if metadata.get("embedding_dimension") is not None:
        try:
            actual_dimension = int(metadata.get("embedding_dimension"))
        except Exception:
            actual_dimension = None

    final_status = await finalize_ingestion_pipeline(
        db=db,
        file_id=file_id,
        processing_id=processing_id,
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
    finalized_event = (
        "file_ready"
        if final_status in {"ready", "completed", "partial_success", "partial_failed"}
        else "processing_failed"
    )
    _emit_file_lifecycle(
        finalized_event,
        context=lifecycle_context,
        file_id=file_id,
        processing_id=processing_id,
        pipeline_version=str(progress.get("pipeline_version") or ""),
        status_value=final_status,
        embedding_provider=embedding_mode,
        embedding_model=embedding_model,
        embedding_dimension_actual=actual_dimension,
        collection=(str(metadata.get("collection")) if metadata.get("collection") is not None else None),
        namespace=(str(metadata.get("namespace")) if metadata.get("namespace") is not None else None),
        processing_stage=str(progress.get("stage") or ""),
        is_active_processing=(final_status in {"ready", "completed", "partial_success", "partial_failed"}),
        error=error_message,
        error_code=(str(progress.get("failure_code") or "") or None),
        extras={
            "chunks_expected": int(progress.get("total_chunks_expected", 0) or 0),
            "chunks_processed": int(progress.get("chunks_processed", 0) or 0),
            "chunks_indexed": int(progress.get("chunks_indexed", 0) or 0),
            "chunks_failed": int(progress.get("chunks_failed", 0) or 0),
        },
    )
    return final_status


def _normalize_embedding_mode(mode: str) -> str:
    normalized = (mode or "local").strip().lower()
    if normalized == "corporate":
        return "aihub"
    if normalized == "ollama":
        return "local"
    return normalized or "local"


def _provider_source_for_embedding_mode(mode: str) -> str:
    normalized = _normalize_embedding_mode(mode)
    if normalized == "aihub":
        return "aihub"
    if normalized == "openai":
        return "openai"
    return "ollama"


def _resolve_embedding_model(
    mode: str,
    override: Optional[str],
) -> tuple[str, str, str, str]:
    provider_source = _provider_source_for_embedding_mode(mode)
    decision = llm_manager.provider_registry.resolve_embedding_model_decision(provider_source, override)
    logger.info(
        (
            "Embedding resolution: mode=%s provider=%s requested_override=%s resolved_model=%s "
            "resolution_source=%s reason=%s"
        ),
        mode,
        provider_source,
        override,
        decision.resolved_model,
        decision.source,
        decision.reason,
    )
    return provider_source, decision.resolved_model, decision.source, decision.reason


async def _resolve_runtime_embedding_model(
    mode: str,
    override: Optional[str],
) -> tuple[str, str, str, str]:
    provider_source, resolved_model, resolution_source, resolution_reason = _resolve_embedding_model(mode, override)

    # Keep explicit valid overrides unchanged; caller asked for this exact model.
    if resolution_source == "override":
        return provider_source, resolved_model, resolution_source, resolution_reason

    # Provider-aware capability check for local/Ollama defaults.
    if provider_source != "ollama":
        return provider_source, resolved_model, resolution_source, resolution_reason

    try:
        available_models = await llm_manager.get_available_models(source=provider_source)
    except Exception:
        available_models = []

    if not available_models or resolved_model in available_models:
        return provider_source, resolved_model, resolution_source, resolution_reason

    candidate = llm_manager.provider_registry.model_resolver.pick_first_embedding_candidate(
        provider=provider_source,
        available_models=available_models,
        preferred=resolved_model,
    )
    if candidate and candidate != resolved_model:
        logger.warning(
            (
                "Embedding runtime fallback within provider: mode=%s provider=%s requested_override=%s "
                "resolved_default=%s runtime_model=%s reason=%s"
            ),
            mode,
            provider_source,
            override,
            resolved_model,
            candidate,
            "default_not_available",
        )
        return provider_source, candidate, "provider_capability", f"default_unavailable:{resolved_model}"

    return provider_source, resolved_model, resolution_source, resolution_reason


async def _preflight_validate_embedding(
    *,
    embedding_mode: str,
    requested_embedding_model: Optional[str],
    embedding_model: str,
    resolution_source: str,
    resolution_reason: str,
) -> None:
    if not bool(getattr(settings, "EMBEDDING_PREFLIGHT_VALIDATE", True)):
        return

    provider_source = _provider_source_for_embedding_mode(embedding_mode)
    logger.info(
        (
            "Embedding preflight check: provider=%s mode=%s requested_model=%s resolved_model=%s "
            "resolution_source=%s reason=%s"
        ),
        provider_source,
        embedding_mode,
        requested_embedding_model,
        embedding_model,
        resolution_source,
        resolution_reason,
    )
    try:
        probe_embedding = await llm_manager.generate_embedding(
            text="embedding preflight probe",
            model_source=provider_source,
            model_name=embedding_model,
        )
    except ProviderTransientError as exc:
        logger.warning(
            "Embedding preflight transient error, skipping strict failure: provider=%s model=%s err=%s",
            provider_source,
            embedding_model,
            exc,
        )
        return
    except ProviderAuthError as exc:
        raise ValueError(
            f"Embedding auth failed: provider={provider_source} model={embedding_model} "
            f"status={getattr(exc, 'status_code', None) or 'n/a'}"
        ) from exc
    except ProviderConfigError as exc:
        raise ValueError(
            f"Embedding model unavailable/config error: provider={provider_source} model={embedding_model} "
            f"status={getattr(exc, 'status_code', None) or 'n/a'}"
        ) from exc

    if not probe_embedding:
        raise ValueError(
            f"Embedding preflight returned empty vector: provider={provider_source} model={embedding_model}"
        )


def _build_ingestion_idempotency_key(
    *,
    file_id: UUID,
    processing_id: Optional[UUID],
    file_path: Path,
    embedding_mode: str,
    embedding_model: str,
) -> str:
    raw = f"{file_id}|{processing_id}|{str(file_path)}|{embedding_mode}|{embedding_model}"
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
    processing_uuid: Optional[UUID] = None
    if payload.processing_id:
        try:
            processing_uuid = UUID(payload.processing_id)
        except Exception:
            logger.error("Invalid ingestion payload processing_id=%s", payload.processing_id)
            return False, False
    rid_token = None
    request_id_value = str(payload.request_id or "").strip() if payload.request_id is not None else ""
    if request_id_value:
        rid_token = request_id_ctx.set(request_id_value)
    try:
        return await _process_file(
            file_id=file_id,
            processing_id=processing_uuid,
            file_path=Path(payload.file_path),
            embedding_mode=payload.embedding_mode,
            embedding_model=payload.embedding_model,
            pipeline_version=payload.pipeline_version,
            parser_version=payload.parser_version,
            artifact_version=payload.artifact_version,
            chunking_strategy=payload.chunking_strategy,
            retrieval_profile=payload.retrieval_profile,
        )
    finally:
        if rid_token is not None:
            request_id_ctx.reset(rid_token)


async def _ensure_worker_started() -> DurableIngestionWorker:
    global _ingestion_worker
    if _ingestion_worker is not None and bool(_ingestion_worker.snapshot().get("worker_running")):
        return _ingestion_worker

    async with _ingestion_worker_lock:
        if _ingestion_worker is not None and bool(_ingestion_worker.snapshot().get("worker_running")):
            return _ingestion_worker

        queue_path = settings.get_ingestion_queue_path()
        adapter = SqliteIngestionQueueAdapter(queue_path)
        worker = DurableIngestionWorker(
            queue=adapter,
            processor=_process_payload_for_worker,
            config=_build_worker_config(),
        )
        await worker.start()
        _ingestion_worker = worker
        logger.info("Durable ingestion runtime started: queue=%s", queue_path)
        return worker


async def process_file_async(
    file_id: UUID,
    file_path: Path,
    embedding_mode: str = "local",
    embedding_model: Optional[str] = None,
    processing_id: Optional[UUID] = None,
    pipeline_version: Optional[str] = None,
    parser_version: Optional[str] = None,
    artifact_version: Optional[str] = None,
    chunking_strategy: Optional[str] = None,
    retrieval_profile: Optional[str] = None,
) -> UUID:
    mode = _normalize_embedding_mode(embedding_mode)
    provider_source, resolved, resolution_source, resolution_reason = await _resolve_runtime_embedding_model(
        mode,
        embedding_model,
    )
    await _preflight_validate_embedding(
        embedding_mode=mode,
        requested_embedding_model=embedding_model,
        embedding_model=resolved,
        resolution_source=resolution_source,
        resolution_reason=resolution_reason,
    )
    logger.info(
        (
            "Scheduling file processing: file_id=%s mode=%s provider=%s requested_override=%s "
            "resolved_model=%s resolution_source=%s path=%s"
        ),
        file_id,
        mode,
        provider_source,
        embedding_model,
        resolved,
        resolution_source,
        file_path,
    )
    resolved_pipeline_version = str(pipeline_version or settings.FILE_PIPELINE_VERSION_DEFAULT)
    resolved_parser_version = str(parser_version or settings.FILE_PARSER_VERSION_DEFAULT)
    resolved_artifact_version = str(artifact_version or settings.FILE_ARTIFACT_VERSION_DEFAULT)
    resolved_chunking_strategy = str(chunking_strategy or settings.FILE_CHUNKING_STRATEGY_DEFAULT)
    resolved_retrieval_profile = str(retrieval_profile or settings.FILE_RETRIEVAL_PROFILE_DEFAULT)
    expected_dim_decision = llm_manager.provider_registry.resolve_embedding_dimension_decision(provider_source, resolved)
    expected_dimension = int(expected_dim_decision.dimension) if int(expected_dim_decision.dimension or 0) > 0 else None

    target_processing_id = processing_id
    lifecycle_context: Dict[str, Any] = {}
    async with AsyncSessionLocal() as db:
        try:
            if target_processing_id is None:
                profile = await crud_file.create_processing_profile(
                    db,
                    file_id=file_id,
                    pipeline_version=resolved_pipeline_version,
                    parser_version=resolved_parser_version,
                    artifact_version=resolved_artifact_version,
                    embedding_provider=mode,
                    embedding_model=resolved,
                    embedding_dimension=None,
                    chunking_strategy=resolved_chunking_strategy,
                    retrieval_profile=resolved_retrieval_profile,
                    status="queued",
                    is_active=False,
                )
                target_processing_id = profile.id
            await crud_file.update_processing_status(
                db,
                file_id=file_id,
                processing_id=target_processing_id,
                status="queued",
                embedding_model=f"{mode}:{resolved}",
                metadata_patch={
                    "ingestion_progress": {
                        "status": "queued",
                        "stage": "queued",
                        "started_at": _utc_now_iso(),
                        "finished_at": None,
                        "total_chunks_expected": 0,
                        "chunks_processed": 0,
                        "chunks_failed": 0,
                        "chunks_indexed": 0,
                        "vector_upserts_expected": 0,
                        "vector_upserts_actual": 0,
                        "processing_id": str(target_processing_id) if target_processing_id else None,
                        "pipeline_version": resolved_pipeline_version,
                        "parser_version": resolved_parser_version,
                        "artifact_version": resolved_artifact_version,
                        "chunking_strategy": resolved_chunking_strategy,
                        "retrieval_profile": resolved_retrieval_profile,
                    },
                    "error": None,
                    "pipeline_version": resolved_pipeline_version,
                    "parser_version": resolved_parser_version,
                    "artifact_version": resolved_artifact_version,
                    "chunking_strategy": resolved_chunking_strategy,
                    "retrieval_profile": resolved_retrieval_profile,
                },
            )
            lifecycle_context = await _load_file_lifecycle_context(db, file_id=file_id)
        except Exception:
            logger.warning("Failed to pre-mark file queued: file_id=%s", file_id, exc_info=True)
    worker = await _ensure_worker_started()
    job_payload = IngestionJobPayload(
        file_id=str(file_id),
        file_path=str(file_path),
        embedding_mode=mode,
        embedding_model=resolved,
        request_id=(request_id_ctx.get() or None),
        processing_id=(str(target_processing_id) if target_processing_id is not None else None),
        pipeline_version=resolved_pipeline_version,
        parser_version=resolved_parser_version,
        artifact_version=resolved_artifact_version,
        chunking_strategy=resolved_chunking_strategy,
        retrieval_profile=resolved_retrieval_profile,
    )
    result = await worker.enqueue(
        payload=job_payload,
        idempotency_key=_build_ingestion_idempotency_key(
            file_id=file_id,
            processing_id=target_processing_id,
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
    if target_processing_id is None:
        raise RuntimeError("Failed to create or resolve processing profile id")
    _emit_file_lifecycle(
        "processing_created",
        context=lifecycle_context,
        file_id=file_id,
        processing_id=target_processing_id,
        pipeline_version=resolved_pipeline_version,
        status_value="queued",
        embedding_provider=mode,
        embedding_model=resolved,
        embedding_dimension_expected=expected_dimension,
        embedding_dimension_source=f"{expected_dim_decision.source}:{expected_dim_decision.reason}",
        processing_stage="queued",
        is_active_processing=False,
        extras={
            "job_id": result.job_id,
            "queue_status": result.status,
            "deduplicated": bool(result.deduplicated),
            "resolution_source": resolution_source,
            "resolution_reason": resolution_reason,
        },
    )
    return target_processing_id


async def process_file_background(
    file_id: UUID,
    file_path: Path,
    embedding_mode: str = "local",
    embedding_model: Optional[str] = None,
    processing_id: Optional[UUID] = None,
    pipeline_version: Optional[str] = None,
    parser_version: Optional[str] = None,
    artifact_version: Optional[str] = None,
    chunking_strategy: Optional[str] = None,
    retrieval_profile: Optional[str] = None,
) -> None:
    mode = _normalize_embedding_mode(embedding_mode)
    provider_source, resolved, resolution_source, resolution_reason = await _resolve_runtime_embedding_model(
        mode,
        embedding_model,
    )
    await _preflight_validate_embedding(
        embedding_mode=mode,
        requested_embedding_model=embedding_model,
        embedding_model=resolved,
        resolution_source=resolution_source,
        resolution_reason=resolution_reason,
    )
    logger.info(
        (
            "Running file processing (await): file_id=%s mode=%s provider=%s requested_override=%s "
            "resolved_model=%s resolution_source=%s path=%s"
        ),
        file_id,
        mode,
        provider_source,
        embedding_model,
        resolved,
        resolution_source,
        file_path,
    )
    await _process_file(
        file_id=file_id,
        processing_id=processing_id,
        file_path=file_path,
        embedding_mode=mode,
        embedding_model=resolved,
        pipeline_version=pipeline_version,
        parser_version=parser_version,
        artifact_version=artifact_version,
        chunking_strategy=chunking_strategy,
        retrieval_profile=retrieval_profile,
    )


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
            .where(
                FileModel.status.in_(["uploaded", "processing"])
            )
            .options(selectinload(FileModel.processing_profiles))
            .order_by(FileModel.created_at.asc())
            .limit(limit)
        )
        res = await db.execute(stmt)
        files = list(res.scalars().all())

    for file_obj in files:
        file_path = Path(file_obj.storage_path)
        if not file_path.exists():
            logger.warning("Skip recovery: file missing on disk file_id=%s path=%s", file_obj.id, file_path)
            continue

        mode, model = _parse_embedding_model_meta(getattr(file_obj, "embedding_model", None))
        active_processing: Optional[FileProcessingProfile] = None
        processing_profiles = list(getattr(file_obj, "processing_profiles", []) or [])
        if processing_profiles:
            active = [p for p in processing_profiles if bool(getattr(p, "is_active", False))]
            if active:
                active.sort(key=lambda p: getattr(p, "created_at", datetime.min), reverse=True)
                active_processing = active[0]
            else:
                processing_profiles.sort(key=lambda p: getattr(p, "created_at", datetime.min), reverse=True)
                active_processing = processing_profiles[0]

        try:
            processing_id = await process_file_async(
                file_id=file_obj.id,
                file_path=file_path,
                embedding_mode=mode,
                embedding_model=model,
                processing_id=(active_processing.id if active_processing is not None else None),
                pipeline_version=(active_processing.pipeline_version if active_processing is not None else None),
                parser_version=(active_processing.parser_version if active_processing is not None else None),
                artifact_version=(active_processing.artifact_version if active_processing is not None else None),
                chunking_strategy=(active_processing.chunking_strategy if active_processing is not None else None),
                retrieval_profile=(active_processing.retrieval_profile if active_processing is not None else None),
            )
            _emit_file_lifecycle(
                "processing_recovered_after_restart",
                context={
                    "user_id": str(file_obj.user_id),
                    "chat_id": None,
                    "chat_ids": [],
                    "filename": str(file_obj.original_filename),
                    "upload_id": (
                        str(file_obj.custom_metadata.get("upload_id"))
                        if isinstance(file_obj.custom_metadata, dict) and file_obj.custom_metadata.get("upload_id") is not None
                        else None
                    ),
                    "storage_key": str(file_obj.storage_key),
                },
                file_id=file_obj.id,
                processing_id=processing_id,
                pipeline_version=(active_processing.pipeline_version if active_processing is not None else None),
                status_value="queued",
                embedding_provider=mode,
                embedding_model=model,
                processing_stage="queued",
                is_active_processing=bool(active_processing and active_processing.is_active),
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
    processing_id: Optional[UUID] = None,
    pipeline_version: Optional[str] = None,
    parser_version: Optional[str] = None,
    artifact_version: Optional[str] = None,
    chunking_strategy: Optional[str] = None,
    retrieval_profile: Optional[str] = None,
) -> Tuple[bool, bool]:
    return await process_file_pipeline(
        file_id=file_id,
        processing_id=processing_id,
        file_path=file_path,
        embedding_mode=embedding_mode,
        embedding_model=embedding_model,
        pipeline_version=pipeline_version,
        parser_version=parser_version,
        artifact_version=artifact_version,
        chunking_strategy=chunking_strategy,
        retrieval_profile=retrieval_profile,
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
        persist_derived_artifacts_fn=persist_derived_artifacts,
        file_id_ctx_var=file_id_ctx,
        settings_obj=settings,
        observe_ms_fn=observe_ms,
        logger_obj=logger,
    )

"""Persistent user file API."""

from __future__ import annotations

import hashlib
import logging
import mimetypes
import re
import shutil
from pathlib import Path
from typing import Dict, List, Optional
from uuid import UUID, uuid4

import aiofiles
from fastapi import APIRouter, Body, Depends, File as FastAPIFile, Form, HTTPException, Query, UploadFile, status
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_current_user
from app.core.config import settings
from app.crud import crud_file
from app.db.models import Conversation, User
from app.db.models.conversation_file import ConversationFile
from app.db.models.file import File as FileModel
from app.db.models.file_processing import FileProcessingProfile
from app.db.session import get_db
from app.observability.file_lifecycle import log_file_lifecycle_event
from app.rag.vector_store import VectorStoreManager
from app.schemas.file import (
    FileAttachRequest,
    FileAttachResponse,
    FileDeleteResponse,
    FileDetachResponse,
    FileDetachRequest,
    FileInfo,
    FileProcessingProfileInfo,
    FileProcessingStatus,
    FileQuotaInfo,
    FileReprocessRequest,
    FileReprocessResponse,
    FileUploadResponse,
)
from app.services.file import process_file_async
from app.services.tabular.storage_adapter import cleanup_tabular_artifacts_for_file
from app.utils.time import ensure_utc_datetime

logger = logging.getLogger(__name__)
router = APIRouter()

_filename_strip_re = re.compile(r"[^A-Za-z0-9\u0400-\u04FF._() \-\[\]]+")
_ATTACHABLE_FILE_STATUSES = {"uploaded", "processing", "ready"}

def _utcnow():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(tzinfo=None)


def _safe_filename(name: str) -> str:
    name = (name or "").strip().replace("\x00", "")
    name = name.replace("/", "_").replace("\\", "_")
    name = _filename_strip_re.sub("_", name)
    return name or f"file_{uuid4().hex}"


def _detect_mime_type(upload: UploadFile, original_filename: str) -> str:
    content_type = str(upload.content_type or "").strip()
    if content_type:
        return content_type
    guessed, _ = mimetypes.guess_type(original_filename)
    return guessed or "application/octet-stream"


def _storage_paths_for_upload(*, user_id: UUID, file_id: UUID, safe_name: str) -> tuple[str, Path]:
    user_dir = settings.get_raw_files_dir() / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    stored_filename = f"{file_id}_{safe_name}"
    target_path = (user_dir / stored_filename).resolve()
    storage_key = f"raw/{user_id}/{stored_filename}"
    return storage_key, target_path


async def _save_uploadfile_with_limits(
    *,
    upload: UploadFile,
    dst_path: Path,
    max_file_bytes: int,
    quota_used_bytes: int,
    quota_limit_bytes: int,
    chunk_size: int = 1024 * 1024,
) -> int:
    written = 0
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        async with aiofiles.open(dst_path, "wb") as out:
            while True:
                chunk = await upload.read(chunk_size)
                if not chunk:
                    break
                written += len(chunk)
                if written > max_file_bytes:
                    raise HTTPException(
                        status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                        detail=f"File exceeds MAX_FILESIZE_MB={settings.MAX_FILESIZE_MB}",
                    )
                if (quota_used_bytes + written) > quota_limit_bytes:
                    raise HTTPException(
                        status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                        detail=(
                            "User file quota exceeded "
                            f"(used={quota_used_bytes} bytes, limit={quota_limit_bytes} bytes)."
                        ),
                    )
                await out.write(chunk)
    except HTTPException:
        if dst_path.exists():
            dst_path.unlink(missing_ok=True)
        raise
    except Exception as exc:
        if dst_path.exists():
            dst_path.unlink(missing_ok=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to persist upload: {type(exc).__name__}",
        ) from exc
    finally:
        try:
            await upload.seek(0)
        except Exception:
            pass

    return written


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _to_processing_info(profile: FileProcessingProfile) -> FileProcessingProfileInfo:
    return FileProcessingProfileInfo(
        processing_id=profile.id,
        file_id=profile.file_id,
        pipeline_version=profile.pipeline_version,
        parser_version=profile.parser_version,
        artifact_version=profile.artifact_version,
        embedding_provider=profile.embedding_provider,
        embedding_model=profile.embedding_model,
        embedding_dimension=profile.embedding_dimension,
        chunking_strategy=profile.chunking_strategy,
        retrieval_profile=profile.retrieval_profile,
        status=profile.status,
        started_at=ensure_utc_datetime(profile.started_at),
        finished_at=ensure_utc_datetime(profile.finished_at),
        error_message=profile.error_message,
        is_active=bool(profile.is_active),
        ingestion_progress=(
            dict(profile.ingestion_progress) if isinstance(profile.ingestion_progress, dict) else {}
        ),
        artifact_metadata=(
            dict(profile.artifact_metadata) if isinstance(profile.artifact_metadata, dict) else {}
        ),
        created_at=ensure_utc_datetime(profile.created_at),
        updated_at=ensure_utc_datetime(profile.updated_at),
    )


def _to_file_info(
    file_obj: FileModel,
    *,
    chat_ids: List[UUID],
    active_processing: Optional[FileProcessingProfile],
) -> FileInfo:
    return FileInfo(
        file_id=file_obj.id,
        owner_user_id=file_obj.user_id,
        original_filename=file_obj.original_filename,
        stored_filename=file_obj.stored_filename,
        storage_key=file_obj.storage_key,
        storage_path=file_obj.storage_path,
        mime_type=file_obj.mime_type,
        extension=file_obj.extension,
        size_bytes=int(file_obj.size_bytes or 0),
        checksum=file_obj.checksum,
        visibility=file_obj.visibility,
        status=file_obj.status,
        source_kind=file_obj.source_kind,
        created_at=ensure_utc_datetime(file_obj.created_at),
        updated_at=ensure_utc_datetime(file_obj.updated_at),
        deleted_at=ensure_utc_datetime(file_obj.deleted_at),
        chat_ids=list(chat_ids),
        active_processing_id=(active_processing.id if active_processing else None),
        active_processing_status=(active_processing.status if active_processing else None),
        chunks_count=int(file_obj.chunks_count or 0),
    )


async def _get_user_file_or_404(db: AsyncSession, *, user_id: UUID, file_id: UUID) -> FileModel:
    file_obj = await crud_file.get_user_file(db, file_id=file_id, user_id=user_id, include_deleted=False)
    if not file_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")
    return file_obj


async def _get_user_chat_or_404(db: AsyncSession, *, user_id: UUID, chat_id: UUID) -> Conversation:
    query = select(Conversation).where(and_(Conversation.id == chat_id, Conversation.user_id == user_id))
    result = await db.execute(query)
    chat = result.scalar_one_or_none()
    if chat is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Chat not found")
    return chat


def _raise_preflight_validation_error(exc: ValueError) -> None:
    detail = str(exc).strip() or "Embedding preflight validation failed"
    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail) from exc


def _ensure_file_attachable(file_obj: FileModel) -> None:
    raw_status = getattr(file_obj, "status", None)
    if raw_status is None:
        # Defensive fallback for mocked objects in tests; real DB rows always carry status.
        return
    current_status = str(raw_status or "").strip().lower()
    if current_status in _ATTACHABLE_FILE_STATUSES:
        return
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=(
            "File cannot be attached in current state "
            f"(status={current_status or 'unknown'}; allowed=uploaded,processing,ready)."
        ),
    )


async def _chat_ids_by_file(db: AsyncSession, file_ids: List[UUID]) -> Dict[UUID, List[UUID]]:
    if not file_ids:
        return {}
    rows = await db.execute(
        select(ConversationFile.file_id, ConversationFile.chat_id).where(ConversationFile.file_id.in_(file_ids))
    )
    mapping: Dict[UUID, List[UUID]] = {file_id: [] for file_id in file_ids}
    for file_id, chat_id in rows.all():
        mapping.setdefault(file_id, []).append(chat_id)
    return mapping


def _extract_upload_id(file_obj: Optional[FileModel]) -> Optional[str]:
    custom_meta = getattr(file_obj, "custom_metadata", None)
    if not isinstance(custom_meta, dict):
        return None
    raw = custom_meta.get("upload_id")
    value = str(raw).strip() if raw is not None else ""
    return value or None


def _parse_embedding_identity(raw_value: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    raw = str(raw_value or "").strip()
    if not raw:
        return None, None
    if ":" not in raw:
        return None, raw
    provider_raw, model_raw = raw.split(":", 1)
    provider = str(provider_raw or "").strip().lower() or None
    if provider == "corporate":
        provider = "aihub"
    if provider == "ollama":
        provider = "local"
    model = str(model_raw or "").strip() or None
    return provider, model


async def _try_get_active_processing(
    db: AsyncSession,
    *,
    file_id: UUID,
    user_id: UUID,
) -> Optional[FileProcessingProfile]:
    getter = getattr(crud_file, "get_active_processing", None)
    if not callable(getter):
        return None
    try:
        return await getter(db, file_id=file_id, user_id=user_id)
    except Exception:
        logger.warning("Failed to resolve active processing for file lifecycle log file_id=%s", file_id, exc_info=True)
        return None


def _log_file_lifecycle_event(
    event: str,
    *,
    uid: Optional[UUID] = None,
    chat_id: Optional[UUID] = None,
    file_id: Optional[UUID] = None,
    upload_id: Optional[str] = None,
    processing_id: Optional[UUID] = None,
    pipeline_version: Optional[str] = None,
    embedding_provider: Optional[str] = None,
    embedding_model: Optional[str] = None,
    embedding_dimension: Optional[int] = None,
    embedding_dimension_expected: Optional[int] = None,
    embedding_dimension_actual: Optional[int] = None,
    storage_key: Optional[str] = None,
    quota_used_bytes: Optional[int] = None,
    quota_limit_bytes: Optional[int] = None,
    status_value: Optional[str] = None,
    is_active_processing: Optional[bool] = None,
    filename: Optional[str] = None,
    chat_ids: Optional[List[UUID]] = None,
    document_ids: Optional[List[str]] = None,
    processing_stage: Optional[str] = None,
    collection: Optional[str] = None,
    namespace: Optional[str] = None,
    error: Optional[str] = None,
    error_code: Optional[str] = None,
    extras: Optional[Dict[str, object]] = None,
) -> None:
    resolved_actual_dim = embedding_dimension_actual
    if resolved_actual_dim is None:
        resolved_actual_dim = embedding_dimension

    log_file_lifecycle_event(
        logger,
        event,
        user_id=uid,
        chat_id=chat_id,
        conversation_id=chat_id,
        chat_ids=chat_ids,
        conversation_ids=chat_ids,
        file_id=file_id,
        filename=filename,
        upload_id=upload_id,
        processing_id=processing_id,
        document_ids=document_ids,
        pipeline_version=pipeline_version,
        processing_stage=processing_stage,
        status=status_value,
        storage_key=storage_key,
        quota_used_bytes=quota_used_bytes,
        quota_limit_bytes=quota_limit_bytes,
        is_active_processing=is_active_processing,
        embedding_provider=embedding_provider,
        embedding_model=embedding_model,
        embedding_dimension_expected=embedding_dimension_expected,
        embedding_dimension_actual=resolved_actual_dim,
        collection=collection,
        namespace=namespace,
        error=error,
        error_code=error_code,
        extras=extras,
    )


@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(
    file: UploadFile = FastAPIFile(...),
    source_kind: str = Form("upload"),
    chat_id: Optional[UUID] = Form(None),
    auto_process: bool = Form(True),
    embedding_provider: str = Form("local"),
    embedding_model: Optional[str] = Form(None),
    pipeline_version: Optional[str] = Form(None),
    parser_version: Optional[str] = Form(None),
    artifact_version: Optional[str] = Form(None),
    chunking_strategy: Optional[str] = Form(None),
    retrieval_profile: Optional[str] = Form(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if not file.filename:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Filename is empty")

    effective_chat_id = chat_id
    if effective_chat_id is not None:
        await _get_user_chat_or_404(db, user_id=current_user.id, chat_id=effective_chat_id)

    original_filename = str(file.filename)
    extension = Path(original_filename).suffix.lower().lstrip(".")
    safe_name = _safe_filename(original_filename)
    file_id = uuid4()
    upload_id = f"upload-{file_id.hex[:12]}"

    quota_used_before = await crud_file.get_user_storage_usage_bytes(db, user_id=current_user.id)
    quota_limit = int(settings.USER_FILE_QUOTA_BYTES)
    max_file_bytes = int(settings.MAX_FILESIZE_MB * 1024 * 1024)
    storage_key, raw_path = _storage_paths_for_upload(user_id=current_user.id, file_id=file_id, safe_name=safe_name)
    written = await _save_uploadfile_with_limits(
        upload=file,
        dst_path=raw_path,
        max_file_bytes=max_file_bytes,
        quota_used_bytes=quota_used_before,
        quota_limit_bytes=quota_limit,
    )
    checksum = _sha256_file(raw_path)
    mime_type = _detect_mime_type(file, original_filename)

    content_preview = None
    if extension in {"txt", "md", "json"}:
        try:
            async with aiofiles.open(raw_path, "rb") as preview_stream:
                head = await preview_stream.read(5000)
            content_preview = head.decode("utf-8", errors="ignore")[:500]
        except Exception:
            content_preview = None

    try:
        file_obj = await crud_file.create_file(
            db,
            user_id=current_user.id,
            original_filename=original_filename,
            stored_filename=raw_path.name,
            storage_key=storage_key,
            storage_path=str(raw_path),
            mime_type=mime_type,
            extension=extension,
            size_bytes=written,
            checksum=checksum,
            source_kind=source_kind,
            visibility="private",
            content_preview=content_preview,
            custom_metadata={
                "upload_id": upload_id,
            },
        )
    except Exception as exc:
        raw_path.unlink(missing_ok=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save file metadata: {type(exc).__name__}",
        ) from exc

    if effective_chat_id is not None:
        await crud_file.add_file_to_conversation(
            db,
            file_id=file_obj.id,
            conversation_id=effective_chat_id,
            attached_by_user_id=current_user.id,
        )

    scheduled_processing: Optional[UUID] = None
    supported_for_ingestion = settings.is_file_supported(original_filename)
    if auto_process and supported_for_ingestion:
        try:
            scheduled_processing = await process_file_async(
                file_id=file_obj.id,
                file_path=Path(file_obj.storage_path),
                embedding_mode=embedding_provider,
                embedding_model=embedding_model,
                pipeline_version=pipeline_version,
                parser_version=parser_version,
                artifact_version=artifact_version,
                chunking_strategy=chunking_strategy,
                retrieval_profile=retrieval_profile,
            )
        except ValueError as exc:
            _raise_preflight_validation_error(exc)
    elif auto_process and not supported_for_ingestion:
        _log_file_lifecycle_event(
            "processing_skipped_unsupported_extension",
            uid=current_user.id,
            chat_id=effective_chat_id,
            file_id=file_obj.id,
            filename=file_obj.original_filename,
            upload_id=upload_id,
            processing_id=None,
            storage_key=file_obj.storage_key,
            quota_used_bytes=quota_used_before + written,
            quota_limit_bytes=quota_limit,
            status_value=file_obj.status,
        )

    chat_ids_map = await _chat_ids_by_file(db, [file_obj.id])
    active_processing = await _try_get_active_processing(db, file_id=file_obj.id, user_id=current_user.id)
    refreshed = await _get_user_file_or_404(db, user_id=current_user.id, file_id=file_obj.id)
    quota_used_after = await crud_file.get_user_storage_usage_bytes(db, user_id=current_user.id)
    chat_ids = chat_ids_map.get(refreshed.id, [])

    active_embedding_provider: Optional[str] = None
    active_embedding_model: Optional[str] = None
    active_embedding_dimension: Optional[int] = None
    active_pipeline_version: Optional[str] = None
    active_processing_id: Optional[UUID] = None
    if active_processing is not None:
        active_embedding_provider = str(getattr(active_processing, "embedding_provider", "") or "").strip() or None
        active_embedding_model = str(getattr(active_processing, "embedding_model", "") or "").strip() or None
        active_embedding_dimension = (
            int(active_processing.embedding_dimension)
            if getattr(active_processing, "embedding_dimension", None) is not None
            else None
        )
        active_pipeline_version = str(getattr(active_processing, "pipeline_version", "") or "").strip() or None
        active_processing_id = active_processing.id
    else:
        parsed_provider, parsed_model = _parse_embedding_identity(getattr(refreshed, "embedding_model", None))
        active_embedding_provider = parsed_provider
        active_embedding_model = parsed_model

    _log_file_lifecycle_event(
        "file_uploaded",
        uid=current_user.id,
        chat_id=effective_chat_id,
        chat_ids=chat_ids,
        file_id=refreshed.id,
        filename=refreshed.original_filename,
        upload_id=upload_id,
        processing_id=(active_processing_id or scheduled_processing),
        pipeline_version=(active_pipeline_version or pipeline_version or settings.FILE_PIPELINE_VERSION_DEFAULT),
        embedding_provider=active_embedding_provider,
        embedding_model=active_embedding_model,
        embedding_dimension_actual=active_embedding_dimension,
        storage_key=refreshed.storage_key,
        quota_used_bytes=quota_used_after,
        quota_limit_bytes=quota_limit,
        status_value=refreshed.status,
        is_active_processing=(bool(active_processing and active_processing.is_active)),
    )

    if effective_chat_id is not None:
        _log_file_lifecycle_event(
            "file_attached_to_chat",
            uid=current_user.id,
            chat_id=effective_chat_id,
            chat_ids=chat_ids,
            file_id=refreshed.id,
            filename=refreshed.original_filename,
            upload_id=upload_id,
            processing_id=(active_processing_id or scheduled_processing),
            pipeline_version=(active_pipeline_version or pipeline_version or settings.FILE_PIPELINE_VERSION_DEFAULT),
            embedding_provider=active_embedding_provider,
            embedding_model=active_embedding_model,
            embedding_dimension_actual=active_embedding_dimension,
            storage_key=refreshed.storage_key,
            status_value=refreshed.status,
            is_active_processing=(bool(active_processing and active_processing.is_active)),
            extras={"embedding_details_available": bool(active_processing is not None)},
        )

    return FileUploadResponse(
        file=_to_file_info(
            refreshed,
            chat_ids=chat_ids_map.get(refreshed.id, []),
            active_processing=active_processing,
        ),
        quota=FileQuotaInfo(quota_used_bytes=quota_used_after, quota_limit_bytes=quota_limit),
    )


@router.get("/", response_model=List[FileInfo])
async def list_files(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    files = await crud_file.get_user_files(db, user_id=current_user.id, skip=skip, limit=limit)
    chat_ids_map = await _chat_ids_by_file(db, [f.id for f in files])
    output: List[FileInfo] = []
    for item in files:
        active = await crud_file.get_active_processing(db, file_id=item.id, user_id=current_user.id)
        output.append(
            _to_file_info(
                item,
                chat_ids=chat_ids_map.get(item.id, []),
                active_processing=active,
            )
        )
    return output


@router.get("/quota", response_model=FileQuotaInfo)
async def get_my_quota(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    quota_used = await crud_file.get_user_storage_usage_bytes(db, user_id=current_user.id)
    return FileQuotaInfo(
        quota_used_bytes=int(quota_used),
        quota_limit_bytes=int(settings.USER_FILE_QUOTA_BYTES),
    )


@router.get("/processed", response_model=List[FileInfo])
async def list_ready_files(
    chat_id: Optional[UUID] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    target_chat = chat_id
    if target_chat is not None:
        files = await crud_file.get_conversation_ready_files(
            db,
            conversation_id=target_chat,
            user_id=current_user.id,
        )
    else:
        files = await crud_file.get_processed_files(db, user_id=current_user.id)
    chat_ids_map = await _chat_ids_by_file(db, [f.id for f in files])
    response: List[FileInfo] = []
    for item in files:
        active = await crud_file.get_active_processing(db, file_id=item.id, user_id=current_user.id)
        response.append(_to_file_info(item, chat_ids=chat_ids_map.get(item.id, []), active_processing=active))
    return response


@router.get("/{file_id}", response_model=FileInfo)
async def get_file(
    file_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    file_obj = await _get_user_file_or_404(db, user_id=current_user.id, file_id=file_id)
    chat_ids_map = await _chat_ids_by_file(db, [file_id])
    active_processing = await crud_file.get_active_processing(db, file_id=file_id, user_id=current_user.id)
    return _to_file_info(
        file_obj,
        chat_ids=chat_ids_map.get(file_id, []),
        active_processing=active_processing,
    )


@router.get("/{file_id}/status", response_model=FileProcessingStatus)
async def get_file_status(
    file_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    file_obj = await _get_user_file_or_404(db, user_id=current_user.id, file_id=file_id)
    active = await crud_file.get_active_processing(db, file_id=file_id, user_id=current_user.id)
    progress = (active.ingestion_progress if (active and isinstance(active.ingestion_progress, dict)) else {}) or {}
    error_message = None
    if active and active.error_message:
        error_message = active.error_message
    elif isinstance(file_obj.custom_metadata, dict):
        error_message = file_obj.custom_metadata.get("error")
    return FileProcessingStatus(
        file_id=file_obj.id,
        status=file_obj.status,
        chunks_count=int(file_obj.chunks_count or 0),
        total_chunks_expected=int(progress.get("total_chunks_expected", 0) or 0),
        chunks_processed=int(progress.get("chunks_processed", 0) or 0),
        chunks_failed=int(progress.get("chunks_failed", 0) or 0),
        chunks_indexed=int(progress.get("chunks_indexed", 0) or 0),
        started_at=progress.get("started_at"),
        finished_at=progress.get("finished_at"),
        stage=progress.get("stage"),
        error_message=error_message,
        active_processing_id=(active.id if active else None),
        is_active_processing=bool(active is not None and active.is_active),
    )


@router.post("/{file_id}/attach", response_model=FileAttachResponse)
async def attach_file_to_chat(
    file_id: UUID,
    request: FileAttachRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    await _get_user_chat_or_404(db, user_id=current_user.id, chat_id=request.chat_id)
    file_obj = await _get_user_file_or_404(db, user_id=current_user.id, file_id=file_id)
    _ensure_file_attachable(file_obj)
    link = await crud_file.add_file_to_conversation(
        db,
        file_id=file_id,
        conversation_id=request.chat_id,
        attached_by_user_id=current_user.id,
    )
    active_processing = await _try_get_active_processing(db, file_id=file_id, user_id=current_user.id)
    embedding_provider: Optional[str] = None
    embedding_model: Optional[str] = None
    embedding_dimension: Optional[int] = None
    processing_id: Optional[UUID] = None
    pipeline_version: Optional[str] = None
    if active_processing is not None:
        embedding_provider = str(getattr(active_processing, "embedding_provider", "") or "").strip() or None
        embedding_model = str(getattr(active_processing, "embedding_model", "") or "").strip() or None
        embedding_dimension = (
            int(active_processing.embedding_dimension)
            if getattr(active_processing, "embedding_dimension", None) is not None
            else None
        )
        processing_id = active_processing.id
        pipeline_version = str(getattr(active_processing, "pipeline_version", "") or "").strip() or None
    else:
        embedding_provider, embedding_model = _parse_embedding_identity(getattr(file_obj, "embedding_model", None))

    _log_file_lifecycle_event(
        "file_attached_to_chat",
        uid=current_user.id,
        chat_id=request.chat_id,
        file_id=file_id,
        filename=getattr(file_obj, "original_filename", None),
        upload_id=_extract_upload_id(file_obj),
        processing_id=processing_id,
        pipeline_version=pipeline_version,
        embedding_provider=embedding_provider,
        embedding_model=embedding_model,
        embedding_dimension_actual=embedding_dimension,
        storage_key=getattr(file_obj, "storage_key", None),
        status_value=(str(getattr(file_obj, "status", "") or "").strip() or "attached"),
        is_active_processing=(bool(active_processing and active_processing.is_active)),
        extras={"embedding_details_available": bool(active_processing is not None)},
    )
    return FileAttachResponse(
        status="attached",
        file_id=file_id,
        chat_id=request.chat_id,
        attached_at=link.attached_at,
    )


@router.post("/{file_id}/detach", response_model=FileDetachResponse)
async def detach_file_from_chat(
    file_id: UUID,
    request: FileDetachRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    await _get_user_chat_or_404(db, user_id=current_user.id, chat_id=request.chat_id)
    await _get_user_file_or_404(db, user_id=current_user.id, file_id=file_id)
    deleted = await crud_file.remove_file_from_conversation(
        db,
        file_id=file_id,
        conversation_id=request.chat_id,
    )
    _log_file_lifecycle_event(
        "file_detached_from_chat",
        uid=current_user.id,
        chat_id=request.chat_id,
        file_id=file_id,
        status_value="detached",
    )
    return FileDetachResponse(status="detached", file_id=file_id, chat_id=request.chat_id, removed=deleted)


@router.post("/{file_id}/reprocess", response_model=FileReprocessResponse)
async def reprocess_file(
    file_id: UUID,
    request: FileReprocessRequest = Body(default_factory=FileReprocessRequest),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    file_obj = await _get_user_file_or_404(db, user_id=current_user.id, file_id=file_id)
    raw_path = Path(file_obj.storage_path)
    if not raw_path.exists():
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Raw file is missing on disk")

    try:
        processing_id = await process_file_async(
            file_id=file_obj.id,
            file_path=raw_path,
            embedding_mode=request.embedding_provider,
            embedding_model=request.embedding_model,
            pipeline_version=request.pipeline_version,
            parser_version=request.parser_version,
            artifact_version=request.artifact_version,
            chunking_strategy=request.chunking_strategy,
            retrieval_profile=request.retrieval_profile,
        )
    except ValueError as exc:
        _raise_preflight_validation_error(exc)
    _log_file_lifecycle_event(
        "file_reprocess_scheduled",
        uid=current_user.id,
        file_id=file_obj.id,
        filename=file_obj.original_filename,
        upload_id=_extract_upload_id(file_obj),
        processing_id=processing_id,
        pipeline_version=request.pipeline_version,
        embedding_provider=request.embedding_provider,
        embedding_model=request.embedding_model,
        storage_key=file_obj.storage_key,
        status_value="processing",
    )
    return FileReprocessResponse(status="scheduled", file_id=file_obj.id, processing_id=processing_id)


@router.post("/{file_id}/reindex", response_model=FileReprocessResponse)
async def reindex_file(
    file_id: UUID,
    request: FileReprocessRequest = Body(default_factory=FileReprocessRequest),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    return await reprocess_file(file_id=file_id, request=request, db=db, current_user=current_user)


@router.get("/{file_id}/processing", response_model=List[FileProcessingProfileInfo])
async def list_processing_profiles(
    file_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    await _get_user_file_or_404(db, user_id=current_user.id, file_id=file_id)
    profiles = await crud_file.list_processing_profiles(db, file_id=file_id, user_id=current_user.id)
    return [_to_processing_info(item) for item in profiles]


@router.get("/{file_id}/processing/active", response_model=FileProcessingProfileInfo)
async def get_active_processing(
    file_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    await _get_user_file_or_404(db, user_id=current_user.id, file_id=file_id)
    active = await crud_file.get_active_processing(db, file_id=file_id, user_id=current_user.id)
    if active is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Active processing profile not found")
    return _to_processing_info(active)


@router.get("/{file_id}/debug")
async def file_debug_info(
    file_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    file_obj = await _get_user_file_or_404(db, user_id=current_user.id, file_id=file_id)
    chat_ids_map = await _chat_ids_by_file(db, [file_id])
    active = await crud_file.get_active_processing(db, file_id=file_id, user_id=current_user.id)
    profiles = await crud_file.list_processing_profiles(db, file_id=file_id, user_id=current_user.id)
    return {
        "file": _to_file_info(file_obj, chat_ids=chat_ids_map.get(file_id, []), active_processing=active).model_dump(),
        "active_processing": _to_processing_info(active).model_dump() if active else None,
        "processing_versions": [_to_processing_info(item).model_dump() for item in profiles],
        "runtime": {
            "raw_exists": Path(file_obj.storage_path).exists(),
            "raw_path": file_obj.storage_path,
            "storage_key": file_obj.storage_key,
        },
    }


@router.delete("/{file_id}", response_model=FileDeleteResponse)
async def delete_file(
    file_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    file_obj = await _get_user_file_or_404(db, user_id=current_user.id, file_id=file_id)
    await crud_file.mark_file_deleting(db, file_id=file_obj.id)
    profiles = await crud_file.list_processing_profiles(db, file_id=file_id, user_id=current_user.id)

    _log_file_lifecycle_event(
        "file_delete_started",
        uid=current_user.id,
        file_id=file_obj.id,
        filename=file_obj.original_filename,
        upload_id=_extract_upload_id(file_obj),
        storage_key=file_obj.storage_key,
        status_value="deleting",
    )

    await crud_file.remove_file_from_all_conversations(db, file_id=file_obj.id)

    try:
        VectorStoreManager().delete_by_metadata({"file_id": str(file_obj.id)})
    except Exception:
        logger.warning("Vector cleanup failed for file_id=%s", file_obj.id, exc_info=True)

    for profile in profiles:
        try:
            cleanup_tabular_artifacts_for_file(
                file_id=file_obj.id,
                custom_metadata=(
                    dict(profile.artifact_metadata) if isinstance(profile.artifact_metadata, dict) else {}
                ),
            )
        except Exception:
            logger.warning(
                "Tabular cleanup failed file_id=%s processing_id=%s",
                file_obj.id,
                profile.id,
                exc_info=True,
            )
        profile.is_active = False
        profile.status = "deleted"
        profile.error_message = profile.error_message or "deleted_by_user"
        profile.updated_at = _utcnow()

    raw_path = Path(file_obj.storage_path)
    try:
        if raw_path.exists():
            raw_path.unlink()
    except Exception:
        logger.warning("Failed to remove raw file path=%s", raw_path, exc_info=True)

    file_artifacts_dir = settings.get_file_artifacts_dir() / str(file_obj.id)
    if file_artifacts_dir.exists():
        shutil.rmtree(file_artifacts_dir, ignore_errors=True)

    await db.commit()
    await crud_file.mark_file_deleted(db, file_id=file_obj.id)
    quota_used_after = await crud_file.get_user_storage_usage_bytes(db, user_id=current_user.id)
    quota_limit = int(settings.USER_FILE_QUOTA_BYTES)

    _log_file_lifecycle_event(
        "file_delete_completed",
        uid=current_user.id,
        file_id=file_obj.id,
        filename=file_obj.original_filename,
        upload_id=_extract_upload_id(file_obj),
        storage_key=file_obj.storage_key,
        quota_used_bytes=quota_used_after,
        quota_limit_bytes=quota_limit,
        status_value="deleted",
        is_active_processing=False,
    )

    return FileDeleteResponse(
        status="deleted",
        file_id=file_obj.id,
        quota=FileQuotaInfo(quota_used_bytes=quota_used_after, quota_limit_bytes=quota_limit),
    )

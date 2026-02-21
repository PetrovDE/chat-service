"""
File management endpoints
Эндпоинты для работы с файлами: загрузка, обработка, удаление
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List, Optional
from uuid import UUID, uuid4

import aiofiles
from fastapi import APIRouter, Depends, File as FastAPIFile, Form, HTTPException, Query, UploadFile, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.dependencies import get_current_user
from app.core.config import settings
from app.crud import crud_file
from app.db.models import User
from app.db.models.conversation_file import ConversationFile
from app.db.models.file import File as FileModel
from app.db.session import get_db
from app.schemas.file import FileInfo, FileProcessingStatus, FileUploadResponse

logger = logging.getLogger(__name__)
router = APIRouter()

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

_filename_strip_re = re.compile(r"[^A-Za-z0-9А-Яа-яЁё._() \-\[\]]+")


def _safe_filename(name: str) -> str:
    """Безопасное имя файла (латиница/кириллица/цифры/._-()[] и пробел)."""
    name = (name or "").strip().replace("\x00", "")
    name = name.replace("/", "_").replace("\\", "_")
    name = _filename_strip_re.sub("_", name)
    return name or f"file_{uuid4().hex}"


async def _save_uploadfile_with_limit(
    upload: UploadFile,
    dst_path: Path,
    max_bytes: int,
    chunk_size: int = 1024 * 1024,
) -> int:
    """
    Потоково сохраняем файл и считаем размер.
    Если превысили max_bytes -> удаляем частично сохраненный файл и кидаем 413.
    """
    written = 0
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        async with aiofiles.open(dst_path, "wb") as out:
            while True:
                chunk = await upload.read(chunk_size)
                if not chunk:
                    break
                written += len(chunk)
                if written > max_bytes:
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=f"File too large (max {settings.MAX_FILESIZE_MB}MB)",
                    )
                await out.write(chunk)
    except HTTPException:
        try:
            if dst_path.exists():
                dst_path.unlink()
        except Exception:
            logger.warning("Could not remove partial file %s", dst_path, exc_info=True)
        raise
    except Exception as e:
        try:
            if dst_path.exists():
                dst_path.unlink()
        except Exception:
            pass
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save file: {type(e).__name__}: {str(e)}",
        ) from e
    finally:
        try:
            await upload.seek(0)
        except Exception:
            pass

    return written


async def _get_user_file(db: AsyncSession, user_id: UUID, file_id: UUID) -> Optional[FileModel]:
    """Единая точка получения файла пользователя."""
    if hasattr(crud_file, "get_user_file"):
        return await crud_file.get_user_file(db, file_id=file_id, user_id=user_id)

    stmt = (
        select(FileModel)
        .where(FileModel.id == file_id, FileModel.user_id == user_id)
        .options(selectinload(FileModel.conversations))
    )
    res = await db.execute(stmt)
    return res.scalar_one_or_none()


async def _get_conversation_ids_for_file(db: AsyncSession, file_id: UUID) -> List[UUID]:
    stmt = select(ConversationFile.conversation_id).where(ConversationFile.file_id == file_id)
    res = await db.execute(stmt)
    return list(res.scalars().all())


def _to_file_info(file_obj: FileModel, conversation_ids: List[UUID]) -> FileInfo:
    """Маппинг модели File -> FileInfo по схеме app/schemas/file.py"""
    return FileInfo(
        id=file_obj.id,
        filename=file_obj.filename,
        original_filename=file_obj.original_filename,
        file_type=file_obj.file_type,
        file_size=file_obj.file_size,
        is_processed=file_obj.is_processed,
        chunks_count=file_obj.chunks_count,
        uploaded_at=file_obj.uploaded_at,
        processed_at=file_obj.processed_at,
        conversation_ids=conversation_ids,
    )


@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(
    file: UploadFile = FastAPIFile(...),
    conversation_id: UUID = Form(...),
    embedding_mode: str = Form("local"),
    embedding_model: Optional[str] = Form(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Upload and process file, associating it with a specific conversation.
    """

    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename is empty")

    if not settings.is_file_supported(file.filename):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File type not supported. Supported: {settings.supported_filetypes}",
        )

    valid_modes = {"local", "corporate", "aihub", "openai"}
    if embedding_mode not in valid_modes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid embedding_mode. Use one of: {sorted(valid_modes)}",
        )

    logger.info("Uploading file: %s mode=%s model=%s", file.filename, embedding_mode, embedding_model)

    original_filename = file.filename
    safe_name = _safe_filename(original_filename)

    file_id = uuid4()
    stored_name = f"{file_id}_{safe_name}"

    file_path = UPLOAD_DIR / f"{current_user.id}" / stored_name
    max_bytes = int(settings.MAX_FILESIZE_MB * 1024 * 1024)
    written = await _save_uploadfile_with_limit(file, file_path, max_bytes=max_bytes)

    logger.info("File saved: %s bytes=%s", file_path, written)

    file_type = Path(original_filename).suffix.lower().lstrip(".") or "unknown"

    content_preview = None
    if file_type in {"txt", "md"}:
        try:
            async with aiofiles.open(file_path, "rb") as f:
                head = await f.read(5000)
            content_preview = head.decode("utf-8", errors="ignore")[:500]
        except Exception as e:
            logger.warning("Could not create preview: %s", e)

    # Создание записи и привязка к беседе
    try:
        if hasattr(crud_file, "create_with_conversation"):
            file_record = await crud_file.create_with_conversation(
                db,
                user_id=current_user.id,
                conversation_id=conversation_id,
                filename=stored_name,
                original_filename=original_filename,
                path=str(file_path),
                file_type=file_type,
                file_size=written,
                content_preview=content_preview,
            )
        else:
            file_record = await crud_file.create_file(
                db,
                user_id=current_user.id,
                filename=stored_name,
                original_filename=original_filename,
                path=str(file_path),
                file_type=file_type,
                file_size=written,
                content_preview=content_preview,
            )
            await crud_file.add_file_to_conversation(db, file_id=file_record.id, conversation_id=conversation_id)
    except Exception as e:
        logger.error("Failed to create DB record for file: %s", e, exc_info=True)
        try:
            if file_path.exists():
                file_path.unlink()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail="Failed to create file record")

    # Запуск обработки (extract -> chunk -> embed -> vector store)
    try:
        from app.services.file import process_file_async

        await process_file_async(
            file_id=file_record.id,
            file_path=Path(file_record.path),
            embedding_mode=embedding_mode,
            embedding_model=embedding_model,
        )
    except Exception:
        logger.exception("Failed to schedule file processing")

    return FileUploadResponse(
        file_id=file_record.id,
        filename=file_record.filename,
        original_filename=file_record.original_filename,
        file_type=file_record.file_type,
        file_size=file_record.file_size,
        content_preview=file_record.content_preview,
        is_processed=file_record.is_processed,
        chunks_count=file_record.chunks_count,
        uploaded_at=file_record.uploaded_at,
    )


@router.post("/process/{file_id}")
async def reprocess_file(
    file_id: UUID,
    embedding_mode: str = Form("local"),
    embedding_model: Optional[str] = Form(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    file_obj = await _get_user_file(db, user_id=current_user.id, file_id=file_id)
    if not file_obj:
        raise HTTPException(status_code=404, detail="File not found")

    try:
        from app.services.file import process_file_async

        await process_file_async(
            file_id=file_obj.id,
            file_path=Path(file_obj.path),
            embedding_mode=embedding_mode,
            embedding_model=embedding_model,
        )
    except Exception:
        logger.exception("Failed to schedule file processing")

    return {"status": "ok", "file_id": str(file_id)}


@router.get("/", response_model=List[FileInfo])
async def list_files(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    files = await crud_file.get_user_files(db, user_id=current_user.id, skip=skip, limit=limit)
    result: List[FileInfo] = []
    for f in files:
        conversation_ids = await _get_conversation_ids_for_file(db, f.id)
        result.append(_to_file_info(f, conversation_ids))
    return result


@router.get("/processed", response_model=List[FileInfo])
async def get_processed_files(
    conversation_id: Optional[UUID] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    if conversation_id:
        files = await crud_file.get_conversation_files(db, conversation_id=conversation_id, user_id=current_user.id)
    else:
        files = await crud_file.get_processed_files(db, user_id=current_user.id)

    result: List[FileInfo] = []
    for f in files:
        conversation_ids = await _get_conversation_ids_for_file(db, f.id)
        result.append(_to_file_info(f, conversation_ids))
    return result


@router.get("/{file_id}", response_model=FileInfo)
async def get_file(
    file_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    file_obj = await _get_user_file(db, user_id=current_user.id, file_id=file_id)
    if not file_obj:
        raise HTTPException(status_code=404, detail="File not found")

    conversation_ids = await _get_conversation_ids_for_file(db, file_id)
    return _to_file_info(file_obj, conversation_ids)


@router.get("/status/{file_id}", response_model=FileProcessingStatus)
async def get_file_status(
    file_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    file_obj = await _get_user_file(db, user_id=current_user.id, file_id=file_id)
    if not file_obj:
        raise HTTPException(status_code=404, detail="File not found")

    err = None
    try:
        if isinstance(file_obj.custom_metadata, dict):
            err = file_obj.custom_metadata.get("error")
    except Exception:
        err = None

    return FileProcessingStatus(
        file_id=file_obj.id,
        status=file_obj.is_processed,
        chunks_count=file_obj.chunks_count,
        error_message=err,
    )


@router.delete("/{file_id}")
async def delete_file(
    file_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    file_obj = await _get_user_file(db, user_id=current_user.id, file_id=file_id)
    if not file_obj:
        raise HTTPException(status_code=404, detail="File not found")

    await crud_file.remove_file_from_all_conversations(db, file_id=file_id)

    try:
        path = Path(file_obj.path)
        await crud_file.remove(db, id=file_id)
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass
    except Exception as e:
        logger.error("Failed to delete file: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to delete file")

    return {"status": "ok"}

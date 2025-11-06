# app/api/v1/endpoints/files.py
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from uuid import UUID, uuid4
import aiofiles
import os
from pathlib import Path

from app.db.session import get_db
from app.db.models import User
from app.schemas import FileUploadResponse, FileInfo
from app.api.dependencies import get_current_user
from app.crud import crud_file
from app.core.config import settings
from app.services.file import process_file_async

router = APIRouter()

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)


@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(
        file: UploadFile = File(...),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Upload and process file"""
    if not settings.is_file_supported(file.filename):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File type not supported"
        )

    if file.size > settings.MAX_FILESIZE_MB * 1024 * 1024:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File too large (max {settings.MAX_FILESIZE_MB}MB)"
        )

    # Save file
    file_id = uuid4()
    file_path = UPLOAD_DIR / f"{current_user.id}" / f"{file_id}_{file.filename}"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    async with aiofiles.open(file_path, 'wb') as f:
        content = await file.read()
        await f.write(content)

    # Save to database
    file_type = file.filename.split('.')[-1].lower()
    file_record = await crud_file.create_file(
        db,
        user_id=current_user.id,
        filename=f"{file_id}_{file.filename}",
        original_filename=file.filename,
        path=str(file_path),
        file_type=file_type,
        file_size=file.size,
        content_preview=content[:500].decode('utf-8', errors='ignore') if file_type == 'txt' else None
    )

    # Start async processing
    await process_file_async(file_record.id, file_path)

    return FileUploadResponse(
        file_id=file_record.id,
        filename=file_record.filename,
        original_filename=file_record.original_filename,
        file_type=file_record.file_type,
        file_size=file_record.file_size,
        content_preview=file_record.content_preview,
        is_processed=file_record.is_processed,
        chunks_count=file_record.chunks_count,
        uploaded_at=file_record.uploaded_at
    )


@router.get("/", response_model=List[FileInfo])
async def get_files(
        skip: int = 0,
        limit: int = 100,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Get user files"""
    files = await crud_file.get_user_files(
        db,
        user_id=current_user.id,
        skip=skip,
        limit=limit
    )
    return files


@router.get("/{file_id}", response_model=FileInfo)
async def get_file(
        file_id: UUID,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Get file info"""
    file = await crud_file.get_user_file(
        db,
        file_id=file_id,
        user_id=current_user.id
    )

    if not file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )

    return file


@router.delete("/{file_id}")
async def delete_file(
        file_id: UUID,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Delete file"""
    file = await crud_file.get_user_file(
        db,
        file_id=file_id,
        user_id=current_user.id
    )

    if not file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )

    # Delete physical file
    if os.path.exists(file.path):
        os.remove(file.path)

    # Delete from database
    await crud_file.remove(db, id=file_id)

    return {"message": "File deleted"}

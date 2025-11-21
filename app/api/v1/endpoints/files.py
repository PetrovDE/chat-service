# app/api/v1/endpoints/files.py
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from uuid import UUID, uuid4
import aiofiles
import os
from pathlib import Path
import logging

from app.db.session import get_db
from app.db.models import User
from app.schemas import FileUploadResponse, FileInfo
from app.api.dependencies import get_current_user
from app.crud import crud_file
from app.core.config import settings
from app.services.file import process_file_async

logger = logging.getLogger(__name__)

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


@router.get("/processed", response_model=List[FileInfo])
async def get_processed_files(
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Get only processed files for the current user"""
    files = await crud_file.get_processed_files(
        db,
        user_id=current_user.id
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
    """Delete file and all associated embeddings"""
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

    try:
        # 1. Delete embeddings from ChromaDB
        await delete_file_from_chroma(str(file_id))
        logger.info(f"Deleted embeddings from ChromaDB for file {file_id}")

        # 2. Delete embeddings from PostgreSQL (if exists)
        await delete_file_from_postgres(db, str(file_id))
        logger.info(f"Deleted embeddings from PostgreSQL for file {file_id}")

        # 3. Delete physical file
        if os.path.exists(file.path):
            os.remove(file.path)
            logger.info(f"Deleted physical file: {file.path}")

        # 4. Delete from database
        await crud_file.remove(db, id=file_id)
        logger.info(f"Deleted file record from database: {file_id}")

        return {
            "success": True,
            "message": "File and all embeddings deleted successfully",
            "file_id": str(file_id)
        }

    except Exception as e:
        logger.error(f"Error deleting file {file_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting file: {str(e)}"
        )


async def delete_file_from_chroma(file_id: str):
    """Delete all embeddings for a file from ChromaDB"""
    try:
        from app.rag.vector_store import VectorStoreManager

        vector_store = VectorStoreManager()

        # Delete by metadata filter
        vector_store.collection.delete(
            where={"file_id": file_id}
        )

        logger.info(f"Successfully deleted ChromaDB embeddings for file: {file_id}")
    except Exception as e:
        logger.error(f"Error deleting from ChromaDB: {e}")
        raise


async def delete_file_from_postgres(db: AsyncSession, file_id: str):
    """Delete all embeddings for a file from PostgreSQL"""
    try:
        from sqlalchemy import text

        # Check if document_embeddings table exists
        check_query = text("""
                           SELECT EXISTS (SELECT
                                          FROM information_schema.tables
                                          WHERE table_name = 'document_embeddings')
                           """)
        result = await db.execute(check_query)
        table_exists = result.scalar()

        if table_exists:
            delete_query = text("""
                                DELETE
                                FROM document_embeddings
                                WHERE metadata ->> 'file_id' = :file_id
                                """)
            await db.execute(delete_query, {"file_id": file_id})
            await db.commit()
            logger.info(f"Successfully deleted PostgreSQL embeddings for file: {file_id}")
        else:
            logger.info("document_embeddings table does not exist, skipping PostgreSQL cleanup")

    except Exception as e:
        logger.error(f"Error deleting from PostgreSQL: {e}")
        # Don't raise - PostgreSQL embeddings are optional
        pass

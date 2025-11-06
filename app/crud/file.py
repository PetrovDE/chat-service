# app/crud/file.py
from typing import List, Optional
from uuid import UUID
from datetime import datetime
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.db.models.file import File


class CRUDFile(CRUDBase[File, dict, dict]):
    async def create_file(
            self,
            db: AsyncSession,
            *,
            user_id: UUID,
            filename: str,
            original_filename: str,
            path: str,
            file_type: str,
            file_size: int,
            content_preview: Optional[str] = None
    ) -> File:
        """Create a new file record"""
        db_obj = File(
            user_id=user_id,
            filename=filename,
            original_filename=original_filename,
            path=path,
            file_type=file_type,
            file_size=file_size,
            content_preview=content_preview
        )
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    async def get_user_files(
            self,
            db: AsyncSession,
            *,
            user_id: UUID,
            skip: int = 0,
            limit: int = 100
    ) -> List[File]:
        """Get all files for a user"""
        query = select(File).where(
            File.user_id == user_id
        ).order_by(File.uploaded_at.desc())

        query = query.offset(skip).limit(limit)

        result = await db.execute(query)
        return result.scalars().all()

    async def get_user_file(
            self,
            db: AsyncSession,
            *,
            file_id: UUID,
            user_id: UUID
    ) -> Optional[File]:
        """Get a specific file for a user"""
        query = select(File).where(
            and_(
                File.id == file_id,
                File.user_id == user_id
            )
        )
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def update_processing_status(
            self,
            db: AsyncSession,
            *,
            file_id: UUID,
            status: str,
            chunks_count: Optional[int] = None,
            embedding_model: Optional[str] = None
    ) -> Optional[File]:
        """Update file processing status"""
        file = await self.get(db, id=file_id)
        if file:
            file.is_processed = status
            if chunks_count is not None:
                file.chunks_count = chunks_count
            if embedding_model:
                file.embedding_model = embedding_model
            if status == "completed":
                file.processed_at = datetime.utcnow()
            await db.commit()
            await db.refresh(file)
        return file

    async def get_processed_files(
            self,
            db: AsyncSession,
            *,
            user_id: UUID
    ) -> List[File]:
        """Get all processed files for a user"""
        query = select(File).where(
            and_(
                File.user_id == user_id,
                File.is_processed == "completed"
            )
        ).order_by(File.processed_at.desc())

        result = await db.execute(query)
        return result.scalars().all()


# Create instance
crud_file = CRUDFile(File)

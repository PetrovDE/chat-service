# app/crud/file.py
"""CRUD operations for file management"""

from __future__ import annotations

from typing import List, Optional
from uuid import UUID
from datetime import datetime

from sqlalchemy import select, and_, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.db.models.file import File
from app.db.models.conversation_file import ConversationFile


class CRUDFile(CRUDBase[File, dict, dict]):
    """CRUD operations for File model with conversation association"""

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
        content_preview: Optional[str] = None,
    ) -> File:
        """Create a new file record"""
        db_obj = File(
            user_id=user_id,
            filename=filename,
            original_filename=original_filename,
            path=path,
            file_type=file_type,
            file_size=file_size,
            content_preview=content_preview,
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
        limit: int = 100,
    ) -> List[File]:
        """Get all files for a user"""
        query = (
            select(File)
            .where(File.user_id == user_id)
            .order_by(File.uploaded_at.desc())
            .offset(skip)
            .limit(limit)
        )
        result = await db.execute(query)
        return result.scalars().all()

    async def get_user_file(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
        user_id: UUID,
    ) -> Optional[File]:
        """Get a specific file for a user"""
        query = select(File).where(and_(File.id == file_id, File.user_id == user_id))
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def get_processed_files(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
    ) -> List[File]:
        """Get all processed files for a user"""
        query = (
            select(File)
            .where(and_(File.user_id == user_id, File.is_processed == "completed"))
            .order_by(File.processed_at.desc())
        )
        result = await db.execute(query)
        return result.scalars().all()

    async def get_conversation_files(
        self,
        db: AsyncSession,
        *,
        conversation_id: UUID,
        user_id: UUID,
    ) -> List[File]:
        """Get all files associated with a specific conversation for the current user"""
        query = (
            select(File)
            .join(ConversationFile, File.id == ConversationFile.file_id)
            .where(
                and_(
                    ConversationFile.conversation_id == conversation_id,
                    File.user_id == user_id,
                    File.is_processed == "completed",
                )
            )
            .order_by(File.uploaded_at.desc())
        )
        result = await db.execute(query)
        return result.scalars().all()

    async def get_conversation_file_ids(
        self,
        db: AsyncSession,
        *,
        conversation_id: UUID,
    ) -> List[UUID]:
        """Get all file IDs associated with a conversation"""
        query = select(ConversationFile.file_id).where(ConversationFile.conversation_id == conversation_id)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def add_file_to_conversation(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
        conversation_id: UUID,
    ) -> ConversationFile:
        """Associate a file with a conversation"""
        existing = await db.execute(
            select(ConversationFile).where(
                and_(
                    ConversationFile.file_id == file_id,
                    ConversationFile.conversation_id == conversation_id,
                )
            )
        )
        obj = existing.scalar_one_or_none()
        if obj:
            return obj

        db_obj = ConversationFile(
            file_id=file_id,
            conversation_id=conversation_id,
            added_at=datetime.utcnow(),
        )
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    async def update_processing_status(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
        status: str,
        chunks_count: Optional[int] = None,
        embedding_model: Optional[str] = None,
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

    # ----------------------------
    # NEW: delete associations
    # ----------------------------

    async def remove_file_from_conversation(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
        conversation_id: UUID,
    ) -> int:
        """
        Remove file<->conversation link.
        Returns number of deleted rows.
        """
        stmt = delete(ConversationFile).where(
            and_(
                ConversationFile.file_id == file_id,
                ConversationFile.conversation_id == conversation_id,
            )
        )
        res = await db.execute(stmt)
        await db.commit()
        return int(res.rowcount or 0)

    async def remove_file_from_all_conversations(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
    ) -> int:
        """
        Remove file from all conversations (delete all ConversationFile links).
        Returns number of deleted rows.
        """
        stmt = delete(ConversationFile).where(ConversationFile.file_id == file_id)
        res = await db.execute(stmt)
        await db.commit()
        return int(res.rowcount or 0)

    async def delete_file_record(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
    ) -> Optional[File]:
        """
        Delete File row itself (without touching filesystem).
        """
        obj = await self.get(db, id=file_id)
        if not obj:
            return None
        await db.delete(obj)
        await db.commit()
        return obj


# Create instance
crud_file = CRUDFile(File)

"""CRUD and lifecycle helpers for persistent user-owned files."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from sqlalchemy import and_, delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.db.models.conversation_file import ConversationFile
from app.db.models.file import File
from app.db.models.file_processing import FileProcessingProfile


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _normalize_file_status(status: str) -> str:
    raw = str(status or "").strip().lower()
    if raw in {"ready", "completed", "partial_success", "partial_failed"}:
        return "ready"
    if raw in {"failed"}:
        return "failed"
    if raw in {"deleting", "deleted", "uploaded"}:
        return raw
    return "processing"


def _normalize_processing_status(status: str) -> str:
    raw = str(status or "").strip().lower()
    if raw in {"completed"}:
        return "ready"
    if raw in {"partial_success", "partial_failed"}:
        return raw
    if raw in {"ready"}:
        return "ready"
    return raw or "queued"


def _parse_embedding_identity(raw_value: Optional[str]) -> tuple[str, Optional[str]]:
    raw = str(raw_value or "").strip()
    if not raw:
        return "local", None
    if ":" not in raw:
        return "local", raw
    provider, model = raw.split(":", 1)
    provider_norm = (provider or "local").strip().lower()
    if provider_norm == "corporate":
        provider_norm = "aihub"
    if provider_norm == "ollama":
        provider_norm = "local"
    return provider_norm, (model or "").strip() or None


class CRUDFile(CRUDBase[File, dict, dict]):
    async def create_file(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        original_filename: str,
        stored_filename: str,
        storage_key: str,
        storage_path: str,
        mime_type: Optional[str],
        extension: str,
        size_bytes: int,
        checksum: Optional[str],
        visibility: str = "private",
        source_kind: str = "upload",
        content_preview: Optional[str] = None,
        custom_metadata: Optional[Dict[str, Any]] = None,
    ) -> File:
        db_obj = File(
            user_id=user_id,
            original_filename=original_filename,
            stored_filename=stored_filename,
            storage_key=storage_key,
            storage_path=storage_path,
            mime_type=mime_type,
            extension=(extension or "").strip().lower().lstrip("."),
            size_bytes=int(size_bytes or 0),
            checksum=checksum,
            visibility=(visibility or "private").strip().lower() or "private",
            status="uploaded",
            source_kind=(source_kind or "upload").strip().lower() or "upload",
            created_at=_utcnow(),
            updated_at=_utcnow(),
            content_preview=content_preview,
            chunks_count=0,
            custom_metadata=(dict(custom_metadata) if isinstance(custom_metadata, dict) else {}),
        )
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    async def get_user_storage_usage_bytes(self, db: AsyncSession, *, user_id: UUID) -> int:
        query = select(func.coalesce(func.sum(File.size_bytes), 0)).where(
            File.user_id == user_id,
            File.deleted_at.is_(None),
            File.status != "deleted",
        )
        result = await db.execute(query)
        return int(result.scalar() or 0)

    async def get_user_files(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        skip: int = 0,
        limit: int = 100,
        include_deleted: bool = False,
    ) -> List[File]:
        query = (
            select(File)
            .where(File.user_id == user_id)
            .options(selectinload(File.processing_profiles))
            .order_by(File.created_at.desc())
            .offset(skip)
            .limit(limit)
        )
        if not include_deleted:
            query = query.where(File.deleted_at.is_(None), File.status != "deleted")
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_user_file(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
        user_id: UUID,
        include_deleted: bool = False,
    ) -> Optional[File]:
        query = (
            select(File)
            .where(and_(File.id == file_id, File.user_id == user_id))
            .options(selectinload(File.processing_profiles))
        )
        if not include_deleted:
            query = query.where(File.deleted_at.is_(None), File.status != "deleted")
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def get_processed_files(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
    ) -> List[File]:
        query = (
            select(File)
            .where(and_(File.user_id == user_id, File.status == "ready", File.deleted_at.is_(None)))
            .order_by(File.processed_at.desc().nullslast(), File.created_at.desc())
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_user_ready_files_for_resolution(
        self,
        db: AsyncSession,
        *,
        user_id: UUID,
        limit: int = 300,
    ) -> List[File]:
        query = (
            select(File)
            .where(
                and_(
                    File.user_id == user_id,
                    File.status == "ready",
                    File.deleted_at.is_(None),
                )
            )
            .options(selectinload(File.processing_profiles))
            .order_by(File.created_at.desc())
            .limit(max(1, int(limit or 1)))
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_conversation_files(
        self,
        db: AsyncSession,
        *,
        conversation_id: UUID,
        user_id: UUID,
    ) -> List[File]:
        query = (
            select(File)
            .join(ConversationFile, File.id == ConversationFile.file_id)
            .where(
                and_(
                    ConversationFile.chat_id == conversation_id,
                    File.user_id == user_id,
                    File.status == "ready",
                    File.deleted_at.is_(None),
                )
            )
            .options(selectinload(File.processing_profiles))
            .order_by(File.created_at.desc())
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_conversation_file_ids(
        self,
        db: AsyncSession,
        *,
        conversation_id: UUID,
    ) -> List[UUID]:
        query = select(ConversationFile.file_id).where(ConversationFile.chat_id == conversation_id)
        result = await db.execute(query)
        return list(result.scalars().all())

    async def add_file_to_conversation(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
        conversation_id: UUID,
        attached_by_user_id: Optional[UUID] = None,
    ) -> ConversationFile:
        existing = await db.execute(
            select(ConversationFile).where(
                and_(
                    ConversationFile.file_id == file_id,
                    ConversationFile.chat_id == conversation_id,
                )
            )
        )
        obj = existing.scalar_one_or_none()
        if obj:
            return obj

        if attached_by_user_id is None:
            file_obj = await self.get(db, id=file_id)
            if not file_obj:
                raise ValueError("File not found for attach")
            attached_by_user_id = file_obj.user_id

        db_obj = ConversationFile(
            file_id=file_id,
            chat_id=conversation_id,
            attached_by_user_id=attached_by_user_id,
            attached_at=_utcnow(),
        )
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    async def remove_file_from_conversation(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
        conversation_id: UUID,
    ) -> int:
        stmt = delete(ConversationFile).where(
            and_(
                ConversationFile.file_id == file_id,
                ConversationFile.chat_id == conversation_id,
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
        stmt = delete(ConversationFile).where(ConversationFile.file_id == file_id)
        res = await db.execute(stmt)
        await db.commit()
        return int(res.rowcount or 0)

    async def create_processing_profile(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
        pipeline_version: str,
        parser_version: str,
        artifact_version: str,
        embedding_provider: str,
        embedding_model: Optional[str],
        embedding_dimension: Optional[int],
        chunking_strategy: Optional[str],
        retrieval_profile: Optional[str],
        status: str = "queued",
        is_active: bool = False,
    ) -> FileProcessingProfile:
        if is_active:
            existing = await db.execute(
                select(FileProcessingProfile).where(
                    and_(FileProcessingProfile.file_id == file_id, FileProcessingProfile.is_active.is_(True))
                )
            )
            for row in existing.scalars().all():
                row.is_active = False
        db_obj = FileProcessingProfile(
            file_id=file_id,
            pipeline_version=pipeline_version,
            parser_version=parser_version,
            artifact_version=artifact_version,
            embedding_provider=embedding_provider,
            embedding_model=embedding_model,
            embedding_dimension=embedding_dimension,
            chunking_strategy=chunking_strategy,
            retrieval_profile=retrieval_profile,
            status=status,
            started_at=_utcnow() if status in {"processing", "parsing", "chunking", "embedding", "indexing"} else None,
            is_active=bool(is_active),
            created_at=_utcnow(),
            updated_at=_utcnow(),
            ingestion_progress={},
            artifact_metadata={},
        )
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    async def get_processing_profile(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
        processing_id: UUID,
        user_id: UUID,
    ) -> Optional[FileProcessingProfile]:
        query = (
            select(FileProcessingProfile)
            .join(File, File.id == FileProcessingProfile.file_id)
            .where(
                FileProcessingProfile.id == processing_id,
                FileProcessingProfile.file_id == file_id,
                File.user_id == user_id,
            )
        )
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def list_processing_profiles(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
        user_id: UUID,
    ) -> List[FileProcessingProfile]:
        query = (
            select(FileProcessingProfile)
            .join(File, File.id == FileProcessingProfile.file_id)
            .where(FileProcessingProfile.file_id == file_id, File.user_id == user_id)
            .order_by(FileProcessingProfile.created_at.desc(), FileProcessingProfile.started_at.desc().nullslast())
        )
        result = await db.execute(query)
        return list(result.scalars().all())

    async def get_active_processing(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
        user_id: UUID,
    ) -> Optional[FileProcessingProfile]:
        query = (
            select(FileProcessingProfile)
            .join(File, File.id == FileProcessingProfile.file_id)
            .where(
                FileProcessingProfile.file_id == file_id,
                FileProcessingProfile.is_active.is_(True),
                File.user_id == user_id,
            )
            .order_by(FileProcessingProfile.created_at.desc())
            .limit(1)
        )
        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def activate_processing(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
        processing_id: UUID,
    ) -> Optional[FileProcessingProfile]:
        file_obj = await self.get(db, id=file_id)
        if not file_obj:
            return None
        if file_obj.deleted_at is not None or str(file_obj.status or "").lower() in {"deleting", "deleted"}:
            # Never reactivate processing for tombstoned files.
            return None

        query = select(FileProcessingProfile).where(
            FileProcessingProfile.file_id == file_id,
            FileProcessingProfile.id == processing_id,
        )
        result = await db.execute(query)
        profile = result.scalar_one_or_none()
        if profile is None:
            return None

        all_profiles_result = await db.execute(
            select(FileProcessingProfile).where(FileProcessingProfile.file_id == file_id)
        )
        for item in all_profiles_result.scalars().all():
            item.is_active = bool(item.id == profile.id)

        file_obj.embedding_model = (
            f"{profile.embedding_provider}:{profile.embedding_model}"
            if profile.embedding_model
            else profile.embedding_provider
        )
        file_obj.custom_metadata = dict(profile.artifact_metadata or {})
        if str(profile.status or "").lower() in {"ready", "completed", "partial_success", "partial_failed"}:
            file_obj.status = "ready"
            file_obj.processed_at = profile.finished_at or _utcnow()
        file_obj.updated_at = _utcnow()
        await db.commit()
        await db.refresh(profile)
        return profile

    async def update_processing_status(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
        status: str,
        chunks_count: Optional[int] = None,
        embedding_model: Optional[str] = None,
        metadata_patch: Optional[Dict[str, Any]] = None,
        processing_id: Optional[UUID] = None,
    ) -> Optional[File]:
        file_obj = await db.get(File, file_id)
        if not file_obj:
            return None
        if file_obj.deleted_at is not None or str(file_obj.status or "").lower() in {"deleting", "deleted"}:
            # Deletion wins against async ingestion updates to avoid status resurrection races.
            return file_obj

        file_obj.status = _normalize_file_status(status)
        file_obj.updated_at = _utcnow()
        if chunks_count is not None:
            file_obj.chunks_count = int(chunks_count)
        if embedding_model:
            file_obj.embedding_model = str(embedding_model)

        if metadata_patch:
            existing_meta = file_obj.custom_metadata if isinstance(file_obj.custom_metadata, dict) else {}
            file_obj.custom_metadata = {**existing_meta, **metadata_patch}

        if file_obj.status in {"ready", "failed"}:
            file_obj.processed_at = _utcnow()

        target_profile: Optional[FileProcessingProfile] = None
        if processing_id is not None:
            result = await db.execute(
                select(FileProcessingProfile).where(
                    FileProcessingProfile.id == processing_id,
                    FileProcessingProfile.file_id == file_id,
                )
            )
            target_profile = result.scalar_one_or_none()

        if target_profile is None:
            result = await db.execute(
                select(FileProcessingProfile)
                .where(FileProcessingProfile.file_id == file_id)
                .order_by(FileProcessingProfile.is_active.desc(), FileProcessingProfile.created_at.desc())
            )
            target_profile = result.scalars().first()

        if target_profile is not None:
            normalized_processing_status = _normalize_processing_status(status)
            target_profile.status = normalized_processing_status
            target_profile.updated_at = _utcnow()
            if normalized_processing_status in {"processing", "parsing", "chunking", "embedding", "indexing"}:
                target_profile.started_at = target_profile.started_at or _utcnow()
            if normalized_processing_status in {"ready", "partial_success", "partial_failed", "failed"}:
                target_profile.finished_at = _utcnow()

            if embedding_model:
                provider, model = _parse_embedding_identity(embedding_model)
                target_profile.embedding_provider = provider
                target_profile.embedding_model = model
            if metadata_patch:
                progress = metadata_patch.get("ingestion_progress")
                if isinstance(progress, dict):
                    existing = target_profile.ingestion_progress if isinstance(target_profile.ingestion_progress, dict) else {}
                    target_profile.ingestion_progress = {**existing, **progress}

                raw_dimension = metadata_patch.get("embedding_dimension")
                if raw_dimension is not None:
                    try:
                        target_profile.embedding_dimension = int(raw_dimension)
                    except Exception:
                        pass

                artifact_existing = (
                    target_profile.artifact_metadata
                    if isinstance(target_profile.artifact_metadata, dict)
                    else {}
                )
                artifact_delta = {k: v for k, v in metadata_patch.items() if k != "ingestion_progress"}
                target_profile.artifact_metadata = {**artifact_existing, **artifact_delta}
                if artifact_delta.get("error") is not None:
                    target_profile.error_message = str(artifact_delta.get("error"))

            if normalized_processing_status in {"ready", "partial_success", "partial_failed"}:
                all_profiles = await db.execute(
                    select(FileProcessingProfile).where(FileProcessingProfile.file_id == file_id)
                )
                for profile in all_profiles.scalars().all():
                    profile.is_active = bool(profile.id == target_profile.id)

                file_obj.status = "ready"
                file_obj.embedding_model = (
                    f"{target_profile.embedding_provider}:{target_profile.embedding_model}"
                    if target_profile.embedding_model
                    else target_profile.embedding_provider
                )
                if isinstance(target_profile.artifact_metadata, dict):
                    file_obj.custom_metadata = dict(target_profile.artifact_metadata)
                file_obj.processed_at = target_profile.finished_at or _utcnow()

        await db.commit()
        await db.refresh(file_obj)
        return file_obj

    async def mark_file_deleting(self, db: AsyncSession, *, file_id: UUID) -> Optional[File]:
        file_obj = await self.get(db, id=file_id)
        if not file_obj:
            return None
        file_obj.status = "deleting"
        file_obj.updated_at = _utcnow()
        await db.commit()
        await db.refresh(file_obj)
        return file_obj

    async def mark_file_deleted(self, db: AsyncSession, *, file_id: UUID) -> Optional[File]:
        file_obj = await self.get(db, id=file_id)
        if not file_obj:
            return None
        file_obj.status = "deleted"
        file_obj.deleted_at = _utcnow()
        file_obj.updated_at = _utcnow()
        file_obj.embedding_model = None
        file_obj.chunks_count = 0
        file_obj.custom_metadata = {}
        await db.commit()
        await db.refresh(file_obj)
        return file_obj

    async def delete_file_record(
        self,
        db: AsyncSession,
        *,
        file_id: UUID,
    ) -> Optional[File]:
        obj = await self.get(db, id=file_id)
        if not obj:
            return None
        await db.delete(obj)
        await db.commit()
        return obj


crud_file = CRUDFile(File)

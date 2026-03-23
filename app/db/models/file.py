"""Persistent user-owned file model."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Column, DateTime, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.base import Base


class File(Base):
    """
    Persistent user-owned file.
    Chat linkage and processing versions are kept in dedicated tables.
    """

    __tablename__ = "files"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False, index=True)
    original_filename = Column(String(512), nullable=False)
    stored_filename = Column(String(512), nullable=False)
    storage_key = Column(String(1024), nullable=False, unique=True)
    storage_path = Column(String(2048), nullable=False)
    mime_type = Column(String(255), nullable=True)
    extension = Column(String(32), nullable=False, default="")
    size_bytes = Column(BigInteger, nullable=False, default=0)
    checksum = Column(String(128), nullable=True)
    visibility = Column(String(32), nullable=False, default="private")
    status = Column(String(32), nullable=False, default="uploaded", index=True)
    source_kind = Column(String(64), nullable=False, default="upload")
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    deleted_at = Column(DateTime, nullable=True)

    # Denormalized summary for fast UI/retrieval access.
    chunks_count = Column(Integer, default=0, nullable=False)
    embedding_model = Column(String(255), nullable=True)
    processed_at = Column(DateTime, nullable=True)
    custom_metadata = Column(JSON, nullable=True)
    content_preview = Column(Text, nullable=True)

    owner = relationship("User", back_populates="files")
    conversations_association = relationship(
        "ConversationFile",
        back_populates="file",
        cascade="all, delete-orphan",
    )
    conversations = relationship(
        "Conversation",
        secondary="chat_file_links",
        back_populates="files",
        viewonly=True,
        lazy="selectin",
    )
    processing_profiles = relationship(
        "FileProcessingProfile",
        back_populates="file",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    @property
    def active_processing(self) -> Optional["FileProcessingProfile"]:
        active = [p for p in list(self.processing_profiles or []) if bool(getattr(p, "is_active", False))]
        if not active:
            return None
        active.sort(
            key=lambda item: (
                getattr(item, "started_at", None) or datetime.min,
                getattr(item, "created_at", None) or datetime.min,
            ),
            reverse=True,
        )
        return active[0]

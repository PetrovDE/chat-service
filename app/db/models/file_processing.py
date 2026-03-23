"""Versioned processing profile for persistent user files."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.base import Base


class FileProcessingProfile(Base):
    __tablename__ = "file_processing_profiles"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    file_id = Column(UUID(as_uuid=True), ForeignKey("files.id", ondelete="CASCADE"), nullable=False, index=True)

    pipeline_version = Column(String(64), nullable=False)
    parser_version = Column(String(64), nullable=False)
    artifact_version = Column(String(64), nullable=False)
    embedding_provider = Column(String(64), nullable=False)
    embedding_model = Column(String(255), nullable=True)
    embedding_dimension = Column(Integer, nullable=True)
    chunking_strategy = Column(String(128), nullable=True)
    retrieval_profile = Column(String(128), nullable=True)

    status = Column(String(32), nullable=False, default="queued", index=True)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    is_active = Column(Boolean, nullable=False, default=False, index=True)

    ingestion_progress = Column(JSON, nullable=True)
    artifact_metadata = Column(JSON, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    file = relationship("File", back_populates="processing_profiles")

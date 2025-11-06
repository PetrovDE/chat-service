# app/db/models/file.py
from sqlalchemy import Column, String, DateTime, ForeignKey, Integer, JSON, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime

from app.db.base import Base


class File(Base):
    __tablename__ = "files"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    filename = Column(String(255), nullable=False)
    original_filename = Column(String(255), nullable=False)
    path = Column(String(500), nullable=False)
    file_type = Column(String(50))  # pdf, docx, txt, csv, etc.
    file_size = Column(Integer)  # in bytes
    content_preview = Column(Text)  # First 500 chars for preview

    # RAG metadata
    is_processed = Column(String(20), default="pending")  # pending, processing, completed, failed
    chunks_count = Column(Integer, default=0)
    embedding_model = Column(String(100))
    processed_at = Column(DateTime)

    # Additional metadata
    custom_metadata = Column(JSON)
    uploaded_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    owner = relationship("User", back_populates="files")

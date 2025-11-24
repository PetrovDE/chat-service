# app/db/models/conversation_file.py
"""Association model between conversations and files"""
from sqlalchemy import Column, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime

from app.db.base import Base


class ConversationFile(Base):
    """Association table between conversations and files - tracks which chat has which files"""
    __tablename__ = "conversation_files"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id = Column(UUID(as_uuid=True), ForeignKey("conversations.id", ondelete="CASCADE"), nullable=False)
    file_id = Column(UUID(as_uuid=True), ForeignKey("files.id", ondelete="CASCADE"), nullable=False)
    # Timestamp when file was added to this conversation
    added_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    conversation = relationship("Conversation", back_populates="files_association")
    file = relationship("File", back_populates="conversations_association")

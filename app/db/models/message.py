# app/db/models/message.py
from sqlalchemy import Column, String, DateTime, ForeignKey, Text, Integer, Float
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime

from app.db.base import Base


class Message(Base):
    __tablename__ = "messages"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    conversation_id = Column(UUID(as_uuid=True), ForeignKey("conversations.id"), nullable=False)
    role = Column(String(20), nullable=False)  # user, assistant, system
    content = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)

    # LLM metadata
    model_name = Column(String(100))
    temperature = Column(Float)
    max_tokens = Column(Integer)
    tokens_used = Column(Integer)
    generation_time = Column(Float)  # in seconds

    # Relationships
    conversation = relationship("Conversation", back_populates="messages")

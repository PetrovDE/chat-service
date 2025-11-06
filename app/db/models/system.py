# app/db/models/system.py
from sqlalchemy import Column, String, DateTime, ForeignKey, Integer, Float, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime

from app.db.base import Base


class APIUsageLog(Base):
    __tablename__ = "api_usage_logs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    endpoint = Column(String(255), nullable=False)
    method = Column(String(10), nullable=False)  # GET, POST, PUT, DELETE
    timestamp = Column(DateTime, default=datetime.utcnow)
    tokens_used = Column(Integer)
    response_time = Column(Float)  # in seconds
    status_code = Column(Integer)
    error_message = Column(Text)

    # Relationships
    user = relationship("User", back_populates="api_usage_logs")


class SystemSetting(Base):
    __tablename__ = "system_settings"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    key = Column(String(255), unique=True, nullable=False, index=True)
    value = Column(Text, nullable=False)
    value_type = Column(String(50))  # string, integer, boolean, json
    description = Column(String(500))
    is_public = Column(String(10), default=False)  # Can be exposed to non-admin users
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

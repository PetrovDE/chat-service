# app/schemas/file.py
"""Pydantic schemas for file operations"""
import uuid
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field


class FileUploadResponse(BaseModel):
    """Response schema for file upload"""
    file_id: uuid.UUID
    filename: str
    original_filename: str
    file_type: str
    file_size: int
    content_preview: Optional[str] = None
    is_processed: str
    chunks_count: int
    uploaded_at: datetime

    model_config = ConfigDict(from_attributes=True)


class FileInfo(BaseModel):
    """Schema for file information with conversation associations"""
    id: uuid.UUID
    filename: str
    original_filename: str
    file_type: str
    file_size: int
    is_processed: str
    chunks_count: int
    uploaded_at: datetime
    processed_at: Optional[datetime] = None
    # Новое поле: список ID бесед, где используется файл
    conversation_ids: List[uuid.UUID] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class FileProcessingStatus(BaseModel):
    """Schema for file processing status"""
    file_id: uuid.UUID
    status: str  # pending, processing, completed, failed
    chunks_count: int
    error_message: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)

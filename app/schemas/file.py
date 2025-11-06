# app/schemas/file.py
import uuid
from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class FileUploadResponse(BaseModel):
    file_id: uuid.UUID
    filename: str
    original_filename: str
    file_type: str
    file_size: int
    content_preview: Optional[str] = None
    is_processed: str
    chunks_count: int
    uploaded_at: datetime

    class Config:
        from_attributes = True


class FileInfo(BaseModel):
    id: uuid.UUID
    filename: str
    original_filename: str
    file_type: str
    file_size: int
    is_processed: str
    chunks_count: int
    uploaded_at: datetime
    processed_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class FileProcessingStatus(BaseModel):
    file_id: uuid.UUID
    status: str  # pending, processing, completed, failed
    chunks_count: int
    error_message: Optional[str] = None

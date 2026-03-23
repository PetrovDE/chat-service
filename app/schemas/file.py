"""Pydantic schemas for persistent user files and processing profiles."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class FileQuotaInfo(BaseModel):
    quota_used_bytes: int
    quota_limit_bytes: int


class FileProcessingProfileInfo(BaseModel):
    processing_id: uuid.UUID
    file_id: uuid.UUID
    pipeline_version: str
    parser_version: str
    artifact_version: str
    embedding_provider: str
    embedding_model: Optional[str] = None
    embedding_dimension: Optional[int] = None
    chunking_strategy: Optional[str] = None
    retrieval_profile: Optional[str] = None
    status: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error_message: Optional[str] = None
    is_active: bool
    ingestion_progress: Dict[str, Any] = Field(default_factory=dict)
    artifact_metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class FileInfo(BaseModel):
    file_id: uuid.UUID
    owner_user_id: uuid.UUID
    original_filename: str
    stored_filename: str
    storage_key: str
    storage_path: str
    mime_type: Optional[str] = None
    extension: str
    size_bytes: int
    checksum: Optional[str] = None
    visibility: str
    status: str
    source_kind: str
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime] = None
    chat_ids: List[uuid.UUID] = Field(default_factory=list)
    active_processing_id: Optional[uuid.UUID] = None
    active_processing_status: Optional[str] = None
    chunks_count: int = 0


class FileUploadResponse(BaseModel):
    file: FileInfo
    quota: FileQuotaInfo

    model_config = ConfigDict(from_attributes=True)


class FileProcessingStatus(BaseModel):
    file_id: uuid.UUID
    status: str
    chunks_count: int = 0
    total_chunks_expected: int = 0
    chunks_processed: int = 0
    chunks_failed: int = 0
    chunks_indexed: int = 0
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    stage: Optional[str] = None
    error_message: Optional[str] = None
    active_processing_id: Optional[uuid.UUID] = None
    is_active_processing: bool = False

    model_config = ConfigDict(from_attributes=True)


class FileReprocessRequest(BaseModel):
    embedding_provider: str = Field(default="local")
    embedding_model: Optional[str] = None
    pipeline_version: str = Field(default="pipeline-v1")
    parser_version: str = Field(default="parser-v1")
    artifact_version: str = Field(default="artifact-v1")
    chunking_strategy: Optional[str] = Field(default="smart")
    retrieval_profile: Optional[str] = Field(default="default")


class FileReprocessResponse(BaseModel):
    status: str
    file_id: uuid.UUID
    processing_id: uuid.UUID


class FileDeleteResponse(BaseModel):
    status: str
    file_id: uuid.UUID
    quota: FileQuotaInfo


class FileAttachRequest(BaseModel):
    chat_id: uuid.UUID


class FileDetachRequest(BaseModel):
    chat_id: uuid.UUID


class FileDetachResponse(BaseModel):
    status: str
    file_id: uuid.UUID
    chat_id: uuid.UUID
    removed: int


class FileAttachResponse(BaseModel):
    status: str
    file_id: uuid.UUID
    chat_id: uuid.UUID
    attached_at: datetime

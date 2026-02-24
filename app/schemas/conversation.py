# app/schemas/conversation.py
import uuid
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field


class ConversationCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    model_source: str = Field("ollama", pattern=r"^(ollama|openai|local|aihub|corporate)$")
    model_name: Optional[str] = None


class ConversationUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=200)
    is_archived: Optional[bool] = None


class MessageResponse(BaseModel):
    id: uuid.UUID
    role: str
    content: str
    timestamp: datetime
    model_name: Optional[str] = None
    tokens_used: Optional[int] = None
    generation_time: Optional[float] = None

    class Config:
        from_attributes = True


class ConversationResponse(BaseModel):
    id: uuid.UUID
    user_id: Optional[uuid.UUID]
    title: str
    model_source: str
    model_name: Optional[str]
    is_archived: bool
    message_count: int
    created_at: datetime
    updated_at: datetime


    class Config:
        from_attributes = True


class ConversationList(BaseModel):
    conversations: List[ConversationResponse]
    total: int
    page: int
    per_page: int

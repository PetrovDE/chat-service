# app/schemas/chat.py
import uuid
from typing import Optional, List
from pydantic import BaseModel, Field

class ChatMessage(BaseModel):
    message: str = Field(..., min_length=1, max_length=10000)
    conversation_id: Optional[uuid.UUID] = None
    model_source: Optional[str] = Field(None, pattern=r"^(ollama|openai|local|aihub|corporate)$")
    model_name: Optional[str] = None
    temperature: Optional[float] = Field(None, ge=0.0, le=2.0)
    max_tokens: Optional[int] = Field(None, ge=1, le=8192)
    file_ids: Optional[List[str]] = None  # Добавлено для поддержки файлов


class ChatResponse(BaseModel):
    response: str
    conversation_id: uuid.UUID
    message_id: uuid.UUID
    model_used: str
    tokens_used: Optional[int] = None
    generation_time: Optional[float] = None


class StreamChunk(BaseModel):
    type: str  # start, chunk, done, error
    content: Optional[str] = None
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None
    error: Optional[str] = None
    metadata: Optional[dict] = None

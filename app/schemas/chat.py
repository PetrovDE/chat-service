# app/schemas/chat.py
import uuid
from typing import Optional, List
from pydantic import BaseModel, Field


class ChatMessage(BaseModel):
    message: str = Field(..., min_length=1, max_length=10000)
    conversation_id: Optional[uuid.UUID] = None
    model_source: Optional[str] = Field(None, pattern=r"^(ollama|openai|local|aihub|corporate)$")
    provider_mode: Optional[str] = Field(None, pattern=r"^(explicit|policy)$")
    model_name: Optional[str] = None
    temperature: Optional[float] = Field(None, ge=0.0, le=2.0)
    max_tokens: Optional[int] = Field(None, ge=1, le=8192)
    prompt_max_chars: Optional[int] = Field(None, ge=1000, le=200000)
    file_ids: Optional[List[str]] = None
    rag_mode: Optional[str] = Field(None, pattern=r"^(auto|hybrid|full_file)$")
    summarize: bool = False
    rag_debug: bool = False
    cannot_wait: bool = False
    sla_tier: Optional[str] = Field(None, pattern=r"^(normal|critical)$")
    policy_class: Optional[str] = Field(None, max_length=64)


class ChatResponse(BaseModel):
    response: str
    conversation_id: uuid.UUID
    message_id: uuid.UUID
    model_used: str
    model_route: str = Field(default="aihub_primary", pattern=r"^(aihub_primary|ollama_fallback|aihub|ollama|openai)$")
    route_mode: str = Field(default="policy", pattern=r"^(explicit|policy)$")
    provider_selected: Optional[str] = None
    provider_effective: str = Field(default="aihub", pattern=r"^(aihub|ollama|openai|none|unknown)$")
    fallback_reason: Optional[str] = Field(default="none", pattern=r"^(none|timeout|network|hub_5xx|circuit_open)$")
    fallback_allowed: bool = False
    fallback_attempted: bool = False
    fallback_policy_version: str = "p1-aihub-first-v1"
    aihub_attempted: bool = False
    execution_route: str = Field(
        default="narrative",
        pattern=r"^(tabular_sql|complex_analytics|narrative|clarification|unknown)$",
    )
    executor_attempted: bool = False
    executor_status: str = Field(default="not_attempted", pattern=r"^(not_attempted|success|error|timeout|blocked)$")
    executor_error_code: Optional[str] = None
    artifacts_count: int = Field(default=0, ge=0, le=1024)
    tokens_used: Optional[int] = None
    generation_time: Optional[float] = None
    summary: Optional[str] = None
    caveats: Optional[List[str]] = None
    sources: Optional[List[str]] = None
    artifacts: Optional[List[dict]] = None
    rag_debug: Optional[dict] = None


class StreamChunk(BaseModel):
    type: str  # start, chunk, done, error
    content: Optional[str] = None
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None
    error: Optional[str] = None
    metadata: Optional[dict] = None

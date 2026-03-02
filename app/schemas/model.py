from typing import List, Optional

from pydantic import BaseModel


class ModelInfo(BaseModel):
    name: str
    size: int = 0
    context_window: Optional[int] = None
    max_output_tokens: Optional[int] = None


class ModelsListResponse(BaseModel):
    mode: str
    models: List[ModelInfo]
    count: int
    error: Optional[str] = None


class ProviderStatus(BaseModel):
    available: bool
    url: Optional[str] = None


class ModelsStatusResponse(BaseModel):
    ollama: ProviderStatus
    aihub: ProviderStatus
    openai: ProviderStatus

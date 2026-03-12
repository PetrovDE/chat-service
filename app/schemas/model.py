from typing import List, Optional

from pydantic import BaseModel


class ModelInfo(BaseModel):
    name: str
    size: int = 0
    context_window: Optional[int] = None
    max_output_tokens: Optional[int] = None
    capability: Optional[str] = None
    is_default: Optional[bool] = None


class ModelsListResponse(BaseModel):
    mode: str
    capability: Optional[str] = None
    default_model: Optional[str] = None
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

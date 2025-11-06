# app/schemas/common.py
from typing import Generic, TypeVar, Optional, List
from pydantic import BaseModel, Field

T = TypeVar('T')


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response"""
    items: List[T]
    total: int
    page: int = Field(ge=1)
    per_page: int = Field(ge=1, le=100)
    pages: int

    class Config:
        from_attributes = True


class SuccessResponse(BaseModel):
    success: bool = True
    message: str


class ErrorResponse(BaseModel):
    success: bool = False
    error: str
    detail: Optional[str] = None


class HealthCheck(BaseModel):
    status: str = "ok"
    timestamp: str
    version: str
    database: str
    vector_store: str

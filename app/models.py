"""
Pydantic models for request/response validation
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


# ==================== CHAT MODELS ====================

class ChatRequest(BaseModel):
    """Basic chat request"""
    message: str = Field(..., min_length=1, description="User message")
    temperature: Optional[float] = Field(default=0.7, ge=0.0, le=2.0)
    max_tokens: Optional[int] = Field(default=2048, ge=1, le=8192)


class ChatRequestExtended(ChatRequest):
    """Extended chat request with conversation context"""
    conversation_id: Optional[str] = Field(default=None)
    include_history: bool = Field(default=False)
    history_length: Optional[int] = Field(default=10, ge=1, le=50)


class ChatResponse(BaseModel):
    """Chat response"""
    response: str = Field(..., description="LLM response")
    model: str = Field(..., description="Model used")
    tokens_used: Optional[int] = Field(default=None)
    generation_time: Optional[float] = Field(default=None)
    conversation_id: Optional[str] = Field(default=None)


class ChatMessage(BaseModel):
    """Individual chat message"""
    role: str = Field(..., description="Message role (user/assistant)")
    content: str = Field(..., description="Message content")


# ==================== CONVERSATION MODELS ====================

class ConversationCreate(BaseModel):
    """Create conversation request"""
    title: Optional[str] = Field(default=None, max_length=500)
    model_source: str = Field(default="ollama")
    model_name: str = Field(default="llama3.1:8b")


class ConversationResponse(BaseModel):
    """Conversation response"""
    id: str
    title: str
    model_source: str
    model_name: str
    is_archived: bool
    message_count: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class MessageResponse(BaseModel):
    """Message response"""
    id: str
    role: str
    content: str
    model_name: Optional[str] = None
    tokens_used: Optional[int] = None
    generation_time: Optional[float] = None
    created_at: datetime

    class Config:
        from_attributes = True


class ConversationHistoryResponse(BaseModel):
    """Conversation with messages"""
    conversation: ConversationResponse
    messages: List[MessageResponse]


class ConversationListResponse(BaseModel):
    """List of conversations"""
    conversations: List[ConversationResponse]
    total: int


# ==================== AUTHENTICATION MODELS ====================

class UserRegister(BaseModel):
    """User registration request"""
    username: str = Field(min_length=3, max_length=50)
    email: str
    password: str = Field(min_length=8)
    full_name: Optional[str] = Field(default=None, max_length=255)


class UserLogin(BaseModel):
    """User login request"""
    username: str
    password: str


class UserResponse(BaseModel):
    """User response model"""
    id: str
    username: str
    email: str
    full_name: Optional[str] = None
    is_active: bool
    is_admin: bool
    created_at: datetime

    class Config:
        from_attributes = True


class TokenResponse(BaseModel):
    """JWT token response"""
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


class UserUpdate(BaseModel):
    """User update request"""
    username: Optional[str] = Field(default=None, min_length=3, max_length=50)
    email: Optional[str] = Field(default=None)
    full_name: Optional[str] = Field(default=None, max_length=255)


class PasswordChange(BaseModel):
    """Password change request"""
    current_password: str
    new_password: str = Field(min_length=8)


# ==================== FILE MODELS ====================

class FileAnalysisRequest(BaseModel):
    """File analysis request"""
    filename: str
    content: str
    analysis_type: str = Field(default="summarize")
    custom_prompt: Optional[str] = None


class FileAnalysisResponse(BaseModel):
    """File analysis response"""
    filename: str
    analysis_type: str
    result: str
    model: str
    timestamp: datetime


# ==================== MODEL MANAGEMENT ====================

class ModelInfo(BaseModel):
    """Model information"""
    name: str
    source: str
    size: Optional[str] = None
    modified: Optional[str] = None


class ModelListResponse(BaseModel):
    """List of available models"""
    models: List[ModelInfo]
    active_source: str
    active_model: str


class ModelSwitchRequest(BaseModel):
    """Switch model request"""
    source: str = Field(..., description="Model source (ollama/openai)")
    model_name: str = Field(..., description="Model name")


# ==================== STATISTICS ====================

class UsageStatsResponse(BaseModel):
    """Usage statistics response"""
    total_requests: int
    total_tokens: int
    average_response_time: float
    successful_requests: int
    failed_requests: int
    period_start: datetime
    period_end: datetime

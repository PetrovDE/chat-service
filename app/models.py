from pydantic import BaseModel, Field
from typing import List, Optional, Literal
from datetime import datetime


class ChatMessage(BaseModel):
    """Represents a single message in a conversation"""
    role: Literal["user", "assistant", "system"] = Field(
        description="The role of the message sender"
    )
    content: str = Field(
        description="The content of the message",
        min_length=1
    )
    timestamp: Optional[datetime] = Field(
        default_factory=datetime.now,
        description="When the message was created"
    )


class ChatRequest(BaseModel):
    """Request model for chat endpoint"""
    message: str = Field(
        description="The user's message to send to the LLM",
        min_length=1,
        max_length=10000
    )
    conversation_history: Optional[List[ChatMessage]] = Field(
        default=[],
        description="Previous messages in the conversation for context"
    )
    temperature: Optional[float] = Field(
        default=0.7,
        ge=0.0,
        le=2.0,
        description="Controls randomness in responses (0.0 = deterministic, 2.0 = very random)"
    )
    max_tokens: Optional[int] = Field(
        default=1000,
        ge=1,
        le=4000,
        description="Maximum number of tokens in the response"
    )


class ChatResponse(BaseModel):
    """Response model for chat endpoint"""
    response: str = Field(
        description="The LLM's response message"
    )
    model: str = Field(
        description="The model used to generate the response"
    )
    timestamp: datetime = Field(
        default_factory=datetime.now,
        description="When the response was generated"
    )
    tokens_used: Optional[int] = Field(
        default=None,
        description="Number of tokens used in generation"
    )
    conversation_id: Optional[str] = Field(
        default=None,
        description="Unique identifier for this conversation"
    )


class HealthResponse(BaseModel):
    """Health check response"""
    status: str = Field(default="healthy")
    timestamp: datetime = Field(default_factory=datetime.now)
    ollama_status: str = Field(description="Status of Ollama connection")
    model_available: bool = Field(description="Whether Llama 3.1 8b is available")


class FileAnalysisRequest(BaseModel):
    """Request model for file analysis"""
    content: str = Field(
        description="The file content to analyze",
        min_length=1
    )
    filename: str = Field(
        description="Original filename"
    )
    analysis_type: Literal["summary", "extract_data", "qa", "custom"] = Field(
        default="summary",
        description="Type of analysis to perform"
    )
    custom_prompt: Optional[str] = Field(
        default=None,
        description="Custom analysis prompt"
    )


class FileAnalysisResponse(BaseModel):
    """Response model for file analysis"""
    filename: str = Field(description="Original filename")
    analysis_type: str = Field(description="Type of analysis performed")
    result: str = Field(description="Analysis result")
    model: str = Field(description="Model used for analysis")
    timestamp: datetime = Field(default_factory=datetime.now)


class ErrorResponse(BaseModel):
    """Error response model"""
    error: str = Field(description="Error message")
    detail: Optional[str] = Field(default=None, description="Additional error details")
    timestamp: datetime = Field(default_factory=datetime.now)


class ModelSourceConfig(BaseModel):
    """Configuration for model source"""
    source: Literal["local", "api"] = Field(
        description="Source type - local Ollama or API"
    )
    api_config: Optional[dict] = Field(
        default=None,
        description="API configuration if source is 'api'"
    )


class ModelInfo(BaseModel):
    """Information about a model"""
    name: str = Field(description="Model name")
    size: Optional[int] = Field(default=None, description="Model size in bytes")
    modified: Optional[str] = Field(default=None, description="Last modified date")
    source: Literal["local", "api"] = Field(description="Model source")


class ModelsListResponse(BaseModel):
    """Response for listing models"""
    models: List[str] = Field(description="List of available model names")
    current_model: Optional[str] = Field(description="Currently active model")
    source: str = Field(description="Current source (local/api)")
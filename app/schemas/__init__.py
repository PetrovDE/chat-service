# app/schemas/__init__.py
"""Pydantic schemas for API models"""
from app.schemas.user import (
    UserBase, UserCreate, UserUpdate, UserInDB, UserResponse
)
from app.schemas.auth import (
    UserLogin, PasswordChange, PasswordChangeResponse, Token, TokenData
)
from app.schemas.chat import (
    ChatMessage, ChatResponse, StreamChunk
)
from app.schemas.conversation import (
    ConversationCreate, ConversationUpdate, ConversationResponse,
    ConversationList, MessageResponse, ConversationMessageItem, ConversationDeleteResponse
)
from app.schemas.file import (
    FileAttachRequest,
    FileAttachResponse,
    FileDeleteResponse,
    FileDetachRequest,
    FileDetachResponse,
    FileInfo,
    FileProcessingProfileInfo,
    FileProcessingStatus,
    FileQuotaInfo,
    FileReprocessRequest,
    FileReprocessResponse,
    FileUploadResponse,
)
from app.schemas.model import (
    ModelInfo, ModelsListResponse, ProviderStatus, ModelsStatusResponse
)
from app.schemas.stats import (
    UserStatsResponse, SystemStatsResponse, ObservabilityStatsResponse
)
from app.schemas.common import (
    PaginatedResponse, SuccessResponse, ErrorResponse, HealthCheck
)

__all__ = [
    # User
    "UserBase", "UserCreate", "UserUpdate", "UserInDB", "UserResponse",
    # Auth
    "UserLogin", "PasswordChange", "PasswordChangeResponse", "Token", "TokenData",
    # Chat
    "ChatMessage", "ChatResponse", "StreamChunk",
    # Conversation
    "ConversationCreate", "ConversationUpdate", "ConversationResponse",
    "ConversationList", "MessageResponse", "ConversationMessageItem", "ConversationDeleteResponse",
    # File
    "FileUploadResponse",
    "FileInfo",
    "FileProcessingStatus",
    "FileReprocessRequest",
    "FileReprocessResponse",
    "FileDeleteResponse",
    "FileProcessingProfileInfo",
    "FileQuotaInfo",
    "FileAttachRequest",
    "FileDetachRequest",
    "FileDetachResponse",
    "FileAttachResponse",
    # Models
    "ModelInfo", "ModelsListResponse", "ProviderStatus", "ModelsStatusResponse",
    # Stats
    "UserStatsResponse", "SystemStatsResponse", "ObservabilityStatsResponse",
    # Common
    "PaginatedResponse", "SuccessResponse", "ErrorResponse", "HealthCheck"
]

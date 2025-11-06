# app/schemas/__init__.py
from app.schemas.user import (
    UserBase, UserCreate, UserUpdate, UserInDB, UserResponse
)
from app.schemas.auth import (
    UserLogin, PasswordChange, Token, TokenData
)
from app.schemas.chat import (
    ChatMessage, ChatResponse, StreamChunk
)
from app.schemas.conversation import (
    ConversationCreate, ConversationUpdate, ConversationResponse,
    ConversationList, MessageResponse
)
from app.schemas.file import (
    FileUploadResponse, FileInfo, FileProcessingStatus
)
from app.schemas.common import (
    PaginatedResponse, SuccessResponse, ErrorResponse, HealthCheck
)

__all__ = [
    # User
    "UserBase", "UserCreate", "UserUpdate", "UserInDB", "UserResponse",
    # Auth
    "UserLogin", "PasswordChange", "Token", "TokenData",
    # Chat
    "ChatMessage", "ChatResponse", "StreamChunk",
    # Conversation
    "ConversationCreate", "ConversationUpdate", "ConversationResponse",
    "ConversationList", "MessageResponse",
    # File
    "FileUploadResponse", "FileInfo", "FileProcessingStatus",
    # Common
    "PaginatedResponse", "SuccessResponse", "ErrorResponse", "HealthCheck"
]

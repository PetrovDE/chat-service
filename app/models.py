# app/models.py
from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid


# ============================================================================
# AUTHENTICATION MODELS
# ============================================================================

class UserRegister(BaseModel):
    """Схема для регистрации пользователя"""
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=8, max_length=200)  # Увеличили до 200
    full_name: Optional[str] = Field(None, max_length=100)

    @field_validator('password')
    @classmethod
    def validate_password(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if len(v) > 200:
            raise ValueError('Password is too long (max 200 characters)')
        # Убрали проверку на 72 байта!
        return v

    @field_validator('username')
    @classmethod
    def validate_username(cls, v: str) -> str:
        if not v.replace('_', '').replace('-', '').isalnum():
            raise ValueError('Username can only contain letters, numbers, hyphens and underscores')
        return v


class UserLogin(BaseModel):
    """Схема для входа пользователя"""
    username: str
    password: str = Field(..., max_length=200)


class UserUpdate(BaseModel):
    """Схема для обновления профиля пользователя"""
    email: Optional[EmailStr] = None
    full_name: Optional[str] = Field(None, max_length=100)


class PasswordChange(BaseModel):
    """Схема для смены пароля"""
    old_password: str = Field(..., max_length=200)
    new_password: str = Field(..., min_length=8, max_length=200)

    @field_validator('new_password')
    @classmethod
    def validate_new_password(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        if len(v) > 200:
            raise ValueError('Password is too long (max 200 characters)')
        return v


class UserResponse(BaseModel):
    """Схема ответа с данными пользователя"""
    id: uuid.UUID
    username: str
    email: str
    full_name: Optional[str] = None
    is_active: bool
    is_admin: bool
    created_at: datetime

    class Config:
        from_attributes = True


class Token(BaseModel):
    """Схема JWT токена"""
    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    """Данные из JWT токена"""
    username: Optional[str] = None
    user_id: Optional[str] = None


# ============================================================================
# CHAT MODELS
# ============================================================================

class ChatMessage(BaseModel):
    """Схема сообщения чата"""
    message: str = Field(..., min_length=1, max_length=10000)
    conversation_id: Optional[uuid.UUID] = None
    model_source: Optional[str] = Field(None, pattern="^(ollama|openai)$")
    model_name: Optional[str] = None
    temperature: Optional[float] = Field(None, ge=0.0, le=2.0)
    max_tokens: Optional[int] = Field(None, ge=1, le=8192)


class ChatResponse(BaseModel):
    """Схема ответа чата"""
    response: str
    conversation_id: uuid.UUID
    message_id: uuid.UUID
    model_used: str
    tokens_used: Optional[int] = None
    generation_time: Optional[float] = None


class StreamChunk(BaseModel):
    """Chunk данных для streaming"""
    type: str  # 'start', 'chunk', 'done', 'error'
    content: Optional[str] = None
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None
    generation_time: Optional[float] = None
    message: Optional[str] = None


# ============================================================================
# CONVERSATION MODELS
# ============================================================================

class ConversationCreate(BaseModel):
    """Схема создания беседы"""
    title: str = Field(..., min_length=1, max_length=500)
    model_source: Optional[str] = Field("ollama", pattern="^(ollama|openai)$")
    model_name: Optional[str] = None


class ConversationUpdate(BaseModel):
    """Схема обновления беседы"""
    title: Optional[str] = Field(None, min_length=1, max_length=500)
    is_archived: Optional[bool] = None


class ConversationResponse(BaseModel):
    """Схема ответа с данными беседы"""
    id: uuid.UUID
    user_id: Optional[uuid.UUID]
    title: str
    model_source: str
    model_name: str
    is_archived: bool
    message_count: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ConversationListResponse(BaseModel):
    """Список бесед"""
    conversations: List[ConversationResponse]
    total: int


class MessageResponse(BaseModel):
    """Схема ответа с данными сообщения"""
    id: uuid.UUID
    conversation_id: uuid.UUID
    role: str
    content: str
    model_name: Optional[str] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    tokens_used: Optional[int] = None
    generation_time: Optional[float] = None
    created_at: datetime

    class Config:
        from_attributes = True


class ConversationHistoryResponse(BaseModel):
    """Схема ответа с историей беседы"""
    conversation: ConversationResponse
    messages: List[MessageResponse]


class ConversationDetailResponse(BaseModel):
    """Детальная информация о беседе"""
    id: uuid.UUID
    title: str
    model_source: str
    model_name: str
    message_count: int
    created_at: datetime
    updated_at: datetime
    messages: List[MessageResponse]

    class Config:
        from_attributes = True


# ============================================================================
# FILE MODELS
# ============================================================================

class FileUploadResponse(BaseModel):
    """Схема ответа при загрузке файла"""
    file_id: uuid.UUID
    filename: str
    original_filename: str
    file_type: str
    file_size: int
    content_preview: Optional[str] = None


class FileInfo(BaseModel):
    """Информация о файле"""
    id: uuid.UUID
    filename: str
    original_filename: str
    file_type: str
    file_size: int
    created_at: datetime

    class Config:
        from_attributes = True


class FileAnalysisRequest(BaseModel):
    """Запрос на анализ файла"""
    file_id: uuid.UUID
    query: Optional[str] = Field(None, max_length=1000)


class FileAnalysisResponse(BaseModel):
    """Результат анализа файла"""
    file_id: uuid.UUID
    analysis: str
    extracted_text: Optional[str] = None


# ============================================================================
# MODEL MANAGEMENT MODELS
# ============================================================================

class ModelSwitch(BaseModel):
    """Схема переключения модели"""
    model_source: str = Field(..., pattern="^(ollama|openai)$")
    model_name: str = Field(..., min_length=1, max_length=100)


class ModelInfo(BaseModel):
    """Информация о модели"""
    name: str
    source: str
    size: Optional[str] = None
    modified_at: Optional[datetime] = None
    is_available: bool = True


class ModelListResponse(BaseModel):
    """Список доступных моделей"""
    current_source: str
    current_model: dict
    available_models: dict


class CurrentModelResponse(BaseModel):
    """Текущая активная модель"""
    source: str
    model: str


class OllamaModelResponse(BaseModel):
    """Ответ Ollama с информацией о моделях"""
    models: List[dict]


# ============================================================================
# STATISTICS MODELS
# ============================================================================

class UsageStats(BaseModel):
    """Статистика использования (внутренняя модель)"""
    total_messages: int
    total_conversations: int
    total_tokens: Optional[int] = 0
    average_response_time: Optional[float] = None
    user_count: Optional[int] = None
    active_users_today: Optional[int] = None


class UsageStatsResponse(BaseModel):
    """Ответ со статистикой использования (для API)"""
    total_messages: int
    total_conversations: int
    total_tokens: int = 0
    average_response_time: Optional[float] = None
    user_count: Optional[int] = None
    active_users_today: Optional[int] = None
    total_users: Optional[int] = None
    models_used: Optional[Dict[str, int]] = None
    peak_usage_hour: Optional[int] = None


class UserStats(BaseModel):
    """Статистика пользователя"""
    username: str
    message_count: int
    conversation_count: int
    total_tokens: Optional[int] = 0
    average_response_time: Optional[float] = None
    first_message_date: Optional[datetime] = None
    last_message_date: Optional[datetime] = None


class UserStatsResponse(BaseModel):
    """Ответ со статистикой пользователя"""
    user_id: uuid.UUID
    username: str
    email: str
    message_count: int
    conversation_count: int
    total_tokens: int = 0
    average_response_time: Optional[float] = None
    joined_date: datetime
    last_active: Optional[datetime] = None


class DailyStats(BaseModel):
    """Статистика по дням"""
    date: datetime
    message_count: int
    conversation_count: int
    unique_users: int
    total_tokens: Optional[int] = 0


class DailyStatsResponse(BaseModel):
    """Ответ со статистикой по дням"""
    stats: List[DailyStats]
    period_start: datetime
    period_end: datetime
    total_days: int


class ModelUsageStats(BaseModel):
    """Статистика использования моделей"""
    model_source: str
    model_name: str
    usage_count: int
    total_tokens: Optional[int] = 0
    average_response_time: Optional[float] = None
    last_used: Optional[datetime] = None


class ModelUsageStatsResponse(BaseModel):
    """Ответ со статистикой использования моделей"""
    models: List[ModelUsageStats]
    total_requests: int
    most_used_model: Optional[str] = None


class SystemStats(BaseModel):
    """Системная статистика"""
    uptime_seconds: float
    total_requests: int
    successful_requests: int
    failed_requests: int
    average_response_time: float
    database_size_mb: Optional[float] = None


# ============================================================================
# SYSTEM MODELS
# ============================================================================

class HealthCheck(BaseModel):
    """Проверка здоровья системы"""
    status: str
    database: str
    ollama_host: str
    default_model: str
    ollama_status: Optional[str] = None
    ollama_available: bool = False


class AppInfo(BaseModel):
    """Информация о приложении"""
    name: str
    version: str
    default_model_source: str
    ollama_host: str
    ollama_model: str
    openai_configured: bool


class ErrorResponse(BaseModel):
    """Схема ответа с ошибкой"""
    error: str
    detail: Optional[str] = None
    status_code: int


class SuccessResponse(BaseModel):
    """Схема успешного ответа"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


# ============================================================================
# SETTINGS MODELS
# ============================================================================

class SystemSettings(BaseModel):
    """Системные настройки"""
    key: str
    value: str
    description: Optional[str] = None


class SettingsUpdate(BaseModel):
    """Обновление настроек"""
    key: str
    value: str


class SettingsResponse(BaseModel):
    """Ответ с настройками"""
    settings: Dict[str, str]


# ============================================================================
# API USAGE LOG MODELS
# ============================================================================

class APIUsageLog(BaseModel):
    """Лог использования API"""
    user_id: Optional[uuid.UUID] = None
    conversation_id: Optional[uuid.UUID] = None
    model_source: str
    model_name: str
    endpoint: str
    tokens_prompt: Optional[int] = None
    tokens_completion: Optional[int] = None
    tokens_total: Optional[int] = None
    response_time: Optional[float] = None
    status: str
    error_message: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class APIUsageLogResponse(BaseModel):
    """Ответ с логом использования API"""
    logs: List[APIUsageLog]
    total: int
    page: int
    page_size: int


# ============================================================================
# BATCH OPERATIONS
# ============================================================================

class BatchDeleteRequest(BaseModel):
    """Запрос на массовое удаление"""
    conversation_ids: List[uuid.UUID] = Field(..., min_length=1)


class BatchArchiveRequest(BaseModel):
    """Запрос на массовую архивацию"""
    conversation_ids: List[uuid.UUID] = Field(..., min_length=1)
    archive: bool = True


class BatchOperationResponse(BaseModel):
    """Результат массовой операции"""
    success: bool
    processed: int
    failed: int
    errors: Optional[List[str]] = None


# ============================================================================
# EXPORT/IMPORT MODELS
# ============================================================================

class ConversationExportRequest(BaseModel):
    """Запрос на экспорт беседы"""
    conversation_id: uuid.UUID
    format: str = Field("json", pattern="^(json|txt|md)$")


class ConversationExportResponse(BaseModel):
    """Результат экспорта беседы"""
    conversation_id: uuid.UUID
    format: str
    content: str
    filename: str


class ConversationImportRequest(BaseModel):
    """Запрос на импорт беседы"""
    title: str
    messages: List[Dict[str, str]]
    model_source: Optional[str] = "ollama"
    model_name: Optional[str] = None


# ============================================================================
# PAGINATION
# ============================================================================

class PaginationParams(BaseModel):
    """Параметры пагинации"""
    page: int = Field(1, ge=1)
    page_size: int = Field(20, ge=1, le=100)


class PaginatedResponse(BaseModel):
    """Ответ с пагинацией"""
    items: List[Any]
    total: int
    page: int
    page_size: int
    total_pages: int


# ============================================================================
# SEARCH MODELS
# ============================================================================

class SearchRequest(BaseModel):
    """Запрос на поиск"""
    query: str = Field(..., min_length=1, max_length=500)
    search_in: Optional[str] = Field("all", pattern="^(all|conversations|messages)$")
    limit: Optional[int] = Field(20, ge=1, le=100)


class SearchResult(BaseModel):
    """Результат поиска"""
    type: str  # 'conversation' or 'message'
    id: uuid.UUID
    conversation_id: uuid.UUID
    title: Optional[str] = None
    content: str
    snippet: str
    relevance: float
    created_at: datetime


class SearchResponse(BaseModel):
    """Ответ на поиск"""
    results: List[SearchResult]
    total: int
    query: str
    execution_time: float


# ============================================================================
# WEBHOOK MODELS
# ============================================================================

class WebhookConfig(BaseModel):
    """Конфигурация webhook"""
    url: str = Field(..., pattern="^https?://.*")
    events: List[str] = Field(..., min_length=1)
    secret: Optional[str] = None
    enabled: bool = True


class WebhookEvent(BaseModel):
    """Событие webhook"""
    event_type: str
    timestamp: datetime
    data: Dict[str, Any]
    user_id: Optional[uuid.UUID] = None


# ============================================================================
# NOTIFICATION MODELS
# ============================================================================

class NotificationPreferences(BaseModel):
    """Предпочтения уведомлений"""
    email_notifications: bool = True
    conversation_updates: bool = True
    system_announcements: bool = True


class Notification(BaseModel):
    """Уведомление"""
    id: uuid.UUID
    user_id: uuid.UUID
    type: str
    title: str
    message: str
    read: bool = False
    created_at: datetime
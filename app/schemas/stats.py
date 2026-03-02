from typing import Any, Dict

from pydantic import BaseModel


class UserStatsResponse(BaseModel):
    total_conversations: int
    total_messages: int
    total_files: int
    recent_messages_7d: int
    account_created: str


class SystemStatsResponse(BaseModel):
    total_users: int
    active_users_30d: int
    total_conversations: int
    total_messages: int


class ObservabilityStatsResponse(BaseModel):
    metrics: Dict[str, Any]
    file_processing: Dict[str, Any]

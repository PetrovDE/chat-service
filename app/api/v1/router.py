# app/api/v1/router.py
from fastapi import APIRouter

from app.api.v1.endpoints import (
    auth,
    chat,
    conversations,
    files,
    models,
    stats
)

api_router = APIRouter()

# Include all routers
api_router.include_router(auth.router, prefix="/auth", tags=["Authentication"])
api_router.include_router(chat.router, prefix="/chat", tags=["Chat"])
api_router.include_router(conversations.router, prefix="/conversations", tags=["Conversations"])
api_router.include_router(files.router, prefix="/files", tags=["Files"])
api_router.include_router(models.router, prefix="/models", tags=["Models Management"])
api_router.include_router(stats.router, prefix="/stats", tags=["Statistics"])

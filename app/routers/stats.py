# app/routers/stats.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db, crud
from app.database.models import User
from app.routers.auth import get_current_user, get_current_user_optional
from app import models
import logging

router = APIRouter()  # <-- БЕЗ prefix="/stats"
logger = logging.getLogger(__name__)


@router.get("/usage", response_model=models.UsageStatsResponse)
async def get_usage_stats(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user_optional)
):
    """Получить статистику использования"""
    try:
        if current_user:
            # Статистика конкретного пользователя
            stats = await crud.get_user_stats(db, current_user.id)
            
            return models.UsageStatsResponse(
                total_messages=stats.get("message_count", 0),
                total_conversations=stats.get("conversation_count", 0),
                total_tokens=stats.get("total_tokens", 0),
                average_response_time=stats.get("average_response_time"),
                user_count=1
            )
        else:
            # Глобальная статистика
            stats = await crud.get_global_stats(db)
            
            return models.UsageStatsResponse(
                total_messages=stats.get("message_count", 0),
                total_conversations=stats.get("conversation_count", 0),
                total_tokens=stats.get("total_tokens", 0),
                average_response_time=stats.get("average_response_time"),
                user_count=stats.get("user_count", 0)
            )
            
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get statistics")


@router.get("/user", response_model=models.UserStatsResponse)
async def get_user_stats(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Получить статистику текущего пользователя"""
    try:
        stats = await crud.get_user_stats(db, current_user.id)
        
        return models.UserStatsResponse(
            user_id=current_user.id,
            username=current_user.username,
            email=current_user.email,
            message_count=stats.get("message_count", 0),
            conversation_count=stats.get("conversation_count", 0),
            total_tokens=stats.get("total_tokens", 0),
            average_response_time=stats.get("average_response_time"),
            joined_date=current_user.created_at,
            last_active=current_user.updated_at
        )
        
    except Exception as e:
        logger.error(f"Error getting user stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get user statistics")
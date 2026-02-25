# app/api/v1/endpoints/stats.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from datetime import datetime, timedelta
from typing import Dict, Any

from app.db.session import get_db
from app.db.models import User, Conversation, Message, File
from app.api.dependencies import get_current_user
from app.observability.metrics import snapshot_metrics
from app.services.file import get_file_processing_worker_stats

router = APIRouter()


@router.get("/user", response_model=Dict[str, Any])
async def get_user_stats(
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Get user statistics"""
    # Count conversations
    conv_result = await db.execute(
        select(func.count(Conversation.id))
        .where(Conversation.user_id == current_user.id)
    )
    total_conversations = conv_result.scalar()

    # Count messages
    msg_result = await db.execute(
        select(func.count(Message.id))
        .join(Conversation)
        .where(Conversation.user_id == current_user.id)
    )
    total_messages = msg_result.scalar()

    # Count files
    file_result = await db.execute(
        select(func.count(File.id))
        .where(File.user_id == current_user.id)
    )
    total_files = file_result.scalar()

    # Recent activity (last 7 days)
    week_ago = datetime.utcnow() - timedelta(days=7)
    recent_result = await db.execute(
        select(func.count(Message.id))
        .join(Conversation)
        .where(
            Conversation.user_id == current_user.id,
            Message.timestamp >= week_ago
        )
    )
    recent_messages = recent_result.scalar()

    return {
        "total_conversations": total_conversations,
        "total_messages": total_messages,
        "total_files": total_files,
        "recent_messages_7d": recent_messages,
        "account_created": current_user.created_at.isoformat()
    }


@router.get("/system")
async def get_system_stats(
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Get system statistics (admin only)"""
    if not current_user.is_admin:
        return {"error": "Admin access required"}

    # Total users
    users_result = await db.execute(select(func.count(User.id)))
    total_users = users_result.scalar()

    # Active users (last 30 days)
    month_ago = datetime.utcnow() - timedelta(days=30)
    active_result = await db.execute(
        select(func.count(func.distinct(Conversation.user_id)))
        .where(Conversation.updated_at >= month_ago)
    )
    active_users = active_result.scalar()

    # Total conversations
    conv_result = await db.execute(select(func.count(Conversation.id)))
    total_conversations = conv_result.scalar()

    # Total messages
    msg_result = await db.execute(select(func.count(Message.id)))
    total_messages = msg_result.scalar()

    return {
        "total_users": total_users,
        "active_users_30d": active_users,
        "total_conversations": total_conversations,
        "total_messages": total_messages
    }


@router.get("/observability")
async def get_observability_stats(
        current_user: User = Depends(get_current_user)
):
    """Get in-memory observability metrics (admin only)."""
    if not current_user.is_admin:
        return {"error": "Admin access required"}

    return {
        "metrics": snapshot_metrics(),
        "file_processing": get_file_processing_worker_stats(),
    }

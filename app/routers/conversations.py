# app/routers/conversations.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from app.database import get_db, crud
from app.database.models import User
from app.routers.auth import get_current_user, get_current_user_optional
from app import models
import logging
import uuid

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/", response_model=List[models.ConversationResponse])
async def get_conversations(
    db: AsyncSession = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional)
):
    """Получить список бесед пользователя"""
    try:
        user_id = current_user.id if current_user else None
        conversations = await crud.get_user_conversations(db, user_id)
        return conversations
    except Exception as e:
        logger.error(f"Error getting conversations: {e}")
        raise HTTPException(status_code=500, detail="Failed to get conversations")


@router.get("/{conversation_id}", response_model=models.ConversationHistoryResponse)
async def get_conversation(
    conversation_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional)
):
    """Получить беседу с историей сообщений"""
    try:
        user_id = current_user.id if current_user else None
        conversation = await crud.get_conversation(db, conversation_id)
        
        if not conversation:
            raise HTTPException(status_code=404, detail="Conversation not found")
        
        if conversation.user_id != user_id:
            raise HTTPException(status_code=403, detail="Access denied")
        
        messages = await crud.get_conversation_messages(db, conversation_id)
        
        return {
            "conversation": conversation,
            "messages": messages
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting conversation: {e}")
        raise HTTPException(status_code=500, detail="Failed to get conversation")


@router.patch("/{conversation_id}", response_model=models.ConversationResponse)
async def update_conversation(
    conversation_id: uuid.UUID,
    conversation_update: models.ConversationUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional)
):
    """Обновить беседу"""
    try:
        user_id = current_user.id if current_user else None
        conversation = await crud.get_conversation(db, conversation_id)
        
        if not conversation:
            raise HTTPException(status_code=404, detail="Conversation not found")
        
        if conversation.user_id != user_id:
            raise HTTPException(status_code=403, detail="Access denied")
        
        updated = await crud.update_conversation(
            db, conversation_id, 
            title=conversation_update.title,
            is_archived=conversation_update.is_archived
        )
        
        return updated
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating conversation: {e}")
        raise HTTPException(status_code=500, detail="Failed to update conversation")


@router.delete("/{conversation_id}")
async def delete_conversation(
    conversation_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional)
):
    """Удалить беседу"""
    try:
        user_id = current_user.id if current_user else None
        conversation = await crud.get_conversation(db, conversation_id)
        
        if not conversation:
            raise HTTPException(status_code=404, detail="Conversation not found")
        
        if conversation.user_id != user_id:
            raise HTTPException(status_code=403, detail="Access denied")
        
        await crud.delete_conversation(db, conversation_id)
        return {"success": True, "message": "Conversation deleted"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting conversation: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete conversation")
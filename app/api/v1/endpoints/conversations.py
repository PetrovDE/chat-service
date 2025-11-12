# app/api/v1/endpoints/conversations.py
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from uuid import UUID

from app.db.session import get_db
from app.db.models import User
from app.schemas import ConversationResponse, ConversationUpdate
from app.api.dependencies import get_current_user
from app.crud import crud_conversation, crud_message

import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_model=List[ConversationResponse])
async def get_conversations(
        skip: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=100),
        include_archived: bool = False,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Get user conversations"""
    logger.info(f"📋 Getting conversations for user {current_user.username} (ID: {current_user.id})")

    try:
        conversations = await crud_conversation.get_user_conversations(
            db,
            user_id=current_user.id,
            skip=skip,
            limit=limit,
            include_archived=include_archived
        )
        logger.info(f"✅ Found {len(conversations)} conversations")
        return conversations
    except Exception as e:
        logger.error(f"❌ Error getting conversations: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/{conversation_id}/messages")
async def get_conversation_messages(
        conversation_id: UUID,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Get all messages in a conversation"""
    conversation = await crud_conversation.get(db, id=conversation_id)

    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")

    if conversation.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")

    messages = await crud_message.get_conversation_messages(
        db,
        conversation_id=conversation_id
    )

    return [
        {
            "id": str(msg.id),
            "role": msg.role,
            "content": msg.content,
            "timestamp": msg.timestamp.isoformat() if msg.timestamp else None
        }
        for msg in messages
    ]


@router.patch("/{conversation_id}", response_model=ConversationResponse)
async def update_conversation(
        conversation_id: UUID,
        update_data: ConversationUpdate,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Update conversation (e.g., title, archive status)"""
    conversation = await crud_conversation.get(db, id=conversation_id)

    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")

    if conversation.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")

    updated_conversation = await crud_conversation.update(
        db,
        db_obj=conversation,
        obj_in=update_data
    )

    return updated_conversation


@router.delete("/{conversation_id}")
async def delete_conversation(
        conversation_id: UUID,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Delete a conversation"""
    conversation = await crud_conversation.get(db, id=conversation_id)

    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")

    if conversation.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")

    await crud_conversation.remove(db, id=conversation_id)

    return {"status": "deleted", "conversation_id": str(conversation_id)}

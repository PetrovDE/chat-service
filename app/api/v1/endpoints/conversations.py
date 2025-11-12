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
from app.api.dependencies import get_current_user_optional
from app.schemas import ConversationCreate

import logging
from app.core.logging import setup_logging

setup_logging()
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


@router.get("/{conversation_id}", response_model=ConversationResponse)
async def get_conversation(
        conversation_id: UUID,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Get conversation with messages"""
    conversation = await crud_conversation.get_with_messages(
        db,
        conversation_id=conversation_id,
        user_id=current_user.id
    )

    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found"
        )

    return conversation


@router.post("/", response_model=ConversationResponse)
async def create_conversation(
        conversation_data: ConversationCreate,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user_optional)
):
    """
    Create new conversation (works for authenticated and anonymous users)
    Создать новый разговор (работает для авторизованных и анонимных пользователей)
    """
    user_id = current_user.id if current_user else None
    username = current_user.username if current_user else "anonymous"

    logger.info(f"➕ Creating conversation for {username}: {conversation_data.title}")

    try:
        # Create conversation
        conversation = await crud_conversation.create_for_user(
            db=db,
            obj_in=conversation_data,
            user_id=user_id
        )

        logger.info(f"✅ Created conversation {conversation.id}")

        return conversation

    except Exception as e:
        logger.error(f"❌ Create conversation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create conversation: {str(e)}"
        )


@router.put("/{conversation_id}", response_model=ConversationResponse)
async def update_conversation(
        conversation_id: UUID,
        update_data: ConversationUpdate,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Update conversation (title, archived status)"""
    conversation = await crud_conversation.get(db, id=conversation_id)

    if not conversation or conversation.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found"
        )

    conversation = await crud_conversation.update(
        db,
        db_obj=conversation,
        obj_in=update_data
    )
    return conversation


@router.delete("/{conversation_id}")
async def delete_conversation(
        conversation_id: UUID,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Delete conversation"""
    conversation = await crud_conversation.get(db, id=conversation_id)

    if not conversation or conversation.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found"
        )

    await crud_conversation.remove(db, id=conversation_id)
    return {"message": "Conversation deleted"}


@router.post("/{conversation_id}/archive")
async def archive_conversation(
        conversation_id: UUID,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Archive conversation"""
    conversation = await crud_conversation.archive(
        db,
        conversation_id=conversation_id,
        user_id=current_user.id
    )

    if not conversation:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conversation not found"
        )

    return {"message": "Conversation archived"}

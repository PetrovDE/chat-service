"""
Conversations router - handles conversation management
"""

from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from uuid import UUID
import logging

from ..models import (
    ConversationCreate, ConversationResponse, MessageResponse,
    ConversationHistoryResponse, ConversationListResponse
)
from ..database import get_db, crud
from ..auth import get_optional_user
from ..database.models import User

router = APIRouter(prefix="/conversations", tags=["conversations"])
logger = logging.getLogger(__name__)


@router.post("", response_model=ConversationResponse, status_code=status.HTTP_201_CREATED)
async def create_conversation(
    conversation: ConversationCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_optional_user)
):
    """Create a new conversation"""
    try:
        user_id = current_user.id if current_user else None

        conv = await crud.create_conversation(
            db=db,
            user_id=user_id,
            model_source=conversation.model_source,
            model_name=conversation.model_name,
            title=conversation.title
        )

        return ConversationResponse(
            id=str(conv.id),
            title=conv.title,
            model_source=conv.model_source,
            model_name=conv.model_name,
            is_archived=conv.is_archived,
            message_count=conv.message_count,
            created_at=conv.created_at,
            updated_at=conv.updated_at
        )

    except Exception as e:
        logger.error(f"Error creating conversation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=ConversationListResponse)
async def list_conversations(
    skip: int = 0,
    limit: int = 50,
    include_archived: bool = False,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_optional_user)
):
    """Get list of all conversations"""
    try:
        user_id = current_user.id if current_user else None

        conversations = await crud.get_user_conversations(
            db=db,
            user_id=user_id,
            skip=skip,
            limit=limit,
            include_archived=include_archived
        )

        conversation_list = [
            ConversationResponse(
                id=str(conv.id),
                title=conv.title,
                model_source=conv.model_source,
                model_name=conv.model_name,
                is_archived=conv.is_archived,
                message_count=conv.message_count,
                created_at=conv.created_at,
                updated_at=conv.updated_at
            )
            for conv in conversations
        ]

        return ConversationListResponse(
            conversations=conversation_list,
            total=len(conversation_list)
        )

    except Exception as e:
        logger.error(f"Error listing conversations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{conversation_id}", response_model=ConversationHistoryResponse)
async def get_conversation_history(
    conversation_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_optional_user)
):
    """Get conversation with all messages"""
    try:
        conv_uuid = UUID(conversation_id)

        conversation = await crud.get_conversation(db, conv_uuid)
        if not conversation:
            raise HTTPException(status_code=404, detail="Conversation not found")

        # Check access
        if current_user:
            if conversation.user_id != current_user.id:
                raise HTTPException(status_code=403, detail="Access denied")
        else:
            if conversation.user_id is not None:
                raise HTTPException(status_code=403, detail="Access denied")

        messages = await crud.get_conversation_messages(db, conv_uuid)

        message_list = [
            MessageResponse(
                id=str(msg.id),
                role=msg.role,
                content=msg.content,
                model_name=msg.model_name,
                tokens_used=msg.tokens_used,
                generation_time=msg.generation_time,
                created_at=msg.created_at
            )
            for msg in messages
        ]

        return ConversationHistoryResponse(
            conversation=ConversationResponse(
                id=str(conversation.id),
                title=conversation.title,
                model_source=conversation.model_source,
                model_name=conversation.model_name,
                is_archived=conversation.is_archived,
                message_count=conversation.message_count,
                created_at=conversation.created_at,
                updated_at=conversation.updated_at
            ),
            messages=message_list
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting conversation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_conversation(
    conversation_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_optional_user)
):
    """Delete a conversation and all its messages"""
    try:
        conv_uuid = UUID(conversation_id)

        conversation = await crud.get_conversation(db, conv_uuid)
        if not conversation:
            raise HTTPException(status_code=404, detail="Conversation not found")

        # Check access
        if current_user:
            if conversation.user_id != current_user.id:
                raise HTTPException(status_code=403, detail="Access denied")
        else:
            if conversation.user_id is not None:
                raise HTTPException(status_code=403, detail="Access denied")

        deleted = await crud.delete_conversation(db, conv_uuid)
        if not deleted:
            raise HTTPException(status_code=404, detail="Conversation not found")

        return None

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting conversation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/{conversation_id}", response_model=ConversationResponse)
async def update_conversation(
    conversation_id: str,
    title: Optional[str] = None,
    is_archived: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_optional_user)
):
    """Update conversation (title, archive status)"""
    try:
        conv_uuid = UUID(conversation_id)

        conversation = await crud.get_conversation(db, conv_uuid)
        if not conversation:
            raise HTTPException(status_code=404, detail="Conversation not found")

        # Check access
        if current_user:
            if conversation.user_id != current_user.id:
                raise HTTPException(status_code=403, detail="Access denied")
        else:
            if conversation.user_id is not None:
                raise HTTPException(status_code=403, detail="Access denied")

        conversation = await crud.update_conversation(
            db=db,
            conversation_id=conv_uuid,
            title=title,
            is_archived=is_archived
        )

        if not conversation:
            raise HTTPException(status_code=404, detail="Conversation not found")

        return ConversationResponse(
            id=str(conversation.id),
            title=conversation.title,
            model_source=conversation.model_source,
            model_name=conversation.model_name,
            is_archived=conversation.is_archived,
            message_count=conversation.message_count,
            created_at=conversation.created_at,
            updated_at=conversation.updated_at
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating conversation: {e}")
        raise HTTPException(status_code=500, detail=str(e))
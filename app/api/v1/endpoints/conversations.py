# app/api/v1/endpoints/conversations.py
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List
from uuid import UUID

from app.db.session import get_db
from app.db.models import User
from app.db.models.conversation_file import ConversationFile
from app.schemas import (
    ConversationDeleteResponse,
    ConversationMessageItem,
    ConversationResponse,
    ConversationUpdate,
)
from app.api.dependencies import get_current_user
from app.crud import crud_conversation, crud_file, crud_message
from app.rag.vector_store import VectorStoreManager

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


@router.get("/{conversation_id}/messages", response_model=List[ConversationMessageItem])
async def get_conversation_messages(
        conversation_id: UUID,
        skip: int = Query(0, ge=0),
        limit: int = Query(200, ge=1, le=1000),
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
        conversation_id=conversation_id,
        skip=skip,
        limit=limit,
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


@router.delete("/{conversation_id}", response_model=ConversationDeleteResponse)
async def delete_conversation(
        conversation_id: UUID,
        delete_orphan_files: bool = Query(True, description="Also delete files left without any conversation links"),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Delete a conversation"""
    conversation = await crud_conversation.get(db, id=conversation_id)

    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")

    if conversation.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied")

    linked_file_ids = await crud_file.get_conversation_file_ids(db, conversation_id=conversation_id)
    await crud_conversation.remove(db, id=conversation_id)

    if delete_orphan_files and linked_file_ids:
        try:
            vector_store = VectorStoreManager()
        except Exception:
            vector_store = None
            logger.warning("Vector store is unavailable during orphan cleanup", exc_info=True)

        for file_id in linked_file_ids:
            links_left_stmt = select(func.count(ConversationFile.id)).where(ConversationFile.file_id == file_id)
            links_left_result = await db.execute(links_left_stmt)
            links_left = int(links_left_result.scalar() or 0)
            if links_left > 0:
                continue

            file_obj = await crud_file.get(db, id=file_id)
            if not file_obj:
                continue

            try:
                if vector_store is not None:
                    try:
                        vector_store.delete_by_metadata({"file_id": str(file_id)})
                    except Exception:
                        logger.warning("Vector cleanup failed for orphan file_id=%s", file_id, exc_info=True)

                file_path = Path(file_obj.path)
                await crud_file.remove(db, id=file_id)
                try:
                    if file_path.exists():
                        file_path.unlink()
                except Exception:
                    logger.warning("Filesystem cleanup failed for orphan file_id=%s path=%s", file_id, file_path)
            except Exception:
                logger.warning("Orphan file cleanup failed for file_id=%s", file_id, exc_info=True)

    return {"status": "deleted", "conversation_id": str(conversation_id)}

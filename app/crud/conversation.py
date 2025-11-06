# app/crud/conversation.py
from typing import List, Optional
from uuid import UUID
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.db.models.conversation import Conversation
from app.schemas.conversation import ConversationCreate, ConversationUpdate


class CRUDConversation(CRUDBase[Conversation, ConversationCreate, ConversationUpdate]):
    async def get_user_conversations(
            self,
            db: AsyncSession,
            *,
            user_id: UUID,
            skip: int = 0,
            limit: int = 100,
            include_archived: bool = False
    ) -> List[Conversation]:
        """Get conversations for a specific user"""
        query = select(Conversation).where(
            Conversation.user_id == user_id
        )

        if not include_archived:
            query = query.where(Conversation.is_archived == False)

        query = query.order_by(Conversation.updated_at.desc())
        query = query.offset(skip).limit(limit)

        result = await db.execute(query)
        return result.scalars().all()

    async def get_with_messages(
            self,
            db: AsyncSession,
            *,
            conversation_id: UUID,
            user_id: Optional[UUID] = None
    ) -> Optional[Conversation]:
        """Get conversation with all messages"""
        query = select(Conversation).where(
            Conversation.id == conversation_id
        ).options(selectinload(Conversation.messages))

        if user_id:
            query = query.where(Conversation.user_id == user_id)

        result = await db.execute(query)
        return result.scalar_one_or_none()

    async def create_for_user(
            self,
            db: AsyncSession,
            *,
            obj_in: ConversationCreate,
            user_id: UUID
    ) -> Conversation:
        """Create a new conversation for a user"""
        db_obj = Conversation(
            user_id=user_id,
            title=obj_in.title,
            model_source=obj_in.model_source,
            model_name=obj_in.model_name
        )
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    async def archive(
            self,
            db: AsyncSession,
            *,
            conversation_id: UUID,
            user_id: UUID
    ) -> Optional[Conversation]:
        """Archive a conversation"""
        conversation = await self.get_with_messages(
            db, conversation_id=conversation_id, user_id=user_id
        )
        if conversation:
            conversation.is_archived = True
            await db.commit()
            await db.refresh(conversation)
        return conversation

    async def update_message_count(
            self,
            db: AsyncSession,
            *,
            conversation_id: UUID
    ) -> None:
        """Update message count for a conversation"""
        result = await db.execute(
            select(func.count()).where(
                and_(
                    Conversation.id == conversation_id,
                    Conversation.messages.any()
                )
            )
        )
        count = result.scalar()

        conversation = await self.get(db, id=conversation_id)
        if conversation:
            conversation.message_count = count
            await db.commit()


# Create instance
crud_conversation = CRUDConversation(Conversation)

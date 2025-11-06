# app/crud/message.py
from typing import List, Optional
from uuid import UUID
from datetime import datetime
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.db.models.message import Message


class CRUDMessage(CRUDBase[Message, dict, dict]):
    async def create_message(
            self,
            db: AsyncSession,
            *,
            conversation_id: UUID,
            role: str,
            content: str,
            model_name: Optional[str] = None,
            temperature: Optional[float] = None,
            max_tokens: Optional[int] = None,
            tokens_used: Optional[int] = None,
            generation_time: Optional[float] = None
    ) -> Message:
        """Create a new message in conversation"""
        db_obj = Message(
            conversation_id=conversation_id,
            role=role,
            content=content,
            model_name=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            tokens_used=tokens_used,
            generation_time=generation_time,
            timestamp=datetime.utcnow()
        )
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)

        # Update conversation's updated_at
        from app.crud.conversation import crud_conversation
        conversation = await crud_conversation.get(db, id=conversation_id)
        if conversation:
            conversation.updated_at = datetime.utcnow()
            conversation.message_count += 1
            await db.commit()

        return db_obj

    async def get_conversation_messages(
            self,
            db: AsyncSession,
            *,
            conversation_id: UUID,
            limit: Optional[int] = None
    ) -> List[Message]:
        """Get all messages in a conversation"""
        query = select(Message).where(
            Message.conversation_id == conversation_id
        ).order_by(Message.timestamp)

        if limit:
            query = query.limit(limit)

        result = await db.execute(query)
        return result.scalars().all()

    async def get_last_messages(
            self,
            db: AsyncSession,
            *,
            conversation_id: UUID,
            count: int = 10
    ) -> List[Message]:
        """Get last N messages from conversation"""
        query = select(Message).where(
            Message.conversation_id == conversation_id
        ).order_by(Message.timestamp.desc()).limit(count)

        result = await db.execute(query)
        messages = result.scalars().all()
        # Return in chronological order
        return list(reversed(messages))


# Create instance
crud_message = CRUDMessage(Message)

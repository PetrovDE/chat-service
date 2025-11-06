# app/conversations.py

from typing import List, Optional
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.db.models import Conversation, Message
from app.crud.base import CRUDBase

conversation_crud = CRUDBase(Conversation)
message_crud = CRUDBase(Message)

class ConversationsManager:
    def __init__(self):
        pass  # Все параметры вызываются на уровне методов

    async def create_conversation(self, db: AsyncSession, user_id: str, title: str, model_source: str = "local", model_name: str = None) -> Conversation:
        # Можно расширить: сохранять source/mode/model в metadata для RAG
        new_conv = await conversation_crud.create(
            db,
            user_id=user_id,
            title=title,
            created_at=datetime.utcnow(),
            model_source=model_source,
            model_name=model_name,
        )
        return new_conv

    async def list_conversations_by_user(self, db: AsyncSession, user_id: str) -> List[Conversation]:
        stmt = select(Conversation).where(Conversation.user_id == user_id)
        result = await db.execute(stmt)
        return result.scalars().all()

    async def add_message(self, db: AsyncSession, conversation_id: str, role: str, content: str) -> Message:
        msg = await message_crud.create(
            db,
            conversation_id=conversation_id,
            role=role,
            content=content,
            timestamp=datetime.utcnow()
        )
        return msg

    async def get_messages(self, db: AsyncSession, conversation_id: str, limit: int = 50) -> List[Message]:
        stmt = (
            select(Message)
            .where(Message.conversation_id == conversation_id)
            .order_by(Message.timestamp.desc())
            .limit(limit)
        )
        result = await db.execute(stmt)
        return result.scalars().all()


# Create instance
conversations_manager = ConversationsManager()

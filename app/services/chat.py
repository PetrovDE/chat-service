# app/chat.py

from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from app.services.conversation import conversations_manager
from app.services.llm.manager import llm_manager

class ChatManager:
    def __init__(self):
        pass

    async def send_message(
        self,
        db: AsyncSession,
        conversation_id: str,
        user_id: str,
        prompt: str,
        model_source: str = "local",
        model_name: str = None
    ) -> Dict[str, Any]:
        # 1. Получить режим/модель для диалога
        llm_manager.switch_mode(model_source)
        if model_name:
            llm_manager.switch_model(model_name)

        # 2. Получить историю сообщений, если нужно: context = ...
        messages = await conversations_manager.get_messages(db, conversation_id, limit=30)
        history = [{"role": msg.role, "content": msg.content} for msg in reversed(messages)]

        # 3. Собрать prompt, сгенерировать ответ
        # (Можно расширять системный prompt/инструкции поводя model_source/model_name)
        response = llm_manager.generate(prompt, parameters={"history": history if history else []})

        # 4. Сохранить новый message
        await conversations_manager.add_message(db, conversation_id, role="user", content=prompt)
        await conversations_manager.add_message(db, conversation_id, role="assistant", content=response)

        return {"response": response}

    async def chat_history(self, db: AsyncSession, conversation_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        messages = await conversations_manager.get_messages(db, conversation_id, limit)
        return [{"role": msg.role, "content": msg.content, "timestamp": msg.timestamp} for msg in reversed(messages)]

chat_manager = ChatManager()

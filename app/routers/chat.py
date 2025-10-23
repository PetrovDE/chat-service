# app/routers/chat.py
# ⭐ ПОЛНОСТЬЮ ИСПРАВЛЕННЫЙ - RAG РАБОТАЕТ В ЧАТЕ ⭐

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db, crud
from app import models
from app.llm_manager import llm_manager
from app.routers.auth import get_current_user_optional
from app.database.models import User
from app.rag.retriever import rag_retriever
from typing import Optional
import logging
import json
import uuid
import asyncio
from datetime import datetime

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/stream")
async def chat_stream(
        chat_data: models.ChatMessage,
        db: AsyncSession = Depends(get_db),
        current_user: Optional[User] = Depends(get_current_user_optional)
):
    """
    Чат с потоковым ответом + автоматический RAG
    """
    try:
        user_id = current_user.id if current_user else None
        username = current_user.username if current_user else "anonymous"

        logger.info(f"📨 Chat request from {username}: {chat_data.message[:50]}...")

        # Получить или создать беседу
        if chat_data.conversation_id:
            conversation = await crud.get_conversation(db, chat_data.conversation_id)
            if not conversation:
                raise HTTPException(status_code=404, detail="Conversation not found")
            if conversation.user_id != user_id:
                raise HTTPException(status_code=403, detail="Access denied")
            conversation_id = conversation.id
        else:
            title = chat_data.message[:100] if len(chat_data.message) <= 100 else chat_data.message[:97] + "..."
            conversation = await crud.create_conversation(
                db=db,
                user_id=user_id,
                title=title,
                model_source=chat_data.model_source or "ollama",
                model_name=chat_data.model_name or llm_manager.ollama_model
            )
            conversation_id = conversation.id
            logger.info(f"✅ Created conversation {conversation_id}")

        # Сохранить сообщение пользователя
        await crud.create_message(
            db=db,
            conversation_id=conversation_id,
            role="user",
            content=chat_data.message
        )

        # Получить историю
        messages = await crud.get_conversation_messages(db, conversation_id)
        conversation_history = [
            {"role": msg.role, "content": msg.content}
            for msg in messages[:-1]
        ]

        # 🆕 ПРОВЕРКА ФАЙЛОВ ДЛЯ RAG
        user_files = []
        rag_context_used = False
        final_prompt = chat_data.message

        if user_id:
            try:
                user_files = await crud.get_user_files(db, user_id)
                logger.info(f"📂 User has {len(user_files)} files")
            except Exception as e:
                logger.warning(f"⚠️ Could not fetch files: {e}")

        # 🆕 ИСПОЛЬЗОВАНИЕ RAG ЕСЛИ ЕСТЬ ФАЙЛЫ
        if user_files:
            try:
                logger.info("🤖 Retrieving RAG context...")

                # ✅ ИСПРАВЛЕНО: asyncio.to_thread для синхронного метода
                context_docs = await asyncio.to_thread(
                    rag_retriever.retrieve_context,
                    query=chat_data.message,
                    k=3,
                    filter={'user_id': str(user_id)} if user_id else None
                )

                if context_docs:
                    # Построить промпт с контекстом
                    final_prompt = await asyncio.to_thread(
                        rag_retriever.build_context_prompt,
                        query=chat_data.message,
                        context_documents=context_docs
                    )
                    rag_context_used = True
                    logger.info(f"✅ Using RAG with {len(context_docs)} documents")
                else:
                    logger.info("ℹ️ No relevant context found")

            except Exception as e:
                logger.warning(f"⚠️ RAG retrieval failed: {e}")

        # Генерировать ID для ответа
        assistant_message_id = uuid.uuid4()

        # Функция-генератор для SSE
        async def event_stream():
            full_response = ""
            start_time = datetime.utcnow()

            try:
                # Метаданные
                metadata = {
                    'type': 'start',
                    'conversation_id': str(conversation_id),
                    'message_id': str(assistant_message_id),
                    'rag_enabled': rag_context_used
                }
                yield f"data: {json.dumps(metadata)}\n\n"

                # Генерация ответа
                async for chunk in llm_manager.generate_response_stream(
                        prompt=final_prompt,
                        model_source=chat_data.model_source,
                        model_name=chat_data.model_name,
                        temperature=chat_data.temperature or 0.7,
                        max_tokens=chat_data.max_tokens or 2000,
                        conversation_history=conversation_history if not rag_context_used else None
                ):
                    full_response += chunk
                    yield f"data: {json.dumps({'type': 'chunk', 'content': chunk})}\n\n"

                # Вычислить время
                end_time = datetime.utcnow()
                generation_time = (end_time - start_time).total_seconds()

                # Сохранить ответ
                await crud.create_message(
                    db=db,
                    conversation_id=conversation_id,
                    role="assistant",
                    content=full_response,
                    model_name=chat_data.model_name or llm_manager.ollama_model,
                    temperature=chat_data.temperature,
                    max_tokens=chat_data.max_tokens,
                    generation_time=generation_time
                )

                # Завершение
                completion_data = {
                    'type': 'done',
                    'generation_time': generation_time,
                    'rag_used': rag_context_used
                }
                yield f"data: {json.dumps(completion_data)}\n\n"

                logger.info(
                    f"✅ Streaming completed in {generation_time:.2f}s {'with RAG' if rag_context_used else 'without RAG'}")

            except Exception as e:
                logger.error(f"❌ Streaming error: {e}")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no"
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Chat stream error: {e}")
        raise HTTPException(status_code=500, detail=f"Chat failed: {str(e)}")


@router.post("/", response_model=models.ChatResponse)
async def chat(
        chat_data: models.ChatMessage,
        db: AsyncSession = Depends(get_db),
        current_user: Optional[User] = Depends(get_current_user_optional)
):
    """
    Чат без streaming (для обратной совместимости)
    """
    try:
        user_id = current_user.id if current_user else None
        username = current_user.username if current_user else "anonymous"

        logger.info(f"📨 Chat request (non-stream) from {username}")

        # Получить или создать беседу
        if chat_data.conversation_id:
            conversation = await crud.get_conversation(db, chat_data.conversation_id)
            if not conversation:
                raise HTTPException(status_code=404, detail="Conversation not found")
            if conversation.user_id != user_id:
                raise HTTPException(status_code=403, detail="Access denied")
            conversation_id = conversation.id
        else:
            title = chat_data.message[:100] if len(chat_data.message) <= 100 else chat_data.message[:97] + "..."
            conversation = await crud.create_conversation(
                db=db,
                user_id=user_id,
                title=title,
                model_source=chat_data.model_source or "ollama",
                model_name=chat_data.model_name or llm_manager.ollama_model
            )
            conversation_id = conversation.id

        # Сохранить сообщение
        await crud.create_message(
            db=db,
            conversation_id=conversation_id,
            role="user",
            content=chat_data.message
        )

        # История
        messages = await crud.get_conversation_messages(db, conversation_id)
        conversation_history = [
            {"role": msg.role, "content": msg.content}
            for msg in messages[:-1]
        ]

        # RAG
        user_files = []
        rag_context_used = False
        final_prompt = chat_data.message

        if user_id:
            try:
                user_files = await crud.get_user_files(db, user_id)
            except Exception as e:
                logger.warning(f"Files fetch error: {e}")

        if user_files:
            try:
                context_docs = await asyncio.to_thread(
                    rag_retriever.retrieve_context,
                    query=chat_data.message,
                    k=3,
                    filter={'user_id': str(user_id)} if user_id else None
                )

                if context_docs:
                    final_prompt = await asyncio.to_thread(
                        rag_retriever.build_context_prompt,
                        query=chat_data.message,
                        context_documents=context_docs
                    )
                    rag_context_used = True
            except Exception as e:
                logger.warning(f"RAG error: {e}")

        # Генерация
        start_time = datetime.utcnow()
        result = await llm_manager.generate_response(
            prompt=final_prompt,
            model_source=chat_data.model_source,
            model_name=chat_data.model_name,
            temperature=chat_data.temperature or 0.7,
            max_tokens=chat_data.max_tokens or 2000,
            conversation_history=conversation_history if not rag_context_used else None
        )
        end_time = datetime.utcnow()
        generation_time = (end_time - start_time).total_seconds()

        # Сохранить ответ
        assistant_message = await crud.create_message(
            db=db,
            conversation_id=conversation_id,
            role="assistant",
            content=result["response"],
            model_name=result["model"],
            temperature=chat_data.temperature,
            max_tokens=chat_data.max_tokens,
            tokens_used=result.get("tokens_used"),
            generation_time=generation_time
        )

        logger.info(f"✅ Chat completed in {generation_time:.2f}s")

        return models.ChatResponse(
            response=result["response"],
            conversation_id=conversation_id,
            message_id=assistant_message.id,
            model_used=result["model"],
            tokens_used=result.get("tokens_used"),
            generation_time=generation_time
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Chat error: {e}")
        raise HTTPException(status_code=500, detail=f"Chat failed: {str(e)}")

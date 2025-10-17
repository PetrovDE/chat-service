# app/routers/chat.py
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db, crud
from app import models
from app.llm_manager import llm_manager
from app.routers.auth import get_current_user_optional
from app.database.models import User
from typing import Optional
import logging
import json
import uuid
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
    Отправить сообщение и получить потоковый ответ (Server-Sent Events)
    """
    try:
        user_id = current_user.id if current_user else None
        username = current_user.username if current_user else "anonymous"

        logger.info(f"Received streaming chat request: {chat_data.message[:50]}...")
        logger.info(f"User: {username} (ID: {user_id})")

        # Получить или создать беседу
        if chat_data.conversation_id:
            conversation = await crud.get_conversation(db, chat_data.conversation_id)
            if not conversation:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Conversation not found"
                )
            if conversation.user_id != user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied"
                )
            conversation_id = conversation.id
        else:
            # Создать новую беседу
            title = chat_data.message[:100] if len(chat_data.message) <= 100 else chat_data.message[:97] + "..."
            conversation = await crud.create_conversation(
                db=db,
                user_id=user_id,
                title=title,
                model_source=chat_data.model_source or "ollama",
                model_name=chat_data.model_name or llm_manager.ollama_model
            )
            conversation_id = conversation.id
            logger.info(f"Created conversation {conversation_id}")

        # Сохранить сообщение пользователя
        user_message = await crud.create_message(
            db=db,
            conversation_id=conversation_id,
            role="user",
            content=chat_data.message
        )

        # Получить историю беседы
        messages = await crud.get_conversation_messages(db, conversation_id)
        conversation_history = [
            {"role": msg.role, "content": msg.content}
            for msg in messages[:-1]  # Исключаем последнее (текущее) сообщение
        ]

        # Генерировать ID для ответа заранее
        assistant_message_id = uuid.uuid4()

        # Функция-генератор для SSE
        async def event_stream():
            full_response = ""
            start_time = datetime.utcnow()

            try:
                # Отправить метаданные
                yield f"data: {json.dumps({'type': 'start', 'conversation_id': str(conversation_id), 'message_id': str(assistant_message_id)})}\n\n"

                # Генерировать ответ
                async for chunk in llm_manager.generate_response_stream(
                        prompt=chat_data.message,
                        model_source=chat_data.model_source,
                        model_name=chat_data.model_name,
                        temperature=chat_data.temperature or 0.7,
                        max_tokens=chat_data.max_tokens or 2000,
                        conversation_history=conversation_history
                ):
                    full_response += chunk
                    # Отправить chunk клиенту
                    yield f"data: {json.dumps({'type': 'chunk', 'content': chunk})}\n\n"

                # Вычислить время генерации
                end_time = datetime.utcnow()
                generation_time = (end_time - start_time).total_seconds()

                # Сохранить ответ в БД
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

                # Отправить событие завершения
                yield f"data: {json.dumps({'type': 'done', 'generation_time': generation_time})}\n\n"

                logger.info(f"Streaming completed in {generation_time:.2f}s")

            except Exception as e:
                logger.error(f"Error in streaming: {e}")
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
        logger.error(f"Error in streaming chat endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Chat streaming failed: {str(e)}"
        )


@router.post("/", response_model=models.ChatResponse)
async def chat(
        chat_data: models.ChatMessage,
        db: AsyncSession = Depends(get_db),
        current_user: Optional[User] = Depends(get_current_user_optional)
):
    """
    Отправить сообщение и получить ответ (без streaming, для обратной совместимости)
    """
    try:
        user_id = current_user.id if current_user else None
        username = current_user.username if current_user else "anonymous"

        logger.info(f"Received chat request: {chat_data.message[:50]}...")
        logger.info(f"User: {username} (ID: {user_id})")

        # Получить или создать беседу
        if chat_data.conversation_id:
            conversation = await crud.get_conversation(db, chat_data.conversation_id)
            if not conversation:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Conversation not found"
                )
            if conversation.user_id != user_id:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Access denied"
                )
            conversation_id = conversation.id
        else:
            # Создать новую беседу
            title = chat_data.message[:100] if len(chat_data.message) <= 100 else chat_data.message[:97] + "..."
            conversation = await crud.create_conversation(
                db=db,
                user_id=user_id,
                title=title,
                model_source=chat_data.model_source or "ollama",
                model_name=chat_data.model_name or llm_manager.ollama_model
            )
            conversation_id = conversation.id
            logger.info(f"Created conversation {conversation_id}")

        # Сохранить сообщение пользователя
        user_message = await crud.create_message(
            db=db,
            conversation_id=conversation_id,
            role="user",
            content=chat_data.message
        )

        # Получить историю беседы
        messages = await crud.get_conversation_messages(db, conversation_id)
        conversation_history = [
            {"role": msg.role, "content": msg.content}
            for msg in messages[:-1]
        ]

        # Генерировать ответ
        start_time = datetime.utcnow()

        result = await llm_manager.generate_response(
            prompt=chat_data.message,
            model_source=chat_data.model_source,
            model_name=chat_data.model_name,
            temperature=chat_data.temperature or 0.7,
            max_tokens=chat_data.max_tokens or 2000,
            conversation_history=conversation_history
        )

        end_time = datetime.utcnow()
        generation_time = (end_time - start_time).total_seconds()

        # Сохранить ответ ассистента
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

        logger.info(f"Chat completed in {generation_time:.2f}s")

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
        logger.error(f"Error in chat endpoint: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Chat failed: {str(e)}"
        )
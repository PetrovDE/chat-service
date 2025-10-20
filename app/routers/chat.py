# app/routers/chat.py
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db, crud
from app import models
from app.llm_manager import llm_manager
from app.routers.auth import get_current_user_optional
from app.database.models import User
from app.rag.retriever import rag_retriever  # 🆕 Импорт RAG
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
    🆕 Автоматически использует RAG если есть загруженные файлы
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

        # 🆕 RAG: Проверить наличие файлов пользователя
        user_files = []
        rag_context_used = False

        if user_id:
            try:
                user_files = await crud.get_user_files(db, user_id)
                logger.info(f"📂 User has {len(user_files)} uploaded files")
            except Exception as e:
                logger.warning(f"⚠️ Could not fetch user files: {e}")

        # 🆕 RAG: Построить промпт с контекстом из файлов
        final_prompt = chat_data.message

        if user_files:
            try:
                logger.info("🤖 Attempting to use RAG for context...")

                # Получить релевантный контекст из файлов
                context_docs = rag_retriever.retrieve_context(
                    query=chat_data.message,
                    k=3,  # Топ-3 релевантных chunks
                    filter={'user_id': str(user_id)} if user_id else None
                )

                if context_docs:
                    # Построить промпт с контекстом
                    final_prompt = rag_retriever.build_context_prompt(
                        query=chat_data.message,
                        context_documents=context_docs
                    )
                    rag_context_used = True
                    logger.info(f"✅ Using RAG context from {len(context_docs)} documents")
                else:
                    logger.info("ℹ️ No relevant context found in files, using original query")

            except Exception as e:
                logger.warning(f"⚠️ RAG context retrieval failed (non-critical): {e}")
                # Продолжаем без RAG контекста

        # Генерировать ID для ответа заранее
        assistant_message_id = uuid.uuid4()

        # Функция-генератор для SSE
        async def event_stream():
            full_response = ""
            start_time = datetime.utcnow()

            try:
                # Отправить метаданные
                metadata = {
                    'type': 'start',
                    'conversation_id': str(conversation_id),
                    'message_id': str(assistant_message_id),
                    'rag_enabled': rag_context_used  # 🆕 Индикатор использования RAG
                }
                yield f"data: {json.dumps(metadata)}\n\n"

                # Генерировать ответ (с RAG контекстом если есть)
                async for chunk in llm_manager.generate_response_stream(
                        prompt=final_prompt,  # 🆕 Используем промпт с контекстом
                        model_source=chat_data.model_source,
                        model_name=chat_data.model_name,
                        temperature=chat_data.temperature or 0.7,
                        max_tokens=chat_data.max_tokens or 2000,
                        conversation_history=conversation_history if not rag_context_used else None
                        # 🆕 История не нужна если используем RAG
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
                completion_data = {
                    'type': 'done',
                    'generation_time': generation_time,
                    'rag_used': rag_context_used  # 🆕 Информация об использовании RAG
                }
                yield f"data: {json.dumps(completion_data)}\n\n"

                logger.info(
                    f"Streaming completed in {generation_time:.2f}s "
                    f"{'with RAG' if rag_context_used else 'without RAG'}"
                )

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
    🆕 Автоматически использует RAG если есть загруженные файлы
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

        # 🆕 RAG: Проверить наличие файлов
        user_files = []
        rag_context_used = False

        if user_id:
            try:
                user_files = await crud.get_user_files(db, user_id)
                logger.info(f"📂 User has {len(user_files)} uploaded files")
            except Exception as e:
                logger.warning(f"⚠️ Could not fetch user files: {e}")

        # 🆕 RAG: Построить промпт с контекстом
        final_prompt = chat_data.message

        if user_files:
            try:
                logger.info("🤖 Attempting to use RAG for context...")

                context_docs = rag_retriever.retrieve_context(
                    query=chat_data.message,
                    k=3,
                    filter={'user_id': str(user_id)} if user_id else None
                )

                if context_docs:
                    final_prompt = rag_retriever.build_context_prompt(
                        query=chat_data.message,
                        context_documents=context_docs
                    )
                    rag_context_used = True
                    logger.info(f"✅ Using RAG context from {len(context_docs)} documents")

            except Exception as e:
                logger.warning(f"⚠️ RAG context retrieval failed (non-critical): {e}")

        # Генерировать ответ
        start_time = datetime.utcnow()

        result = await llm_manager.generate_response(
            prompt=final_prompt,  # 🆕 Промпт с контекстом
            model_source=chat_data.model_source,
            model_name=chat_data.model_name,
            temperature=chat_data.temperature or 0.7,
            max_tokens=chat_data.max_tokens or 2000,
            conversation_history=conversation_history if not rag_context_used else None  # 🆕
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

        logger.info(
            f"Chat completed in {generation_time:.2f}s "
            f"{'with RAG' if rag_context_used else 'without RAG'}"
        )

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


# 🆕 НОВЫЙ ENDPOINT: Чат с явным использованием RAG
@router.post("/rag")
async def chat_with_rag(
        chat_data: models.ChatMessage,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user_optional)
):
    """
    Чат с явным использованием RAG (для тестирования)
    Всегда использует контекст из файлов
    """
    try:
        user_id = current_user.id if current_user else None

        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required for RAG chat"
            )

        logger.info(f"🤖 RAG chat request from user {user_id}")

        # Использовать RAG для генерации ответа
        result = await rag_retriever.generate_answer(
            query=chat_data.message,
            filter={'user_id': str(user_id)},
            temperature=chat_data.temperature or 0.7,
            max_tokens=chat_data.max_tokens or 2000
        )

        logger.info(
            f"✅ RAG chat completed using {result['rag_context']['documents_used']} documents"
        )

        return {
            "response": result["response"],
            "rag_context": result["rag_context"],
            "model_used": result["model"],
            "tokens_used": result.get("tokens_used"),
            "generation_time": result.get("generation_time")
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"RAG chat error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"RAG chat failed: {str(e)}"
        )
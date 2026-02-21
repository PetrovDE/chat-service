# app/api/v1/endpoints/chat.py
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
import logging
import json
import uuid
from datetime import datetime

from app.db.session import get_db
from app.db.models import User
from app.schemas import ChatMessage, ChatResponse
from app.api.dependencies import get_current_user_optional
from app.services.llm.manager import llm_manager
from app.crud import crud_conversation, crud_message, crud_file
from app.rag.retriever import rag_retriever

router = APIRouter()
logger = logging.getLogger(__name__)


def _build_conversation_history(messages):
    return [{"role": msg.role, "content": msg.content} for msg in messages[:-1]]


async def _try_build_rag_prompt(
    *,
    db: AsyncSession,
    user_id: Optional[uuid.UUID],
    conversation_id: uuid.UUID,
    query: str,
    top_k: int = 3,
):
    final_prompt = query
    rag_used = False
    rag_debug = None

    if not user_id:
        return final_prompt, rag_used, rag_debug

    try:
        files = await crud_file.get_conversation_files(db, conversation_id=conversation_id, user_id=user_id)
        logger.info("Conversation files (completed): %d", len(files))
    except Exception as e:
        logger.warning("Could not fetch conversation files: %s", e)
        return final_prompt, rag_used, rag_debug

    if not files:
        return final_prompt, rag_used, rag_debug

    file_ids = [str(f.id) for f in files]

    try:
        rag_result = await rag_retriever.query_rag(
            query,
            top_k=top_k,
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            file_ids=file_ids,
            debug_return=True,
        )

        if isinstance(rag_result, dict) and "docs" in rag_result:
            context_docs = rag_result.get("docs") or []
            rag_debug = rag_result.get("debug")
        else:
            context_docs = rag_result or []

        if context_docs:
            final_prompt = rag_retriever.build_context_prompt(query=query, context_documents=context_docs)
            rag_used = True
            logger.info("RAG enabled: docs=%d", len(context_docs))
        else:
            logger.info("RAG: no relevant chunks")

    except TypeError:
        context_docs = await rag_retriever.query_rag(
            query,
            top_k=top_k,
            user_id=str(user_id),
            conversation_id=str(conversation_id),
            debug_return=True,
        )
        if isinstance(context_docs, dict) and "docs" in context_docs:
            context_docs_list = context_docs.get("docs") or []
            rag_debug = context_docs.get("debug")
            if context_docs_list:
                final_prompt = rag_retriever.build_context_prompt(query=query, context_documents=context_docs_list)
                rag_used = True

    except Exception as e:
        logger.warning("RAG retrieval failed: %s", e)

    return final_prompt, rag_used, rag_debug


@router.post("/stream")
async def chat_stream(
    chat_data: ChatMessage,
    db: AsyncSession = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional),
):
    try:
        user_id = current_user.id if current_user else None
        username = current_user.username if current_user else "anonymous"

        logger.info("Chat(stream) from %s", username)

        # Get or create conversation
        if chat_data.conversation_id:
            conversation = await crud_conversation.get(db, id=chat_data.conversation_id)
            if not conversation:
                raise HTTPException(status_code=404, detail="Conversation not found")
            if conversation.user_id != user_id:
                raise HTTPException(status_code=403, detail="Access denied")
            conversation_id = conversation.id
        else:
            from app.schemas.conversation import ConversationCreate

            conv_data = ConversationCreate(
                title=chat_data.message[:100] if len(chat_data.message) <= 100 else chat_data.message[:97] + "...",
                model_source=chat_data.model_source or "ollama",
                model_name=chat_data.model_name or llm_manager.ollama_model,
            )
            conversation = await crud_conversation.create_for_user(db=db, obj_in=conv_data, user_id=user_id)
            conversation_id = conversation.id

        # Save user message
        await crud_message.create_message(db=db, conversation_id=conversation_id, role="user", content=chat_data.message)

        # History
        messages = await crud_message.get_conversation_messages(db, conversation_id=conversation_id)
        conversation_history = _build_conversation_history(messages)

        # RAG
        final_prompt, rag_used, rag_debug = await _try_build_rag_prompt(
            db=db,
            user_id=user_id,
            conversation_id=conversation_id,
            query=chat_data.message,
            top_k=3,
        )

        assistant_message_id = uuid.uuid4()

        async def event_stream():
            full_response = ""
            start_time = datetime.utcnow()

            try:
                yield f"data: {json.dumps({'type': 'start','conversation_id': str(conversation_id),'message_id': str(assistant_message_id),'rag_enabled': rag_used,'rag_debug': rag_debug})}\n\n"

                async for chunk in llm_manager.generate_response_stream(
                    prompt=final_prompt,
                    model_source=chat_data.model_source,
                    model_name=chat_data.model_name,
                    temperature=chat_data.temperature or 0.7,
                    max_tokens=chat_data.max_tokens or 2000,
                    conversation_history=conversation_history if not rag_used else None,
                ):
                    full_response += chunk
                    yield f"data: {json.dumps({'type': 'chunk', 'content': chunk})}\n\n"

                generation_time = (datetime.utcnow() - start_time).total_seconds()

                await crud_message.create_message(
                    db=db,
                    conversation_id=conversation_id,
                    role="assistant",
                    content=full_response,
                    model_name=chat_data.model_name or llm_manager.ollama_model,
                    temperature=chat_data.temperature,
                    max_tokens=chat_data.max_tokens,
                    generation_time=generation_time,
                )

                yield f"data: {json.dumps({'type': 'done', 'generation_time': generation_time, 'rag_used': rag_used})}\n\n"

            except Exception as e:
                logger.error("Streaming error: %s", e, exc_info=True)
                yield f"data: {json.dumps({'type':'error','message': str(e), 'error_type': type(e).__name__})}\n\n"

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Chat stream error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Chat failed: {str(e)}")


@router.post("/", response_model=ChatResponse)
async def chat(
    chat_data: ChatMessage,
    db: AsyncSession = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional),
):
    try:
        user_id = current_user.id if current_user else None
        username = current_user.username if current_user else "anonymous"
        logger.info("Chat(non-stream) from %s", username)

        if chat_data.conversation_id:
            conversation = await crud_conversation.get(db, id=chat_data.conversation_id)
            if not conversation:
                raise HTTPException(status_code=404, detail="Conversation not found")
            if conversation.user_id != user_id:
                raise HTTPException(status_code=403, detail="Access denied")
            conversation_id = conversation.id
        else:
            from app.schemas.conversation import ConversationCreate

            conv_data = ConversationCreate(
                title=chat_data.message[:100] if len(chat_data.message) <= 100 else chat_data.message[:97] + "...",
                model_source=chat_data.model_source or "ollama",
                model_name=chat_data.model_name or llm_manager.ollama_model,
            )
            conversation = await crud_conversation.create_for_user(db=db, obj_in=conv_data, user_id=user_id)
            conversation_id = conversation.id

        await crud_message.create_message(db=db, conversation_id=conversation_id, role="user", content=chat_data.message)

        messages = await crud_message.get_conversation_messages(db, conversation_id=conversation_id)
        conversation_history = _build_conversation_history(messages)

        final_prompt, rag_used, rag_debug = await _try_build_rag_prompt(
            db=db, user_id=user_id, conversation_id=conversation_id, query=chat_data.message, top_k=3
        )

        start_time = datetime.utcnow()
        result = await llm_manager.generate_response(
            prompt=final_prompt,
            model_source=chat_data.model_source,
            model_name=chat_data.model_name,
            temperature=chat_data.temperature or 0.7,
            max_tokens=chat_data.max_tokens or 2000,
            conversation_history=conversation_history if not rag_used else None,
        )
        generation_time = (datetime.utcnow() - start_time).total_seconds()

        assistant_message = await crud_message.create_message(
            db=db,
            conversation_id=conversation_id,
            role="assistant",
            content=result["response"],
            model_name=result["model"],
            temperature=chat_data.temperature,
            max_tokens=chat_data.max_tokens,
            tokens_used=result.get("tokens_used"),
            generation_time=generation_time,
        )

        return ChatResponse(
            response=result["response"],
            conversation_id=conversation_id,
            message_id=assistant_message.id,
            model_used=result["model"],
            tokens_used=result.get("tokens_used"),
            generation_time=generation_time,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Chat error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Chat failed: {str(e)}")

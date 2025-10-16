"""
Chat router - handles chat interactions with LLM
"""

from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
import time
import logging
from uuid import UUID

from ..models import ChatRequest, ChatResponse, ChatRequestExtended
from ..llm_manager import llm_manager
from ..database import get_db, crud
from ..auth import get_optional_user
from ..database.models import User

router = APIRouter(prefix="/chat", tags=["chat"])
logger = logging.getLogger(__name__)


@router.post("", response_model=ChatResponse)
async def chat(
    request: ChatRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_optional_user)
):
    """Send a message to the LLM and get a response"""
    start_time = time.time()
    conversation_id = None

    try:
        logger.info(f"Received chat request: {request.message[:50]}...")

        # Get user_id
        user_id = current_user.id if current_user else None
        logger.info(f"User: {current_user.username if current_user else 'anonymous'} (ID: {user_id})")

        # Create conversation
        conversation = await crud.create_conversation(
            db=db,
            user_id=user_id,
            model_source=llm_manager.active_source.value,
            model_name=llm_manager.get_current_model_name()
        )
        conversation_id = conversation.id
        logger.info(f"Created conversation {conversation_id}")

        # Save user message
        await crud.create_message(
            db=db,
            conversation_id=conversation.id,
            role="user",
            content=request.message
        )

        # Get LLM response
        response = await llm_manager.get_response(
            message=request.message,
            temperature=request.temperature,
            max_tokens=request.max_tokens
        )

        # Save assistant message
        await crud.create_message(
            db=db,
            conversation_id=conversation.id,
            role="assistant",
            content=response.response,
            model_name=response.model,
            tokens_used=response.tokens_used,
            generation_time=response.generation_time
        )

        # Calculate response time
        response_time = time.time() - start_time

        # Log API usage
        await crud.log_api_usage(
            db=db,
            user_id=user_id,
            conversation_id=conversation.id,
            model_source=llm_manager.active_source.value,
            model_name=llm_manager.get_current_model_name(),
            endpoint="/chat",
            tokens_total=response.tokens_used,
            response_time=response_time,
            status="success"
        )

        return ChatResponse(
            response=response.response,
            model=response.model,
            tokens_used=response.tokens_used,
            generation_time=response.generation_time,
            conversation_id=str(conversation.id)
        )

    except Exception as e:
        logger.error(f"Error in chat endpoint: {str(e)}")

        if conversation_id:
            await crud.log_api_usage(
                db=db,
                user_id=user_id if current_user else None,
                conversation_id=conversation_id,
                model_source=llm_manager.active_source.value,
                model_name=llm_manager.get_current_model_name(),
                endpoint="/chat",
                response_time=time.time() - start_time,
                status="error",
                error_message=str(e)
            )

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get response from LLM: {str(e)}"
        )


@router.post("/stream")
async def chat_stream(
    request: ChatRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_optional_user)
):
    """Stream a response from the LLM in real-time"""
    start_time = time.time()
    conversation_id = None
    full_response = ""

    async def generate_stream():
        nonlocal conversation_id, full_response

        try:
            logger.info(f"Starting streaming response for: {request.message[:50]}...")

            # Get user_id
            user_id = current_user.id if current_user else None

            # Create conversation
            conversation = await crud.create_conversation(
                db=db,
                user_id=user_id,
                model_source=llm_manager.active_source.value,
                model_name=llm_manager.get_current_model_name()
            )
            conversation_id = conversation.id

            # Save user message
            await crud.create_message(
                db=db,
                conversation_id=conversation.id,
                role="user",
                content=request.message
            )

            # Send conversation_id first
            yield f"data: {{'conversation_id': '{conversation.id}'}}\n\n"

            # Stream response
            async for chunk in llm_manager.stream_response(
                message=request.message,
                temperature=request.temperature,
                max_tokens=request.max_tokens
            ):
                full_response += chunk
                yield f"data: {{'token': '{chunk}'}}\n\n"

            # Save assistant message
            await crud.create_message(
                db=db,
                conversation_id=conversation.id,
                role="assistant",
                content=full_response,
                model_name=llm_manager.get_current_model_name()
            )

            # Log usage
            response_time = time.time() - start_time
            await crud.log_api_usage(
                db=db,
                user_id=user_id,
                conversation_id=conversation.id,
                model_source=llm_manager.active_source.value,
                model_name=llm_manager.get_current_model_name(),
                endpoint="/chat/stream",
                response_time=response_time,
                status="success"
            )

            yield "data: [DONE]\n\n"

        except Exception as e:
            logger.error(f"Error in streaming chat: {str(e)}")

            if conversation_id:
                await crud.log_api_usage(
                    db=db,
                    user_id=user_id if current_user else None,
                    conversation_id=conversation_id,
                    model_source=llm_manager.active_source.value,
                    model_name=llm_manager.get_current_model_name(),
                    endpoint="/chat/stream",
                    response_time=time.time() - start_time,
                    status="error",
                    error_message=str(e)
                )

            yield f"data: {{'error': '{str(e)}'}}\n\n"

    return StreamingResponse(
        generate_stream(),
        media_type="text/event-stream"
    )


@router.post("/continue", response_model=ChatResponse)
async def chat_continue(
    request: ChatRequestExtended,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_optional_user)
):
    """Continue an existing conversation or create new one"""
    start_time = time.time()

    try:
        user_id = current_user.id if current_user else None

        # Get or create conversation
        if request.conversation_id:
            conv_uuid = UUID(request.conversation_id)
            conversation = await crud.get_conversation(db, conv_uuid)
            if not conversation:
                raise HTTPException(status_code=404, detail="Conversation not found")

            # Check access
            if current_user and conversation.user_id != current_user.id:
                raise HTTPException(status_code=403, detail="Access denied")
            if not current_user and conversation.user_id is not None:
                raise HTTPException(status_code=403, detail="Access denied")

        else:
            # Create new conversation
            conversation = await crud.create_conversation(
                db=db,
                user_id=user_id,
                model_source=llm_manager.active_source.value,
                model_name=llm_manager.get_current_model_name()
            )

        # Save user message
        await crud.create_message(
            db=db,
            conversation_id=conversation.id,
            role="user",
            content=request.message
        )

        # Get context if needed
        context = []
        if request.include_history:
            messages = await crud.get_conversation_messages(db, conversation.id, limit=request.history_length or 10)
            context = [{"role": msg.role, "content": msg.content} for msg in messages[:-1]]

        # Get response
        response = await llm_manager.get_response(
            message=request.message,
            temperature=request.temperature,
            max_tokens=request.max_tokens,
            context=context if context else None
        )

        # Save assistant message
        await crud.create_message(
            db=db,
            conversation_id=conversation.id,
            role="assistant",
            content=response.response,
            model_name=response.model,
            tokens_used=response.tokens_used,
            generation_time=response.generation_time
        )

        # Log usage
        response_time = time.time() - start_time
        await crud.log_api_usage(
            db=db,
            user_id=user_id,
            conversation_id=conversation.id,
            model_source=llm_manager.active_source.value,
            model_name=llm_manager.get_current_model_name(),
            endpoint="/chat/continue",
            tokens_total=response.tokens_used,
            response_time=response_time,
            status="success"
        )

        return ChatResponse(
            response=response.response,
            model=response.model,
            tokens_used=response.tokens_used,
            generation_time=response.generation_time,
            conversation_id=str(conversation.id)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in continue chat: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
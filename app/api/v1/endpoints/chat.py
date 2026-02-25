import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_current_user_optional
from app.db.models import User
from app.db.session import get_db
from app.schemas import ChatMessage, ChatResponse
from app.services.chat_orchestrator import _try_build_rag_prompt, chat_orchestrator

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/stream")
async def chat_stream(
    chat_data: ChatMessage,
    db: AsyncSession = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional),
):
    try:
        return await chat_orchestrator.chat_stream(chat_data=chat_data, db=db, current_user=current_user)
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
        return await chat_orchestrator.chat(chat_data=chat_data, db=db, current_user=current_user)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Chat error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Chat failed: {str(e)}")

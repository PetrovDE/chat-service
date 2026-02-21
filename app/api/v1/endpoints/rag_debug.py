"""
RAG Debug endpoint

Добавлено:
- GET /api/v1/rag/debug
  params: conversation_id (uuid), q (str), k (int)
Возвращает:
- top-k chunks + score + applied filters + длина контекста

Важно: это endpoint для разработки/диагностики
"""

from __future__ import annotations

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_current_user
from app.db.models import User
from app.db.session import get_db
from app.rag.retriever import rag_retriever

router = APIRouter()


@router.get("/debug")
async def rag_debug(
    conversation_id: UUID = Query(...),
    q: str = Query(..., min_length=1),
    k: int = Query(5, ge=1, le=20),
    fetch_k: int = Query(30, ge=5, le=200),
    embedding_mode: str = Query("local"),
    embedding_model: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    result = await rag_retriever.query_rag(
        q,
        top_k=k,
        fetch_k=fetch_k,
        conversation_id=str(conversation_id),
        user_id=str(current_user.id),
        embedding_mode=embedding_mode,
        embedding_model=embedding_model,
        debug_return=True,
    )
    return result

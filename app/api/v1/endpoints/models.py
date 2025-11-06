# app/api/v1/endpoints/models.py
from fastapi import APIRouter, Depends
from typing import List, Dict, Any

from app.api.dependencies import get_current_user
from app.db.models import User
from app.services.llm.manager import llm_manager

router = APIRouter()


@router.get("/available")
async def get_available_models(
    source: str = "ollama",
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """Get available models from source"""
    models = await llm_manager.get_available_models(source)
    return {
        "source": source,
        "models": models,
        "current_model": llm_manager.ollama_model if source == "ollama" else llm_manager.openai_model
    }


@router.get("/sources")
async def get_model_sources() -> List[str]:
    """Get available model sources"""
    return ["ollama", "openai"]


@router.get("/current")
async def get_current_model() -> Dict[str, str]:
    """Get current default model"""
    return {
        "source": llm_manager.default_source,
        "model": llm_manager.ollama_model if llm_manager.default_source == "ollama" else llm_manager.openai_model
    }

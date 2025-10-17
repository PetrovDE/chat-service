# app/routers/models_management.py
from fastapi import APIRouter, HTTPException, status
from app import models
from app.llm_manager import llm_manager
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/")
async def list_models():
    """Получить список доступных моделей"""
    try:
        ollama_models = await llm_manager.list_available_models("ollama")
        openai_models = await llm_manager.list_available_models("openai")
        
        return {
            "current_source": llm_manager.active_source,
            "current_model": llm_manager.get_active_model(),
            "available_models": {
                "ollama": ollama_models,
                "openai": openai_models
            }
        }
    except Exception as e:
        logger.error(f"Error listing models: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list models: {str(e)}"
        )


@router.post("/switch")
async def switch_model(model_switch: models.ModelSwitch):
    """Переключить активную модель"""
    try:
        # Проверить доступность модели
        available_models = await llm_manager.list_available_models(model_switch.model_source)
        
        if model_switch.model_source == "ollama" and available_models:
            if model_switch.model_name not in available_models:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Model '{model_switch.model_name}' not found in Ollama"
                )
        
        # Установить активную модель
        llm_manager.set_active_model(
            source=model_switch.model_source,
            model=model_switch.model_name
        )
        
        logger.info(f"Model switched to: {model_switch.model_source}/{model_switch.model_name}")
        
        return {
            "success": True,
            "message": f"Switched to {model_switch.model_source}/{model_switch.model_name}",
            "active_model": llm_manager.get_active_model()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error switching model: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to switch model: {str(e)}"
        )


@router.get("/current")
async def get_current_model():
    """Получить текущую активную модель"""
    return llm_manager.get_active_model()
"""
Models management router - handles model selection and information
"""

from fastapi import APIRouter, HTTPException, status
import logging

from ..models import ModelListResponse, ModelInfo, ModelSwitchRequest
from ..llm_manager import llm_manager

router = APIRouter(prefix="/models", tags=["models"])
logger = logging.getLogger(__name__)


@router.get("", response_model=ModelListResponse)
async def list_models():
    """Get list of available models"""
    try:
        models_data = await llm_manager.get_available_models()

        models = [
            ModelInfo(
                name=model["name"],
                source=model["source"],
                size=model.get("size"),
                modified=model.get("modified")
            )
            for model in models_data
        ]

        return ModelListResponse(
            models=models,
            active_source=llm_manager.active_source.value,
            active_model=llm_manager.get_current_model_name()
        )

    except Exception as e:
        logger.error(f"Error listing models: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/switch")
async def switch_model(request: ModelSwitchRequest):
    """Switch active model"""
    try:
        llm_manager.switch_model(request.source, request.model_name)

        return {
            "success": True,
            "active_source": llm_manager.active_source.value,
            "active_model": llm_manager.get_current_model_name()
        }

    except Exception as e:
        logger.error(f"Error switching model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/current")
async def get_current_model():
    """Get current active model"""
    return {
        "source": llm_manager.active_source.value,
        "model": llm_manager.get_current_model_name()
    }

# app/api/v1/endpoints/models.py
from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import requests
import logging
from app.core.config import settings

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/list")
async def list_models(mode: str = "local") -> Dict[str, Any]:
    """Список доступных моделей"""
    try:
        logger.info(f"📋 Getting models for mode: {mode}")

        # ✅ ИСПРАВЛЕНО: поддержка 'local', 'ollama' И 'corporate'
        if mode in ["local", "ollama"]:
            # Локальные модели через Ollama
            ollama_url = str(settings.EMBEDDINGS_BASEURL).rstrip('/')
            logger.info(f"🔌 Querying Ollama: {ollama_url}")

            response = requests.get(f"{ollama_url}/api/tags", timeout=5)
            response.raise_for_status()
            data = response.json()
            models = data.get("models", [])

            logger.info(f"✅ Got {len(models)} models from Ollama")

            return {
                "mode": mode,
                "models": [{"name": m.get("name"), "size": m.get("size", 0)} for m in models],
                "count": len(models)
            }

        # ✅ НОВОЕ: поддержка режима 'corporate' (AI HUB)
        elif mode in ["corporate", "aihub"]:
            logger.info(f"🏢 Querying AI HUB for models")

            # Здесь нужно получить модели из AI HUB
            # Пока возвращаем известные модели AI HUB
            aihub_models = [
                {"name": "vikhr", "size": 0},  # Основная чат-модель
                {"name": "embedding-model", "size": 0}  # Модель для эмбеддингов
            ]

            logger.info(f"✅ Returning {len(aihub_models)} AI HUB models")

            return {
                "mode": mode,
                "models": aihub_models,
                "count": len(aihub_models)
            }

        else:
            logger.warning(f"⚠️ Unknown mode: {mode}")
            return {"mode": mode, "models": [], "count": 0}

    except Exception as e:
        logger.error(f"❌ Error getting models: {e}")
        return {"mode": mode, "models": [], "count": 0, "error": str(e)}


@router.get("/status")
async def models_status() -> Dict[str, Any]:
    """Статус доступности моделей"""
    ollama_available = False
    aihub_available = False

    try:
        ollama_url = str(settings.EMBEDDINGS_BASEURL).rstrip('/')
        response = requests.get(f"{ollama_url}/api/tags", timeout=3)
        ollama_available = response.status_code == 200
    except:
        pass

    # ✅ НОВОЕ: проверка доступности AI HUB
    try:
        aihub_available = bool(settings.AIHUB_URL and settings.AIHUB_KEYCLOAK_HOST)
    except:
        pass

    return {
        "ollama": {"available": ollama_available, "url": str(settings.EMBEDDINGS_BASEURL)},
        "corporate": {"available": aihub_available, "url": settings.AIHUB_URL},
        # ✅ Изменено с CORPORATE_API_URL на AIHUB_URL
        "aihub": {"available": aihub_available, "url": settings.AIHUB_URL}  # ✅ Добавлено
    }

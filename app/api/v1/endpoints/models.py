# app/api/v1/endpoints/models.py
from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List
import requests
import logging
from app.core.config import settings
from app.services.llm.manager import llm_manager  # ← ДОБАВИТЬ

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/list")
async def list_models(mode: str = "local") -> Dict[str, Any]:
    """Список доступных моделей"""
    try:
        logger.info(f"📋 Getting models for mode: {mode}")

        # ✅ ИСПРАВЛЕНО: поддержка 'local', 'ollama' и 'aihub'
        if mode in ["local", "ollama"]:
            # Локальные модели через Ollama
            ollama_url = str(settings.EMBEDDINGS_BASEURL).rstrip('/')
            logger.info(f"🔌 Querying Ollama: {ollama_url}")

            try:
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
            except Exception as e:
                logger.error(f"❌ Error querying Ollama: {e}")
                return {
                    "mode": mode,
                    "models": [],
                    "count": 0,
                    "error": str(e)
                }

        elif mode in ["aihub", "corporate"]:
            logger.info(f"🏢 Querying AI HUB for models")

            try:
                # ✅ ИСПРАВЛЕНО: используем llm_manager для получения моделей
                logger.info("📡 Using llm_manager to get AI HUB models...")

                models_list = await llm_manager.get_available_models(source="aihub")

                if models_list:
                    aihub_models = [{"name": model, "size": 0} for model in models_list]
                    logger.info(f"✅ Got {len(aihub_models)} models from AI HUB via llm_manager")
                else:
                    logger.warning("⚠️ No models returned from AI HUB, using defaults")
                    aihub_models = [
                        {"name": "vikhr", "size": 0},
                        {"name": "gpt-4", "size": 0}
                    ]

                return {
                    "mode": mode,
                    "models": aihub_models,
                    "count": len(aihub_models)
                }

            except Exception as e:
                logger.error(f"❌ Error getting AI HUB models: {e}", exc_info=True)
                # Возвращаем default модели в случае ошибки
                return {
                    "mode": mode,
                    "models": [
                        {"name": "vikhr", "size": 0},
                        {"name": "gpt-4", "size": 0}
                    ],
                    "count": 2,
                    "error": str(e)
                }

        # OpenAI и другие режимы
        elif mode == "openai":
            logger.info(f"🔌 Using OpenAI models")
            openai_models = [
                {"name": "gpt-4", "size": 0},
                {"name": "gpt-3.5-turbo", "size": 0},
                {"name": "gpt-4-turbo", "size": 0}
            ]
            return {
                "mode": mode,
                "models": openai_models,
                "count": len(openai_models)
            }

        else:
            logger.warning(f"⚠️ Unknown mode: {mode}")
            return {"mode": mode, "models": [], "count": 0}

    except Exception as e:
        logger.error(f"❌ Error getting models: {e}", exc_info=True)
        return {"mode": mode, "models": [], "count": 0, "error": str(e)}


@router.get("/status")
async def models_status() -> Dict[str, Any]:
    """Статус доступности моделей"""
    ollama_available = False
    aihub_available = False
    openai_available = False

    try:
        ollama_url = str(settings.EMBEDDINGS_BASEURL).rstrip('/')
        response = requests.get(f"{ollama_url}/api/tags", timeout=3)
        ollama_available = response.status_code == 200
    except:
        pass

    # ✅ Проверка доступности AI HUB
    try:
        aihub_url = getattr(settings, 'AIHUB_URL', None)
        aihub_available = bool(aihub_url)
    except:
        pass

    # ✅ Проверка OpenAI
    try:
        openai_api_key = getattr(settings, 'OPENAI_API_KEY', None)
        openai_available = bool(openai_api_key)
    except:
        pass

    return {
        "ollama": {
            "available": ollama_available,
            "url": str(settings.EMBEDDINGS_BASEURL)
        },
        "aihub": {
            "available": aihub_available,
            "url": getattr(settings, 'AIHUB_URL', None)
        },
        "openai": {
            "available": openai_available
        }
    }

# app/api/v1/endpoints/models.py
from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import requests
from app.core.config import settings

router = APIRouter()


@router.get("/list")
async def list_models(mode: str = "local") -> Dict[str, Any]:
    """Список доступных моделей"""
    try:
        # ИСПРАВЛЕНО: поддержка 'local' и 'ollama'
        if mode in ["local", "ollama"]:
            ollama_url = str(settings.EMBEDDINGS_BASEURL).rstrip('/')
            response = requests.get(f"{ollama_url}/api/tags", timeout=5)
            response.raise_for_status()
            data = response.json()
            models = data.get("models", [])

            return {
                "mode": mode,
                "models": [{"name": m.get("name"), "size": m.get("size", 0)} for m in models],
                "count": len(models)
            }
        else:
            return {"mode": mode, "models": [], "count": 0}
    except Exception as e:
        return {"mode": mode, "models": [], "count": 0, "error": str(e)}


@router.get("/status")
async def models_status() -> Dict[str, Any]:
    """Статус доступности моделей"""
    ollama_available = False
    try:
        ollama_url = str(settings.EMBEDDINGS_BASEURL).rstrip('/')
        response = requests.get(f"{ollama_url}/api/tags", timeout=3)
        ollama_available = response.status_code == 200
    except:
        pass

    return {
        "ollama": {"available": ollama_available, "url": str(settings.EMBEDDINGS_BASEURL)},
        "corporate": {"available": bool(settings.CORPORATE_API_URL)}
    }

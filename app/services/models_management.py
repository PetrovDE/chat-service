# app/models_management.py

from typing import List, Optional
from app.services.llm.manager import llm_manager

class ModelsManagementManager:
    def __init__(self):
        pass

    def list_models(self, mode: Optional[str] = None) -> List[str]:
        """Список моделей для выбранного режима — Ollama (локально) или Корпоративный HUB."""
        if mode:
            llm_manager.switch_mode(mode)
        return llm_manager.get_available_models()

    def set_default_model(self, model: str, mode: Optional[str] = None):
        """Изменение default модели для текущей сессии (или выбранного режима)."""
        if mode:
            llm_manager.switch_mode(mode)
        llm_manager.switch_model(model)

    # Опционально: можно добавить функционал скачивания/обновления локальных моделей
    # и/или получения подробной информации по конкретной модели

models_management_manager = ModelsManagementManager()

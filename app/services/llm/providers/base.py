"""
Base LLM Provider Interface
Базовый интерфейс для всех провайдеров LLM
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List, AsyncGenerator


class BaseLLMProvider(ABC):
    """Базовый класс для всех LLM провайдеров"""

    @abstractmethod
    async def get_available_models(self) -> List[str]:
        """Получить список доступных моделей"""
        pass

    @abstractmethod
    async def generate_response(
            self,
            prompt: str,
            model: str,
            temperature: float = 0.7,
            max_tokens: int = 2000,
            conversation_history: Optional[List[Dict[str, str]]] = None,
            prompt_max_chars: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Генерировать полный ответ (без стриминга)"""
        pass

    @abstractmethod
    async def generate_response_stream(
            self,
            prompt: str,
            model: str,
            temperature: float = 0.7,
            max_tokens: int = 2000,
            conversation_history: Optional[List[Dict[str, str]]] = None,
            prompt_max_chars: Optional[int] = None,
    ) -> AsyncGenerator[str, None]:
        """Генерировать ответ со стримингом"""
        pass

    @abstractmethod
    async def generate_embedding(
            self,
            text: str,
            model: Optional[str] = None
    ) -> Optional[List[float]]:
        """Генерировать эмбеддинг для текста"""
        pass

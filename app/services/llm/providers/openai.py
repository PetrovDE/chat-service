"""
OpenAI LLM Provider
Провайдер для работы с OpenAI API
"""
import logging
import json
from typing import Optional, Dict, Any, List, AsyncGenerator
import httpx

from app.core.config import settings
from app.services.llm.providers.base import BaseLLMProvider

logger = logging.getLogger(__name__)


class OpenAIProvider(BaseLLMProvider):
    """Провайдер для работы с OpenAI"""

    def __init__(self):
        self.api_key = settings.OPENAI_API_KEY
        self.default_model = settings.OPENAI_MODEL
        self.timeout = httpx.Timeout(120.0, connect=10.0, read=120.0)
        self.base_url = "https://api.openai.com/v1"

        logger.info(f"🚀 OpenAIProvider initialized")

    def _get_headers(self) -> Dict[str, str]:
        """Получить заголовки для OpenAI API"""
        if not self.api_key:
            raise ValueError("OpenAI API key is not configured")

        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    async def get_available_models(self) -> List[str]:
        """Получить список доступных моделей OpenAI"""
        return ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo-preview", "gpt-4o"]

    async def generate_response(
            self,
            prompt: str,
            model: str,
            temperature: float = 0.7,
            max_tokens: int = 2000,
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Генерировать полный ответ через OpenAI"""
        messages = []
        if conversation_history:
            messages.extend(conversation_history)
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False
        }

        try:
            headers = self._get_headers()
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info(f"📡 Sending request to OpenAI: model={model}")
                response = await client.post(
                    f"{self.base_url}/chat/completions",
                    json=payload,
                    headers=headers
                )
                response.raise_for_status()
                data = response.json()

                return {
                    "response": data["choices"][0]["message"]["content"],
                    "model": model,
                    "tokens_used": data["usage"]["total_tokens"]
                }
        except Exception as e:
            logger.error(f"❌ OpenAI generation error: {e}")
            raise

    async def generate_response_stream(
            self,
            prompt: str,
            model: str,
            temperature: float = 0.7,
            max_tokens: int = 2000,
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> AsyncGenerator[str, None]:
        """Генерировать ответ со стримингом через OpenAI"""
        messages = []
        if conversation_history:
            messages.extend(conversation_history)
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": True
        }

        try:
            headers = self._get_headers()
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info(f"📡 Starting OpenAI stream: model={model}")

                async with client.stream(
                        "POST",
                        f"{self.base_url}/chat/completions",
                        json=payload,
                        headers=headers
                ) as response:
                    response.raise_for_status()

                    async for line in response.aiter_lines():
                        if line.startswith("data: "):
                            if line == "data: [DONE]":
                                break
                            try:
                                data = json.loads(line[6:])
                                if "choices" in data and len(data["choices"]) > 0:
                                    delta = data["choices"][0].get("delta", {})
                                    if "content" in delta:
                                        yield delta["content"]
                            except json.JSONDecodeError:
                                continue
        except Exception as e:
            logger.error(f"❌ OpenAI streaming error: {e}")
            raise

    async def generate_embedding(
            self,
            text: str,
            model: Optional[str] = None
    ) -> Optional[List[float]]:
        """Генерировать эмбеддинг через OpenAI"""
        embedding_model = model or "text-embedding-ada-002"

        payload = {
            "model": embedding_model,
            "input": text
        }

        try:
            headers = self._get_headers()
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/embeddings",
                    json=payload,
                    headers=headers
                )
                response.raise_for_status()
                data = response.json()
                return data["data"][0]["embedding"]
        except Exception as e:
            logger.error(f"❌ OpenAI embedding error: {e}")
            return None


# Singleton instance
openai_provider = OpenAIProvider()

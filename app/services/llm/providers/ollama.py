"""
Ollama LLM Provider
Провайдер для работы с локальными моделями через Ollama
"""
import logging
import json
from typing import Optional, Dict, Any, List, AsyncGenerator
import httpx

from app.core.config import settings
from app.services.llm.providers.base import BaseLLMProvider

logger = logging.getLogger(__name__)


class OllamaProvider(BaseLLMProvider):
    """Провайдер для работы с Ollama"""

    def __init__(self):
        self.ollama_url = str(settings.EMBEDDINGS_BASEURL).rstrip('/')
        self.default_model = settings.EMBEDDINGS_MODEL
        self.timeout = httpx.Timeout(120.0, connect=10.0, read=120.0)

        logger.info(f"🚀 OllamaProvider initialized: {self.ollama_url}")

    async def get_available_models(self) -> List[str]:
        """Получить список доступных моделей из Ollama"""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(f"{self.ollama_url}/api/tags")
                response.raise_for_status()
                data = response.json()
                models = [model["name"] for model in data.get("models", [])]
                logger.info(f"📋 Available Ollama models: {models}")
                return models
        except Exception as e:
            logger.error(f"❌ Failed to fetch Ollama models: {e}")
            return []

    async def generate_response(
            self,
            prompt: str,
            model: str,
            temperature: float = 0.7,
            max_tokens: int = 2000,
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Генерировать полный ответ через Ollama"""
        messages = []
        if conversation_history:
            messages.extend(conversation_history)
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens
            }
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info(f"📡 Sending request to Ollama: model={model}")
                response = await client.post(
                    f"{self.ollama_url}/api/chat",
                    json=payload
                )
                response.raise_for_status()
                data = response.json()

                return {
                    "response": data["message"]["content"],
                    "model": model,
                    "tokens_used": data.get("eval_count", 0) + data.get("prompt_eval_count", 0)
                }
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ Ollama HTTP error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"❌ Ollama generation error: {e}")
            raise

    async def generate_response_stream(
            self,
            prompt: str,
            model: str,
            temperature: float = 0.7,
            max_tokens: int = 2000,
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> AsyncGenerator[str, None]:
        """Генерировать ответ со стримингом через Ollama"""
        messages = []
        if conversation_history:
            messages.extend(conversation_history)
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": model,
            "messages": messages,
            "stream": True,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens
            }
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info(f"📡 Starting Ollama stream: model={model}")

                async with client.stream(
                        "POST",
                        f"{self.ollama_url}/api/chat",
                        json=payload
                ) as response:
                    response.raise_for_status()

                    async for line in response.aiter_lines():
                        if line:
                            try:
                                data = json.loads(line)
                                if "message" in data and "content" in data["message"]:
                                    content = data["message"]["content"]
                                    if content:
                                        yield content
                            except json.JSONDecodeError as e:
                                logger.warning(f"⚠️ JSON decode error: {e}, line: {line[:100]}")
                                continue

        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
            logger.error(f"❌ Ollama streaming HTTP error: {error_msg}")
            raise Exception(error_msg)
        except httpx.TimeoutException as e:
            logger.error(f"❌ Ollama streaming timeout: {e}")
            raise Exception(f"Request timeout: {str(e)}")
        except Exception as e:
            logger.error(f"❌ Ollama streaming error: {type(e).__name__}: {e}")
            raise

    async def generate_embedding(
            self,
            text: str,
            model: Optional[str] = None
    ) -> Optional[List[float]]:
        """Генерировать эмбеддинг через Ollama"""
        embedding_model = model or self.default_model

        payload = {
            "model": embedding_model,
            "prompt": text
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.ollama_url}/api/embeddings",
                    json=payload
                )
                response.raise_for_status()
                data = response.json()
                return data.get("embedding")
        except Exception as e:
            logger.error(f"❌ Ollama embedding error: {e}")
            return None


# Singleton instance
ollama_provider = OllamaProvider()

"""
Ollama LLM Provider
Провайдер для работы с локальными моделями через Ollama

FIX 2026-02-20:
- Embeddings: используем ТОЛЬКО /api/embed (у пользователя /api/embeddings = 404).
- Жёсткий парсинг ответа: ожидаем {"embeddings":[[...]]} или {"embedding":[...]}.
- Если ответ неожиданный — логируем кусок payload, чтобы не гадать.
"""

import json
import logging
from typing import Optional, Dict, Any, List, AsyncGenerator

import httpx

from app.core.config import settings
from app.services.llm.providers.base import BaseLLMProvider

logger = logging.getLogger(__name__)


class OllamaProvider(BaseLLMProvider):
    """Провайдер для работы с Ollama"""

    def __init__(self):
        self.ollama_url = str(settings.EMBEDDINGS_BASEURL).rstrip("/")
        self.timeout = httpx.Timeout(120.0, connect=10.0, read=120.0)

        logger.info(f"🚀 OllamaProvider initialized: {self.ollama_url}")

    async def get_available_models(self) -> List[str]:
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(f"{self.ollama_url}/api/tags")
                response.raise_for_status()
                data = response.json()
                return [m["name"] for m in data.get("models", [])]
        except Exception as e:
            logger.error(f"❌ Failed to fetch Ollama models: {e}")
            return []

    async def generate_response(
        self,
        prompt: str,
        model: str,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
    ) -> Dict[str, Any]:
        _ = prompt_max_chars
        messages: List[Dict[str, str]] = []
        if conversation_history:
            messages.extend(conversation_history)
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {"temperature": temperature, "num_predict": max_tokens},
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            logger.info(f"📡 Sending request to Ollama chat: model={model}")
            response = await client.post(f"{self.ollama_url}/api/chat", json=payload)
            response.raise_for_status()
            data = response.json()

        return {
            "response": data["message"]["content"],
            "model": model,
            "tokens_used": data.get("eval_count", 0) + data.get("prompt_eval_count", 0),
        }

    async def generate_response_stream(
        self,
        prompt: str,
        model: str,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
    ) -> AsyncGenerator[str, None]:
        _ = prompt_max_chars
        messages: List[Dict[str, str]] = []
        if conversation_history:
            messages.extend(conversation_history)
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": model,
            "messages": messages,
            "stream": True,
            "options": {"temperature": temperature, "num_predict": max_tokens},
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            logger.info(f"📡 Starting Ollama stream: model={model}")
            async with client.stream("POST", f"{self.ollama_url}/api/chat", json=payload) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if "message" in data and "content" in data["message"]:
                        chunk = data["message"]["content"]
                        if chunk:
                            yield chunk

    async def generate_embedding(self, text: str, model: Optional[str] = None) -> Optional[List[float]]:
        """
        Ollama embeddings.

        IMPORTANT:
        - У пользователя /api/embeddings = 404.
        - Рабочий endpoint: POST /api/embed с {"model": "...", "input": "..."}.
        """
        embedding_model = model or settings.EMBEDDINGS_MODEL
        if not embedding_model:
            logger.error("❌ Embedding model is empty (None). Set EMBEDDINGS_MODEL in .env")
            return None

        payload = {"model": embedding_model, "input": text}

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(f"{self.ollama_url}/api/embed", json=payload)
                resp.raise_for_status()
                data = resp.json()

            # ожидаемый формат:
            # {"model":"...","embeddings":[[...]]}
            if isinstance(data, dict):
                if "embeddings" in data and isinstance(data["embeddings"], list) and data["embeddings"]:
                    first = data["embeddings"][0]
                    if isinstance(first, list) and first:
                        return first

                # на случай другого формата
                if "embedding" in data and isinstance(data["embedding"], list) and data["embedding"]:
                    return data["embedding"]

            logger.error(
                "❌ Ollama /api/embed returned unexpected payload for model=%s. Payload head: %s",
                embedding_model,
                str(data)[:200],
            )
            return None

        except httpx.HTTPStatusError as e:
            # логируем кусок тела, чтобы понять что вернуло
            body = ""
            try:
                body = e.response.text[:300]
            except Exception:
                body = "<unreadable>"
            logger.error(
                "❌ Ollama embedding HTTP error: %s for %s/api/embed model=%s body=%s",
                e.response.status_code,
                self.ollama_url,
                embedding_model,
                body,
            )
            return None
        except Exception as e:
            logger.error(f"❌ Ollama embedding error (/api/embed): {type(e).__name__}: {e}", exc_info=True)
            return None


# Singleton instance
ollama_provider = OllamaProvider()

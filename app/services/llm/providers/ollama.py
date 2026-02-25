import json
import logging
import time
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx

from app.core.config import settings
from app.observability.metrics import inc_counter, observe_ms
from app.services.llm.providers.base import BaseLLMProvider
from app.utils.retry import async_retry

logger = logging.getLogger(__name__)


class OllamaProvider(BaseLLMProvider):
    def __init__(self):
        self.ollama_url = str(settings.EMBEDDINGS_BASEURL).rstrip("/")
        self.timeout = httpx.Timeout(120.0, connect=10.0, read=120.0)
        logger.info("OllamaProvider initialized: %s", self.ollama_url)

    async def get_available_models(self) -> List[str]:
        try:
            async def _call() -> List[str]:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.get(f"{self.ollama_url}/api/tags")
                    response.raise_for_status()
                    data = response.json()
                    return [m["name"] for m in data.get("models", [])]

            models = await async_retry(_call, retries=2)
            inc_counter("llm_provider_success_total", provider="ollama", operation="models")
            return models
        except Exception as e:
            logger.error("Failed to fetch Ollama models: %s", e)
            inc_counter("llm_provider_error_total", provider="ollama", operation="models")
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

        started = time.perf_counter()
        try:
            async def _call() -> Dict[str, Any]:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.post(f"{self.ollama_url}/api/chat", json=payload)
                    response.raise_for_status()
                    return response.json()

            data = await async_retry(_call, retries=2)
            observe_ms("llm_provider_duration_ms", (time.perf_counter() - started) * 1000.0, provider="ollama", operation="chat")
            inc_counter("llm_provider_success_total", provider="ollama", operation="chat")
            return {
                "response": data["message"]["content"],
                "model": model,
                "tokens_used": data.get("eval_count", 0) + data.get("prompt_eval_count", 0),
            }
        except Exception:
            inc_counter("llm_provider_error_total", provider="ollama", operation="chat")
            raise

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

        started = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                async with client.stream("POST", f"{self.ollama_url}/api/chat", json=payload) as response:
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        if not line:
                            continue
                        try:
                            data = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        chunk = (data.get("message") or {}).get("content")
                        if chunk:
                            yield chunk
            observe_ms("llm_provider_duration_ms", (time.perf_counter() - started) * 1000.0, provider="ollama", operation="chat_stream")
            inc_counter("llm_provider_success_total", provider="ollama", operation="chat_stream")
        except Exception:
            inc_counter("llm_provider_error_total", provider="ollama", operation="chat_stream")
            raise

    async def generate_embedding(self, text: str, model: Optional[str] = None) -> Optional[List[float]]:
        embedding_model = model or settings.EMBEDDINGS_MODEL
        if not embedding_model:
            logger.error("Embedding model is empty. Set EMBEDDINGS_MODEL in .env")
            inc_counter("llm_provider_error_total", provider="ollama", operation="embedding")
            return None

        payload = {"model": embedding_model, "input": text}
        started = time.perf_counter()
        try:
            async def _call() -> Dict[str, Any]:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    resp = await client.post(f"{self.ollama_url}/api/embed", json=payload)
                    resp.raise_for_status()
                    return resp.json()

            data = await async_retry(_call, retries=2)
            observe_ms("llm_provider_duration_ms", (time.perf_counter() - started) * 1000.0, provider="ollama", operation="embedding")

            if isinstance(data, dict):
                if isinstance(data.get("embeddings"), list) and data["embeddings"]:
                    first = data["embeddings"][0]
                    if isinstance(first, list) and first:
                        inc_counter("llm_provider_success_total", provider="ollama", operation="embedding")
                        return first
                if isinstance(data.get("embedding"), list) and data["embedding"]:
                    inc_counter("llm_provider_success_total", provider="ollama", operation="embedding")
                    return data["embedding"]

            logger.error("Unexpected payload from /api/embed model=%s", embedding_model)
            inc_counter("llm_provider_error_total", provider="ollama", operation="embedding")
            return None
        except Exception as e:
            logger.error("Ollama embedding error: %s", e, exc_info=True)
            inc_counter("llm_provider_error_total", provider="ollama", operation="embedding")
            return None


ollama_provider = OllamaProvider()


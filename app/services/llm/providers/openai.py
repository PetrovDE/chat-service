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


class OpenAIProvider(BaseLLMProvider):
    def __init__(self):
        self.api_key = settings.OPENAI_API_KEY
        self.default_model = settings.OPENAI_MODEL
        self.timeout = httpx.Timeout(120.0, connect=10.0, read=120.0)
        self.base_url = "https://api.openai.com/v1"
        logger.info("OpenAIProvider initialized")

    def _get_headers(self) -> Dict[str, str]:
        if not self.api_key:
            raise ValueError("OpenAI API key is not configured")
        return {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

    async def get_available_models(self) -> List[str]:
        return ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo-preview", "gpt-4o"]

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

        payload = {"model": model, "messages": messages, "temperature": temperature, "max_tokens": max_tokens, "stream": False}
        headers = self._get_headers()
        started = time.perf_counter()
        try:
            async def _call() -> Dict[str, Any]:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.post(f"{self.base_url}/chat/completions", json=payload, headers=headers)
                    response.raise_for_status()
                    return response.json()

            data = await async_retry(_call, retries=2)
            observe_ms("llm_provider_duration_ms", (time.perf_counter() - started) * 1000.0, provider="openai", operation="chat")
            inc_counter("llm_provider_success_total", provider="openai", operation="chat")
            return {
                "response": data["choices"][0]["message"]["content"],
                "model": model,
                "tokens_used": data.get("usage", {}).get("total_tokens", 0),
            }
        except Exception as e:
            logger.error("OpenAI generation error: %s", e)
            inc_counter("llm_provider_error_total", provider="openai", operation="chat")
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

        payload = {"model": model, "messages": messages, "temperature": temperature, "max_tokens": max_tokens, "stream": True}
        headers = self._get_headers()
        started = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                async with client.stream("POST", f"{self.base_url}/chat/completions", json=payload, headers=headers) as response:
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        if not line.startswith("data: "):
                            continue
                        if line == "data: [DONE]":
                            break
                        try:
                            data = json.loads(line[6:])
                        except json.JSONDecodeError:
                            continue
                        choices = data.get("choices") or []
                        if not choices:
                            continue
                        chunk = (choices[0].get("delta") or {}).get("content")
                        if chunk:
                            yield chunk
            observe_ms("llm_provider_duration_ms", (time.perf_counter() - started) * 1000.0, provider="openai", operation="chat_stream")
            inc_counter("llm_provider_success_total", provider="openai", operation="chat_stream")
        except Exception as e:
            logger.error("OpenAI streaming error: %s", e)
            inc_counter("llm_provider_error_total", provider="openai", operation="chat_stream")
            raise

    async def generate_embedding(self, text: str, model: Optional[str] = None) -> Optional[List[float]]:
        embedding_model = model or "text-embedding-ada-002"
        payload = {"model": embedding_model, "input": text}
        headers = self._get_headers()
        started = time.perf_counter()
        try:
            async def _call() -> Dict[str, Any]:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.post(f"{self.base_url}/embeddings", json=payload, headers=headers)
                    response.raise_for_status()
                    return response.json()

            data = await async_retry(_call, retries=2)
            observe_ms("llm_provider_duration_ms", (time.perf_counter() - started) * 1000.0, provider="openai", operation="embedding")
            inc_counter("llm_provider_success_total", provider="openai", operation="embedding")
            return data["data"][0]["embedding"]
        except Exception as e:
            logger.error("OpenAI embedding error: %s", e)
            inc_counter("llm_provider_error_total", provider="openai", operation="embedding")
            return None


openai_provider = OpenAIProvider()


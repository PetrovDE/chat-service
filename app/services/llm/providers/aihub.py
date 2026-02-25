import json
import logging
import time
import uuid
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx
import numpy as np

from app.core.config import settings
from app.observability.metrics import inc_counter, observe_ms
from app.services.llm.providers.aihub_auth import AIHubAuthManager
from app.services.llm.providers.base import BaseLLMProvider
from app.utils.retry import async_retry

logger = logging.getLogger(__name__)


class AIHubProvider(BaseLLMProvider):
    def __init__(self):
        self.base_url = settings.AIHUB_URL.rstrip("/")
        self.timeout = httpx.Timeout(300.0, connect=10.0, read=300.0)
        self.verify_ssl = settings.AIHUB_VERIFY_SSL
        self.stream_path = getattr(settings, "AIHUB_CHAT_STREAM_PATH", "").strip()
        self.default_model = settings.AIHUB_DEFAULT_MODEL or "vikhr"
        self.embedding_model = settings.AIHUB_EMBEDDING_MODEL or "arctic"
        self.auth_manager = AIHubAuthManager()
        logger.info("AIHubProvider initialized: base_url=%s verify_ssl=%s", self.base_url, self.verify_ssl)

    def _build_chat_stream_url(self, model: str) -> Optional[str]:
        path = (self.stream_path or "").strip()
        if not path:
            return None
        if "{model}" in path:
            path = path.format(model=model)
        return f"{self.base_url}{path}"

    @staticmethod
    def _extract_stream_text(payload: Dict[str, Any]) -> str:
        if not isinstance(payload, dict):
            return ""
        if isinstance(payload.get("text"), str):
            return payload["text"]
        msg = payload.get("message")
        if isinstance(msg, dict) and isinstance(msg.get("text"), str):
            return msg["text"]
        delta = payload.get("delta")
        if isinstance(delta, dict) and isinstance(delta.get("text"), str):
            return delta["text"]
        choices = payload.get("choices")
        if isinstance(choices, list) and choices:
            ch0 = choices[0]
            if isinstance(ch0, dict):
                d = ch0.get("delta")
                if isinstance(d, dict) and isinstance(d.get("content"), str):
                    return d["content"]
                if isinstance(ch0.get("text"), str):
                    return ch0["text"]
        return ""

    async def _get_headers(self) -> Dict[str, str]:
        token = await self.auth_manager.get_token()
        if not token:
            raise RuntimeError("Failed to obtain AI HUB authentication token")
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "traceId": str(uuid.uuid4()),
        }

    def _prepare_messages(
        self,
        conversation_history: Optional[List[Dict[str, str]]],
        prompt: str,
        prompt_max_chars: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        messages: List[Dict[str, str]] = []

        max_history_chars = max(200, int(getattr(settings, "AIHUB_MAX_HISTORY_MESSAGE_CHARS", 2000) or 2000))
        configured_prompt_chars = int(getattr(settings, "AIHUB_MAX_PROMPT_CHARS", 50000) or 50000)
        req_chars = int(prompt_max_chars or 0)
        max_prompt_chars = max(2000, min(configured_prompt_chars, req_chars)) if req_chars > 0 else max(2000, configured_prompt_chars)

        if conversation_history:
            for msg in conversation_history:
                role = str(msg.get("role", "user"))[:20]
                text = str(msg.get("content") or msg.get("text") or "")[:max_history_chars]
                if text:
                    messages.append({"role": role, "text": text})

        prompt_text = (prompt or "")
        if len(prompt_text) > max_prompt_chars:
            logger.warning("AI HUB prompt truncated: %d -> %d chars", len(prompt_text), max_prompt_chars)
            prompt_text = prompt_text[:max_prompt_chars]
        messages.append({"role": "user", "text": prompt_text})

        if len(messages) > 10:
            messages = messages[-10:]
        return messages

    async def get_available_models(self) -> List[str]:
        detailed = await self.get_available_models_detailed()
        return [str(m.get("name")) for m in detailed if m.get("name")]

    async def get_available_models_detailed(self) -> List[Dict[str, Any]]:
        started = time.perf_counter()
        try:
            headers = await self._get_headers()
            async def _call() -> httpx.Response:
                async with httpx.AsyncClient(verify=self.verify_ssl, timeout=self.timeout) as client:
                    response = await client.get(f"{self.base_url}/models", headers=headers, params={"type": "chatbot"})
                    response.raise_for_status()
                    return response

            response = await async_retry(_call, retries=2)
            data = response.json()
            models = data if isinstance(data, list) else data.get("models", [])
            out: List[Dict[str, Any]] = []
            for item in models:
                if isinstance(item, str):
                    out.append({"name": item, "context_window": None, "max_output_tokens": None})
                    continue
                if not isinstance(item, dict):
                    continue
                name = item.get("id") or item.get("name") or item.get("model") or item.get("slug")
                if not name:
                    continue
                cw = item.get("contextWindow") or item.get("context_window") or item.get("maxContextTokens") or item.get("maxInputTokens") or item.get("inputTokenLimit")
                mo = item.get("maxOutputTokens") or item.get("outputTokenLimit") or item.get("max_new_tokens")
                try:
                    cw = int(cw) if cw is not None else None
                except Exception:
                    cw = None
                try:
                    mo = int(mo) if mo is not None else None
                except Exception:
                    mo = None
                out.append({"name": str(name), "context_window": cw, "max_output_tokens": mo})
            observe_ms("llm_provider_duration_ms", (time.perf_counter() - started) * 1000.0, provider="aihub", operation="models")
            inc_counter("llm_provider_success_total", provider="aihub", operation="models")
            return out
        except Exception as e:
            logger.error("Error getting AI HUB models: %s", e, exc_info=True)
            inc_counter("llm_provider_error_total", provider="aihub", operation="models")
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
        messages = self._prepare_messages(conversation_history, prompt, prompt_max_chars=prompt_max_chars)
        payload = {
            "messages": messages,
            "parameters": {"stream": False, "temperature": temperature, "maxTokens": str(max_tokens), "reasoningOptions": {"mode": "DISABLED"}},
        }
        started = time.perf_counter()
        try:
            headers = await self._get_headers()
            async def _call() -> httpx.Response:
                async with httpx.AsyncClient(verify=self.verify_ssl, timeout=self.timeout) as client:
                    response = await client.post(f"{self.base_url}/models/{model}/chat", headers=headers, json=payload)
                    response.raise_for_status()
                    return response

            response = await async_retry(_call, retries=2)
            data = response.json()
            msg = data.get("message", {})
            usage = data.get("usage", {})
            observe_ms("llm_provider_duration_ms", (time.perf_counter() - started) * 1000.0, provider="aihub", operation="chat")
            inc_counter("llm_provider_success_total", provider="aihub", operation="chat")
            return {
                "response": msg.get("text", ""),
                "model": model,
                "tokens_used": usage.get("totalTokens", 0),
                "finish_reason": data.get("finishReason", "stop"),
            }
        except Exception as e:
            logger.error("AI HUB generation error: %s", e, exc_info=True)
            inc_counter("llm_provider_error_total", provider="aihub", operation="chat")
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
        messages = self._prepare_messages(conversation_history, prompt, prompt_max_chars=prompt_max_chars)
        payload_stream = {
            "messages": messages,
            "parameters": {"stream": True, "temperature": temperature, "maxTokens": str(max_tokens), "reasoningOptions": {"mode": "DISABLED"}},
        }
        payload_non_stream = {
            "messages": messages,
            "parameters": {"stream": False, "temperature": temperature, "maxTokens": str(max_tokens), "reasoningOptions": {"mode": "DISABLED"}},
        }
        started = time.perf_counter()
        try:
            headers = await self._get_headers()
            stream_url = self._build_chat_stream_url(model)
            if stream_url:
                async with httpx.AsyncClient(verify=self.verify_ssl, timeout=self.timeout) as client:
                    async with client.stream("POST", stream_url, headers=headers, json=payload_stream) as response:
                        if response.status_code == 200:
                            emitted_any = False
                            async for line in response.aiter_lines():
                                raw = (line or "").strip()
                                if raw.startswith("data:"):
                                    raw = raw[5:].strip()
                                if not raw or raw == "[DONE]":
                                    continue
                                try:
                                    data = json.loads(raw)
                                except Exception:
                                    continue
                                chunk = self._extract_stream_text(data)
                                if chunk:
                                    emitted_any = True
                                    yield chunk
                            if emitted_any:
                                observe_ms("llm_provider_duration_ms", (time.perf_counter() - started) * 1000.0, provider="aihub", operation="chat_stream")
                                inc_counter("llm_provider_success_total", provider="aihub", operation="chat_stream")
                                return

            async with httpx.AsyncClient(verify=self.verify_ssl, timeout=self.timeout) as client:
                response = await client.post(f"{self.base_url}/models/{model}/chat", headers=headers, json=payload_non_stream)
                response.raise_for_status()
                content = (response.json().get("message") or {}).get("text", "")
                if content:
                    yield content
            observe_ms("llm_provider_duration_ms", (time.perf_counter() - started) * 1000.0, provider="aihub", operation="chat_stream")
            inc_counter("llm_provider_success_total", provider="aihub", operation="chat_stream")
        except Exception as e:
            logger.error("AI HUB streaming error: %s", e, exc_info=True)
            inc_counter("llm_provider_error_total", provider="aihub", operation="chat_stream")
            raise

    async def generate_embedding(self, text: str, model: Optional[str] = None) -> Optional[List[float]]:
        if not text or not text.strip():
            return None

        embedding_model = model or self.embedding_model
        payload = {"input": text.strip()}
        started = time.perf_counter()
        try:
            headers = await self._get_headers()
            async def _call() -> httpx.Response:
                async with httpx.AsyncClient(verify=self.verify_ssl, timeout=self.timeout) as client:
                    response = await client.post(f"{self.base_url}/models/{embedding_model}/embed", headers=headers, json=payload)
                    response.raise_for_status()
                    return response

            response = await async_retry(_call, retries=2)
            response_data = response.json()
            embedding_data = self._extract_embedding_from_response(response_data)
            if embedding_data is None:
                inc_counter("llm_provider_error_total", provider="aihub", operation="embedding")
                return None
            processed = self._process_embedding_array(embedding_data)
            if processed is None:
                inc_counter("llm_provider_error_total", provider="aihub", operation="embedding")
                return None
            observe_ms("llm_provider_duration_ms", (time.perf_counter() - started) * 1000.0, provider="aihub", operation="embedding")
            inc_counter("llm_provider_success_total", provider="aihub", operation="embedding")
            return processed.tolist()
        except Exception as e:
            logger.error("AI HUB embedding error: %s", e, exc_info=True)
            inc_counter("llm_provider_error_total", provider="aihub", operation="embedding")
            return None

    def _extract_embedding_from_response(self, response_data: Dict[str, Any]) -> Optional[Any]:
        if "embeddings" in response_data:
            embeddings = response_data["embeddings"]
            if isinstance(embeddings, list) and embeddings:
                return embeddings[0]
            return None
        if "embedding" in response_data:
            return response_data["embedding"]
        if isinstance(response_data.get("data"), list) and response_data["data"]:
            return response_data["data"][0].get("embedding")
        return None

    def _process_embedding_array(self, embedding_data: Any) -> Optional[np.ndarray]:
        try:
            arr = np.array(embedding_data)
            if arr.ndim == 0:
                return None
            if arr.ndim == 1:
                out = arr
            elif arr.ndim == 2 and arr.shape[0] == 1:
                out = arr[0]
            else:
                out = arr.flatten()
            if out.ndim != 1 or len(np.unique(out)) == 1:
                return None
            return out
        except Exception:
            return None


aihub_provider = AIHubProvider()


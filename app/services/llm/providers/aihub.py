"""
AI HUB LLM Provider
Провайдер для работы с AI HUB API (chat, embeddings, models)
"""
import logging
import json
import uuid
from typing import Optional, Dict, Any, List, AsyncGenerator
import httpx
import numpy as np

from app.core.config import settings
from app.services.llm.providers.base import BaseLLMProvider
from app.services.llm.providers.aihub_auth import AIHubAuthManager

logger = logging.getLogger(__name__)


class AIHubProvider(BaseLLMProvider):
    """Провайдер для работы с AI HUB"""

    def __init__(self):
        self.base_url = settings.AIHUB_URL.rstrip('/')
        # ✅ ИСПРАВЛЕНО: Увеличен таймаут до 120 секунд (2 минуты)
        self.timeout = httpx.Timeout(
            300.0,  # 5 минут общий таймаут            
            connect=10.0,  # 10 секунд на подключение
            read=300.0  # 5 минут на чтение ответа        
        )
        self.verify_ssl = settings.AIHUB_VERIFY_SSL
        self.stream_path = getattr(settings, "AIHUB_CHAT_STREAM_PATH", "").strip()
        self.default_model = "vikhr"  # ✅ Дефолтная модель для чата
        self.embedding_model = "arctic"  # ✅ Дефолтная модель для embedding
        self.auth_manager = AIHubAuthManager()

        self._log_config()

    def _log_config(self):
        """Логирование конфигурации провайдера"""
        logger.info("=" * 60)
        logger.info("🚀 AI HUB Provider Configuration")
        logger.info("=" * 60)
        logger.info(f"Base URL: {self.base_url}")
        logger.info(f"Verify SSL: {self.verify_ssl}")
        logger.info(f"Request Timeout: 300s (5 minutes)")        
        logger.info(f"Chat Stream Path: {self.stream_path or '(disabled)'}")
        logger.info(f"Default Model: {self.default_model}")
        logger.info(f"Embedding Model: {self.embedding_model}")
        logger.info("=" * 60)

    def _build_chat_stream_url(self, model: str) -> Optional[str]:
        """
        Optional dedicated stream endpoint from env AIHUB_CHAT_STREAM_PATH.
        Supports placeholder {model}, e.g.:
          /models/{model}/chat/stream
          /chat/stream
        """
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
        """Получить заголовки с актуальным токеном и traceId"""
        import sys

        print("=" * 80, file=sys.stderr)
        print("🔑 _get_headers() CALLED!", file=sys.stderr)
        print("=" * 80, file=sys.stderr)

        logger.info("=" * 80)
        logger.info("🔑 _get_headers() called - requesting token...")
        logger.info("=" * 80)

        try:
            print("🔑 About to call auth_manager.get_token()...", file=sys.stderr)
            token = await self.auth_manager.get_token()
            print(f"🔑 get_token() returned: {token is not None}", file=sys.stderr)

            if not token:
                print("❌ CRITICAL: No token returned!", file=sys.stderr)
                logger.error("❌ CRITICAL: Failed to obtain AI HUB authentication token!")
                raise Exception("Failed to obtain AI HUB authentication token")

            print(f"✅ Token obtained: {token[:30]}...", file=sys.stderr)
            logger.info(f"✅ Token obtained in _get_headers(): {token[:30]}...{token[-10:]}")

            # Генерируем уникальный traceId для каждого запроса
            trace_id = str(uuid.uuid4())

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "traceId": trace_id
            }

            logger.info(f"📤 Headers prepared | traceId: {trace_id}")
            logger.info("=" * 80)
            return headers

        except Exception as e:
            print(f"❌ EXCEPTION in _get_headers: {type(e).__name__}: {e}", file=sys.stderr)
            logger.error(f"❌ EXCEPTION in _get_headers: {type(e).__name__}: {e}", exc_info=True)
            raise

    def _prepare_messages(
            self,
            conversation_history: Optional[List[Dict[str, str]]],
            prompt: str,
            prompt_max_chars: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        """
        Подготовить сообщения в формате AI HUB API.
        Конвертирует 'content' -> 'text' и применяет ограничения API.
        """
        messages = []

        max_history_chars = max(200, int(getattr(settings, "AIHUB_MAX_HISTORY_MESSAGE_CHARS", 2000) or 2000))
        configured_prompt_chars = int(getattr(settings, "AIHUB_MAX_PROMPT_CHARS", 50000) or 50000)
        request_prompt_chars = int(prompt_max_chars or 0)
        if request_prompt_chars > 0:
            max_prompt_chars = max(2000, min(configured_prompt_chars, request_prompt_chars))
        else:
            max_prompt_chars = max(2000, configured_prompt_chars)

        # Добавляем историю
        if conversation_history:
            for msg in conversation_history:
                role = msg.get("role", "user")[:20]  # Макс 20 символов
                content = msg.get("content") or msg.get("text", "")
                text = content[:max_history_chars]

                if text:
                    messages.append({"role": role, "text": text})

        # Добавляем текущий запрос
        prompt_text = (prompt or "")
        original_len = len(prompt_text)
        if original_len > max_prompt_chars:
            logger.warning(
                "AI HUB prompt truncated: %d -> %d chars (set AIHUB_MAX_PROMPT_CHARS to increase)",
                original_len,
                max_prompt_chars,
            )
            prompt_text = prompt_text[:max_prompt_chars]

        messages.append({
            "role": "user",
            "text": prompt_text
        })

        # Ограничение API: максимум 10 сообщений
        if len(messages) > 10:
            logger.warning(f"⚠️ Truncating {len(messages)} messages to 10 (API limit)")
            if messages[0].get("role") == "system":
                messages = [messages[0]] + messages[-9:]
            else:
                messages = messages[-10:]

        return messages

    async def get_available_models(self) -> List[str]:
        detailed = await self.get_available_models_detailed()
        names: List[str] = []
        for m in detailed:
            name = str(m.get("name") or "").strip()
            if name:
                names.append(name)
        return names

    async def get_available_models_detailed(self) -> List[Dict[str, Any]]:
        """Return model list with optional limits (if AI HUB provides them)."""
        try:
            headers = await self._get_headers()
            url = f"{self.base_url}/models"

            logger.info(f"Fetching models from: {url}")

            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                response = await client.get(
                    url,
                    headers=headers,
                    params={"type": "chatbot"},
                    timeout=self.timeout,
                )

                logger.info(f"Models response: {response.status_code}")

                if response.status_code == 200:
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

                        context_window = (
                            item.get("contextWindow")
                            or item.get("context_window")
                            or item.get("maxContextTokens")
                            or item.get("maxInputTokens")
                            or item.get("inputTokenLimit")
                        )
                        max_output_tokens = (
                            item.get("maxOutputTokens")
                            or item.get("outputTokenLimit")
                            or item.get("max_new_tokens")
                        )

                        try:
                            context_window = int(context_window) if context_window is not None else None
                        except Exception:
                            context_window = None

                        try:
                            max_output_tokens = int(max_output_tokens) if max_output_tokens is not None else None
                        except Exception:
                            max_output_tokens = None

                        out.append(
                            {
                                "name": str(name),
                                "context_window": context_window,
                                "max_output_tokens": max_output_tokens,
                            }
                        )

                    logger.info("Available AI HUB models(detailed): %d", len(out))
                    return out

                logger.error("Failed to get models: %s", response.status_code)
                logger.error("Response: %s", response.text[:500])
                return []

        except Exception as e:
            logger.error("Error getting models: %s: %s", type(e).__name__, e, exc_info=True)
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
        """Генерировать полный ответ через AI HUB (без стриминга)"""

        messages = self._prepare_messages(conversation_history, prompt, prompt_max_chars=prompt_max_chars)

        payload = {
            "messages": messages,
            "parameters": {
                "stream": False,
                "temperature": temperature,
                "maxTokens": str(max_tokens),  # ✅ Строка
                "reasoningOptions": {"mode": "DISABLED"}
            }
        }

        try:
            headers = await self._get_headers()
            url = f"{self.base_url}/models/{model}/chat"

            logger.info(f"📡 Sending chat request | model: {model}")
            logger.debug(f"Payload: {json.dumps(payload, ensure_ascii=False)[:200]}...")

            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                response = await client.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=self.timeout
                )

                logger.info(f"📥 Response: {response.status_code}")

                if response.status_code == 200:
                    data = response.json()
                    message_data = data.get("message", {})
                    usage_data = data.get("usage", {})

                    result = {
                        "response": message_data.get("text", ""),
                        "model": model,
                        "tokens_used": usage_data.get("totalTokens", 0),
                        "finish_reason": data.get("finishReason", "stop")
                    }
                    logger.info(f"✅ Chat completed | tokens: {result['tokens_used']}")
                    return result
                else:
                    logger.error(f"❌ Chat error: {response.status_code}")
                    logger.error(f"Response: {response.text}")
                    raise Exception(f"AI HUB error: {response.status_code}")

        except Exception as e:
            logger.error(f"❌ Generation error: {e}")
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
        """
        Stream response via dedicated AI HUB stream endpoint if configured,
        otherwise fallback to non-stream /chat endpoint.
        """

        messages = self._prepare_messages(conversation_history, prompt, prompt_max_chars=prompt_max_chars)

        payload_stream = {
            "messages": messages,
            "parameters": {
                "stream": True,
                "temperature": temperature,
                "maxTokens": str(max_tokens),
                "reasoningOptions": {"mode": "DISABLED"}
            }
        }

        payload_non_stream = {
            "messages": messages,
            "parameters": {
                "stream": False,
                "temperature": temperature,
                "maxTokens": str(max_tokens),
                "reasoningOptions": {"mode": "DISABLED"}
            }
        }

        try:
            headers = await self._get_headers()
            stream_url = self._build_chat_stream_url(model)

            if stream_url:
                logger.info(f"?? Starting AI HUB stream request | model: {model} | url: {stream_url}")
                emitted_any = False
                async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                    async with client.stream(
                        "POST",
                        stream_url,
                        headers=headers,
                        json=payload_stream,
                        timeout=self.timeout,
                    ) as response:
                        if response.status_code == 200:
                            async for line in response.aiter_lines():
                                if not line:
                                    continue
                                raw = line.strip()
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
                                logger.info("? AI HUB streaming completed (stream endpoint)")
                                return
                            logger.warning("?? AI HUB stream endpoint returned no chunks, fallback to non-stream")
                        else:
                            body = (await response.aread())[:200]
                            logger.warning(
                                "?? AI HUB stream endpoint failed: %s %s (fallback to non-stream)",
                                response.status_code,
                                body,
                            )

            url = f"{self.base_url}/models/{model}/chat"
            logger.info(f"?? Starting chat request (non-stream fallback) | model: {model}")
            logger.debug(f"Payload: {json.dumps(payload_non_stream, ensure_ascii=False)[:200]}...")

            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                response = await client.post(
                    url,
                    headers=headers,
                    json=payload_non_stream,
                    timeout=self.timeout
                )

            logger.info(f"?? Response: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                message_data = data.get("message", {})
                content = message_data.get("text", "")

                if content:
                    logger.info(f"? Chat completed | length: {len(content)} chars")
                    yield content
                else:
                    logger.warning("?? Empty response from AI HUB")
            else:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                logger.error(f"? Chat error: {error_msg}")
                raise Exception(f"AI HUB error: {error_msg}")

        except httpx.TimeoutException:
            error_msg = "Request timeout after 300 seconds"
            logger.error(f"? {error_msg}")
            raise Exception(error_msg)
        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
            logger.error(f"? Streaming error: {error_msg}")
            raise Exception(error_msg)
        except Exception as e:
            logger.error(f"? Streaming error: {type(e).__name__}: {e}", exc_info=True)
            raise

    async def generate_embedding(

            self,
            text: str,
            model: Optional[str] = None
    ) -> Optional[List[float]]:
        """Генерировать эмбеддинг через AI HUB"""
        if not text or not text.strip():
            logger.warning("⚠️ Empty text for embedding")
            return None

        clean_text = text.strip()
        embedding_model = model or self.embedding_model

        logger.info(f"🔮 Generating embedding | model: {embedding_model} | text length: {len(clean_text)}")

        payload = {"input": clean_text}

        try:
            headers = await self._get_headers()
            url = f"{self.base_url}/models/{embedding_model}/embed"

            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                response = await client.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=self.timeout
                )

                logger.info(f"📥 Response: {response.status_code}")

                if response.status_code != 200:
                    logger.error(f"❌ Embedding error: {response.status_code}")
                    logger.error(f"Response: {response.text}")
                    return None

                response_data = response.json()
                embedding_data = self._extract_embedding_from_response(response_data)

                if not embedding_data:
                    return None

                # Обработка массива
                processed_embedding = self._process_embedding_array(embedding_data)

                if processed_embedding is not None:
                    final_list = processed_embedding.tolist()
                    logger.info(f"✅ Embedding generated | length: {len(final_list)}")
                    return final_list

                return None

        except httpx.TimeoutException:
            logger.error("❌ Embedding timeout")
            return None
        except httpx.ConnectError:
            logger.error("❌ Connection error")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}", exc_info=True)
            return None

    def _extract_embedding_from_response(self, response_data: dict) -> Optional[Any]:
        """Извлечь embedding из ответа API"""
        # ✅ API возвращает {"embeddings": [[...]]}
        if "embeddings" in response_data:
            embeddings = response_data["embeddings"]
            if isinstance(embeddings, list) and len(embeddings) > 0:
                logger.debug("Found embeddings in 'embeddings' field (2D array)")
                return embeddings[0]
            logger.error("❌ Embeddings array is empty")
            return None

        elif "embedding" in response_data:
            logger.debug("Found embedding in 'embedding' field")
            return response_data["embedding"]
        elif "data" in response_data and isinstance(response_data["data"], list):
            if len(response_data["data"]) > 0:
                logger.debug("Found embedding in OpenAI format")
                return response_data["data"][0].get("embedding")

        logger.error(f"❌ Embedding not found | keys: {list(response_data.keys())}")
        return None

    def _process_embedding_array(self, embedding_data) -> Optional[np.ndarray]:
        """Обработать массив embedding"""
        try:
            embedding_array = np.array(embedding_data)
            logger.debug(f"Raw embedding | shape: {embedding_array.shape}")

            if embedding_array.ndim == 0:
                logger.error("❌ Embedding is scalar")
                return None
            elif embedding_array.ndim == 1:
                processed = embedding_array
            elif embedding_array.ndim == 2:
                if embedding_array.shape[0] == 1:
                    processed = embedding_array[0]
                else:
                    processed = embedding_array.flatten()
                logger.debug(f"Flattened to shape: {processed.shape}")
            else:
                processed = embedding_array.flatten()
                logger.debug(f"Flattened {embedding_array.ndim}D to 1D")

            if processed.ndim != 1:
                logger.error(f"❌ Final embedding has {processed.ndim} dimensions")
                return None

            unique_values = len(np.unique(processed))
            if unique_values == 1:
                logger.error("❌ All values are identical!")
                return None

            logger.debug(
                f"Stats | unique: {unique_values}, "
                f"mean: {np.mean(processed):.4f}, "
                f"std: {np.std(processed):.4f}"
            )

            return processed

        except Exception as e:
            logger.error(f"❌ Error processing array: {e}")
            return None


# Singleton instance
aihub_provider = AIHubProvider()

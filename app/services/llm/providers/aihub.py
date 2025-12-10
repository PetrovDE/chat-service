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
        self.timeout = httpx.Timeout(
            settings.AIHUB_REQUEST_TIMEOUT,
            connect=10.0,
            read=settings.AIHUB_REQUEST_TIMEOUT
        )
        self.verify_ssl = settings.AIHUB_VERIFY_SSL
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
        logger.info(f"Request Timeout: {settings.AIHUB_REQUEST_TIMEOUT}s")
        logger.info(f"Default Model: {self.default_model}")
        logger.info(f"Embedding Model: {self.embedding_model}")
        logger.info("=" * 60)

    async def _get_headers(self) -> Dict[str, str]:
        """Получить заголовки с актуальным токеном и traceId"""
        logger.info("=" * 80)
        logger.info("🔑 _get_headers() called - requesting token...")
        logger.info("=" * 80)

        token = await self.auth_manager.get_token()

        if not token:
            logger.error("❌ CRITICAL: Failed to obtain AI HUB authentication token!")
            raise Exception("Failed to obtain AI HUB authentication token")

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

    def _prepare_messages(
            self,
            conversation_history: Optional[List[Dict[str, str]]],
            prompt: str
    ) -> List[Dict[str, str]]:
        """
        Подготовить сообщения в формате AI HUB API.
        Конвертирует 'content' -> 'text' и применяет ограничения API.
        """
        messages = []

        # Добавляем историю
        if conversation_history:
            for msg in conversation_history:
                role = msg.get("role", "user")[:20]  # Макс 20 символов
                content = msg.get("content") or msg.get("text", "")
                text = content[:1000]  # Макс 1000 символов

                if text:
                    messages.append({"role": role, "text": text})

        # Добавляем текущий запрос
        messages.append({
            "role": "user",
            "text": prompt[:1000]
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
        """Получить список доступных моделей из AI HUB"""
        try:
            headers = await self._get_headers()
            url = f"{self.base_url}/models"

            logger.info(f"📊 Fetching models from: {url}")

            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                response = await client.get(
                    url,
                    headers=headers,
                    params={"type": "chatbot"},
                    timeout=self.timeout
                )

                logger.info(f"📥 Response: {response.status_code}")

                if response.status_code == 200:
                    data = response.json()
                    # ✅ ИСПРАВЛЕНО: API возвращает список объектов моделей
                    models = data if isinstance(data, list) else data.get("models", [])
                    model_names = [m.get("id") if isinstance(m, dict) else m for m in models]
                    logger.info(f"✅ Available models: {model_names}")
                    return model_names
                else:
                    logger.error(f"❌ Failed to get models: {response.status_code}")
                    logger.error(f"Response: {response.text[:500]}")
                    return []

        except Exception as e:
            logger.error(f"❌ Error getting models: {type(e).__name__}: {e}", exc_info=True)
            return []

    async def generate_response(
            self,
            prompt: str,
            model: str,
            temperature: float = 0.7,
            max_tokens: int = 2000,
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Генерировать полный ответ через AI HUB (без стриминга)"""

        messages = self._prepare_messages(conversation_history, prompt)

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
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> AsyncGenerator[str, None]:
        """Генерировать ответ со стримингом через AI HUB"""

        messages = self._prepare_messages(conversation_history, prompt)

        payload = {
            "messages": messages,
            "parameters": {
                "stream": True,
                "temperature": temperature,
                "maxTokens": str(max_tokens),  # ✅ Строка
                "reasoningOptions": {"mode": "DISABLED"}
            }
        }

        try:
            headers = await self._get_headers()
            url = f"{self.base_url}/models/{model}/chat"

            logger.info(f"📡 Starting stream | model: {model}")

            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                async with client.stream(
                        "POST",
                        url,
                        headers=headers,
                        json=payload,
                        timeout=self.timeout
                ) as response:
                    response.raise_for_status()

                    async for line in response.aiter_lines():
                        if line:
                            try:
                                data = json.loads(line)
                                if "message" in data and "text" in data["message"]:
                                    content = data["message"]["text"]
                                    if content:
                                        yield content
                            except json.JSONDecodeError as e:
                                logger.warning(f"⚠️ JSON decode error: {e}")
                                continue

                    logger.info("✅ Stream completed")

        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
            logger.error(f"❌ Streaming error: {error_msg}")
            raise Exception(error_msg)
        except Exception as e:
            logger.error(f"❌ Streaming error: {type(e).__name__}: {e}")
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

"""
AI HUB LLM Provider
Провайдер для работы с AI HUB через Keycloak аутентификацию
"""
import logging
import json
import base64
from typing import Optional, Dict, Any, List, AsyncGenerator
from datetime import datetime, timedelta
import httpx
import numpy as np

from app.core.config import settings
from app.services.llm.providers.base import BaseLLMProvider

logger = logging.getLogger(__name__)


class AIHubAuthManager:
    """Менеджер аутентификации для AI HUB через Keycloak"""

    def __init__(self):
        self._token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
        self.keycloak_host = settings.AIHUB_KEYCLOAK_HOST
        self.username = settings.AIHUB_USERNAME
        self.password = settings.AIHUB_PASSWORD
        self.client_id = settings.AIHUB_CLIENT_ID
        self.client_secret = settings.AIHUB_CLIENT_SECRET

        # Определяем режим аутентификации
        # Если есть username/password - используем Password Grant
        # Если только client_id/client_secret - используем Client Credentials
        self.use_client_credentials = bool(
            self.client_id and
            self.client_secret and
            not (self.username and self.password)
        )

        # ✅ Логирование конфигурации (без секретов!)
        logger.info(f"🔑 AI HUB Auth Config:")
        logger.info(f"  - Keycloak Host: {self.keycloak_host}")
        logger.info(f"  - Auth Mode: {'Client Credentials (Basic Auth)' if self.use_client_credentials else 'Password Grant'}")

        if self.use_client_credentials:
            logger.info(f"  - Client ID: {self.client_id}")
            logger.info(f"  - Client Secret: {'*' * len(self.client_secret) if self.client_secret else 'NOT SET'}")
        else:
            logger.info(f"  - Username: {self.username}")
            logger.info(f"  - Password: {'*' * len(self.password) if self.password else 'NOT SET'}")
            logger.info(f"  - Client ID: {self.client_id}")
            logger.info(f"  - Client Secret: {'*' * len(self.client_secret) if self.client_secret else 'NOT SET'}")

    async def get_token(self) -> Optional[str]:
        """
        Получить JWT токен через Keycloak
        Поддерживает два режима:
        1. Client Credentials (Basic Auth в заголовках) - если нет username/password
        2. Password Grant (username/password) - если есть учетные данные
        """
        # Проверяем актуальность токена (с запасом 60 секунд)
        if self._token and self._token_expires_at:
            if datetime.now() < self._token_expires_at - timedelta(seconds=60):
                logger.debug("🔑 Using cached AI HUB token")
                return self._token

        # Получаем новый токен
        if self.use_client_credentials:
            return await self._get_token_client_credentials()
        else:
            return await self._get_token_password_grant()

    async def _get_token_client_credentials(self) -> Optional[str]:
        """
        Получение токена через Client Credentials flow с Basic Auth
        """
        logger.info("🔑 Requesting new AI HUB token (Client Credentials with Basic Auth)...")

        # Кодируем client credentials для Basic Auth
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

        headers = {
            'Authorization': f'Basic {encoded_credentials}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        data = {
            "grant_type": "client_credentials"
        }

        try:
            async with httpx.AsyncClient(verify=settings.AIHUB_VERIFY_SSL) as client:
                logger.info(f"🔑 Sending auth request to: {self.keycloak_host}")
                logger.debug(f"🔑 Using Basic Auth with client_id: {self.client_id}")

                response = await client.post(
                    self.keycloak_host,
                    data=data,
                    headers=headers,
                    timeout=30.0
                )

                logger.info(f"🔑 Keycloak response status: {response.status_code}")

                if response.status_code == 200:
                    token_info = response.json()
                    self._token = token_info.get("access_token")

                    if not self._token:
                        logger.error("❌ Token received but 'access_token' field is missing!")
                        logger.error(f"Response keys: {list(token_info.keys())}")
                        return None

                    # Вычисляем время истечения токена
                    expires_in = token_info.get("expires_in", 300)  # По умолчанию 5 минут
                    self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)

                    token_preview = f"{self._token[:20]}...{self._token[-20:]}" if len(self._token) > 40 else "[short token]"
                    logger.info(f"✅ Successfully obtained AI HUB token (Client Credentials): {token_preview}")
                    logger.info(f"✅ Token expires in {expires_in}s")
                    return self._token
                else:
                    logger.error(f"❌ Failed to get AI HUB token - Status: {response.status_code}")
                    logger.error(f"❌ Response headers: {dict(response.headers)}")

                    try:
                        error_info = response.json()
                        logger.error(f"❌ Error response body: {json.dumps(error_info, indent=2)}")
                        error_msg = error_info.get("error_description") or error_info.get("error", f"Status code: {response.status_code}")
                    except Exception:
                        error_msg = f"Status code: {response.status_code}"
                        logger.error(f"❌ Raw response text: {response.text[:500]}")

                    logger.error(f"❌ Error message: {error_msg}")
                    return None

        except httpx.TimeoutException:
            logger.error("❌ Keycloak authentication timeout")
            return None
        except httpx.ConnectError as e:
            logger.error(f"❌ Keycloak connection error: {e}")
            logger.error(f"❌ Make sure Keycloak is accessible at: {self.keycloak_host}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error getting AI HUB token: {type(e).__name__}: {e}", exc_info=True)
            return None

    async def _get_token_password_grant(self) -> Optional[str]:
        """
        Получение токена через Password Grant flow (существующий метод)
        """
        logger.info("🔑 Requesting new AI HUB token (Password Grant)...")

        data = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": self.username,
            "password": self.password,
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }

        try:
            async with httpx.AsyncClient(verify=settings.AIHUB_VERIFY_SSL) as client:
                logger.info(f"🔑 Sending auth request to: {self.keycloak_host}")

                response = await client.post(
                    self.keycloak_host,
                    data=data,
                    headers=headers,
                    timeout=30.0
                )

                logger.info(f"🔑 Keycloak response status: {response.status_code}")

                if response.status_code == 200:
                    token_info = response.json()
                    self._token = token_info.get("access_token")

                    if not self._token:
                        logger.error("❌ Token received but 'access_token' field is missing!")
                        logger.error(f"Response keys: {list(token_info.keys())}")
                        return None

                    expires_in = token_info.get("expires_in", 300)
                    self._token_expires_at = datetime.now() + timedelta(seconds=expires_in)

                    token_preview = f"{self._token[:20]}...{self._token[-20:]}" if len(self._token) > 40 else "[short token]"
                    logger.info(f"✅ Successfully obtained AI HUB token (Password): {token_preview}")
                    logger.info(f"✅ Token expires in {expires_in}s")
                    return self._token
                else:
                    logger.error(f"❌ Failed to get AI HUB token - Status: {response.status_code}")
                    logger.error(f"❌ Response headers: {dict(response.headers)}")

                    try:
                        error_info = response.json()
                        logger.error(f"❌ Error response body: {json.dumps(error_info, indent=2)}")
                        error_msg = error_info.get("error_description") or error_info.get("error", f"Status code: {response.status_code}")
                    except Exception:
                        error_msg = f"Status code: {response.status_code}"
                        logger.error(f"❌ Raw response text: {response.text[:500]}")

                    logger.error(f"❌ Error message: {error_msg}")
                    return None

        except httpx.TimeoutException:
            logger.error("❌ Keycloak authentication timeout")
            return None
        except httpx.ConnectError as e:
            logger.error(f"❌ Keycloak connection error: {e}")
            logger.error(f"❌ Make sure Keycloak is accessible at: {self.keycloak_host}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error getting AI HUB token: {type(e).__name__}: {e}", exc_info=True)
            return None

    def clear_token(self):
        """Очистить кешированный токен"""
        self._token = None
        self._token_expires_at = None
        logger.info("🗑️ AI HUB token cache cleared")


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
        self.default_model = settings.AIHUB_DEFAULT_MODEL
        self.embedding_model = settings.AIHUB_EMBEDDING_MODEL
        self.auth_manager = AIHubAuthManager()

        logger.info(f"🚀 AIHubProvider initialized")
        logger.info(f"  - Base URL: {self.base_url}")
        logger.info(f"  - Verify SSL: {self.verify_ssl}")
        logger.info(f"  - Default Model: {self.default_model}")
        logger.info(f"  - Embedding Model: {self.embedding_model}")

    async def _get_headers(self) -> Dict[str, str]:
        """Получить заголовки с актуальным токеном"""
        token = await self.auth_manager.get_token()
        if not token:
            raise Exception("Failed to obtain AI HUB authentication token")

        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _prepare_messages(self, conversation_history: Optional[List[Dict[str, str]]], prompt: str) -> List[Dict[str, str]]:
        """
        Подготовить сообщения в формате AI HUB API.
        Конвертирует 'content' -> 'text' и применяет ограничения API.
        """
        messages = []

        # Добавляем системное сообщение, если есть в истории
        if conversation_history:
            for msg in conversation_history:
                role = msg.get("role", "user")[:20]  # Ограничение: макс 20 символов

                # ✅ ИСПРАВЛЕНИЕ: конвертируем 'content' в 'text'
                content = msg.get("content") or msg.get("text", "")
                text = content[:1000]  # Ограничение: макс 1000 символов

                if text:  # Пропускаем пустые сообщения
                    messages.append({
                        "role": role,
                        "text": text
                    })

        # Добавляем текущий запрос
        messages.append({
            "role": "user",
            "text": prompt[:1000]  # Ограничение: макс 1000 символов
        })

        # ✅ Ограничение API: максимум 10 сообщений
        if len(messages) > 10:
            logger.warning(f"⚠️ Truncating messages from {len(messages)} to 10 (API limit)")
            # Оставляем первое (обычно system) и последние 9
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

                logger.info(f"📊 Models response status: {response.status_code}")

                if response.status_code == 200:
                    data = response.json()
                    models = data.get("models", [])
                    model_names = [m["id"] for m in models]
                    logger.info(f"📋 Available AI HUB models: {model_names}")
                    return model_names
                else:
                    logger.error(f"❌ Failed to get AI HUB models: {response.status_code}")
                    logger.error(f"❌ Response: {response.text[:500]}")
                    return []

        except Exception as e:
            logger.error(f"❌ Error getting AI HUB models: {type(e).__name__}: {e}", exc_info=True)
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
                "maxTokens": max_tokens,
                "reasoningOptions": {
                    "mode": "DISABLED"
                }
            }
        }

        try:
            headers = await self._get_headers()

            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                logger.info(f"📡 Sending request to AI HUB: model={model}")
                response = await client.post(
                    f"{self.base_url}/models/{model}/chat",
                    headers=headers,
                    json=payload,
                    timeout=self.timeout
                )

                if response.status_code == 200:
                    data = response.json()
                    message_data = data.get("message", {})
                    usage_data = data.get("usage", {})

                    return {
                        "response": message_data.get("text", ""),
                        "model": model,
                        "tokens_used": usage_data.get("totalTokens", 0),
                        "finish_reason": data.get("finishReason", "stop")
                    }
                else:
                    logger.error(f"❌ AI HUB chat error: {response.status_code} - {response.text}")
                    raise Exception(f"AI HUB error: {response.status_code}")

        except Exception as e:
            logger.error(f"❌ AI HUB generation error: {e}")
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
                "maxTokens": max_tokens,
                "reasoningOptions": {
                    "mode": "DISABLED"
                }
            }
        }

        try:
            headers = await self._get_headers()

            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                logger.info(f"📡 Starting AI HUB stream: model={model}")

                async with client.stream(
                        "POST",
                        f"{self.base_url}/models/{model}/chat",
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
                                logger.warning(f"⚠️ JSON decode error: {e}, line: {line[:100]}")
                                continue

        except httpx.HTTPStatusError as e:
            error_msg = f"HTTP {e.response.status_code}: {e.response.text[:200]}"
            logger.error(f"❌ AI HUB streaming HTTP error: {error_msg}")
            raise Exception(error_msg)
        except Exception as e:
            logger.error(f"❌ AI HUB streaming error: {type(e).__name__}: {e}")
            raise

    async def generate_embedding(
            self,
            text: str,
            model: Optional[str] = None
    ) -> Optional[List[float]]:
        """
        Генерировать эмбеддинг через AI HUB
        Полностью совместимо с вашим примером из connect_to_kc.py
        """
        if not text or not text.strip():
            logger.warning("⚠️ Empty text provided for embedding")
            return None

        clean_text = text.strip()
        embedding_model = model or self.embedding_model
        logger.info(f"🔮 Generating embedding for text: {clean_text[:100]}... (model: {embedding_model})")

        payload = {
            "input": clean_text
        }

        try:
            headers = await self._get_headers()

            async with httpx.AsyncClient(verify=self.verify_ssl) as client:
                response = await client.post(
                    f"{self.base_url}/models/{embedding_model}/embed",
                    headers=headers,
                    json=payload,
                    timeout=self.timeout
                )

                logger.info(f"📊 AI Hub embedding response status: {response.status_code}")

                if response.status_code != 200:
                    logger.error(f"❌ AI Hub response status: {response.status_code} - {response.text}")
                    return None

                response_data = response.json()
                embedding_data = None

                # Пробуем разные форматы ответа
                if "embedding" in response_data:
                    embedding_data = response_data["embedding"]
                    logger.info("✅ Found embedding in 'embedding' field")
                elif "embeddings" in response_data:
                    if isinstance(response_data["embeddings"], list) and len(response_data["embeddings"]) > 0:
                        embedding_data = response_data["embeddings"][0]
                        logger.info("✅ Found embeddings in 'embeddings' list (first element)")
                    else:
                        embedding_data = response_data["embeddings"]
                        logger.info("✅ Found embeddings in 'embeddings' element")
                elif "data" in response_data and isinstance(response_data["data"], list) and len(
                        response_data["data"]) > 0:
                    embedding_data = response_data["data"][0].get("embedding")
                    logger.info("✅ Found embedding in OpenAI-compatible format")
                else:
                    logger.error(f"❌ Embedding not found in response. Available keys: {list(response_data.keys())}")
                    return None

                if not embedding_data:
                    logger.error("❌ Embedding data is empty")
                    return None

                # Обработка массива
                embedding_array = np.array(embedding_data)
                logger.info(f"📐 Raw embedding - shape: {embedding_array.shape}, dtype: {embedding_array.dtype}")

                # Проверка размерности
                if embedding_array.ndim == 0:
                    logger.error("❌ Embedding is scalar, expected vector")
                    return None
                elif embedding_array.ndim == 1:
                    processed_embedding = embedding_array
                elif embedding_array.ndim == 2:
                    if embedding_array.shape[0] == 1:
                        processed_embedding = embedding_array[0]
                        logger.info(f"🔄 Flattened 2D array to 1D, new shape: {processed_embedding.shape}")
                    else:
                        processed_embedding = embedding_array.flatten()
                        logger.info(f"🔄 Flattened 2D array, new shape: {processed_embedding.shape}")
                else:
                    processed_embedding = embedding_array.flatten()
                    logger.info(f"🔄 Flattened {embedding_array.ndim}D array, new shape: {processed_embedding.shape}")

                if processed_embedding.ndim != 1:
                    logger.error(f"❌ Final embedding has {processed_embedding.ndim} dimensions, expected 1")
                    return None

                # Статистика
                unique_values = len(np.unique(processed_embedding))
                logger.info(
                    f"📊 Embedding stats - unique values: {unique_values}, "
                    f"mean: {np.mean(processed_embedding):.6f}, "
                    f"std: {np.std(processed_embedding):.6f}, "
                    f"min: {np.min(processed_embedding):.6f}, "
                    f"max: {np.max(processed_embedding):.6f}, "
                    f"norm: {np.linalg.norm(processed_embedding):.6f}"
                )

                if unique_values == 1:
                    logger.error(f"❌ All embedding values are identical! Unique values: {unique_values}")
                    return None

                final_embedding_list = processed_embedding.tolist()
                logger.info(f"✅ Embedding generated successfully, length: {len(final_embedding_list)}")
                return final_embedding_list

        except httpx.TimeoutException:
            logger.error("❌ AI Hub API timeout")
            return None
        except httpx.ConnectError:
            logger.error("❌ AI Hub API connection error")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected error in generate_embedding: {e}", exc_info=True)
            return None


# Singleton instance
aihub_provider = AIHubProvider()

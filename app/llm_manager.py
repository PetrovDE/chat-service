# app/llm_manager.py
import httpx
from typing import Optional, AsyncGenerator, List, Dict
import json
import logging
from app.config import settings

logger = logging.getLogger(__name__)


class LLMManager:
    """Менеджер для работы с LLM (Ollama и OpenAI)"""

    def __init__(self):
        """Инициализация базовых параметров"""
        self.ollama_host = settings.OLLAMA_HOST
        self.ollama_model = settings.OLLAMA_MODEL
        self.openai_api_key = settings.OPENAI_API_KEY
        self.openai_model = settings.OPENAI_MODEL
        self.default_source = settings.DEFAULT_MODEL_SOURCE

        # Активная конфигурация
        self.active_source = self.default_source
        self.active_model = None

        # Состояние инициализации
        self._initialized = False
        self._ollama_available = False
        self._openai_available = False
        self._available_ollama_models = []

    async def initialize(self):
        """
        Асинхронная инициализация LLM Manager
        Проверяет доступность сервисов и моделей
        """
        if self._initialized:
            logger.debug("LLM Manager already initialized")
            return

        logger.info("Initializing LLM Manager...")

        # Проверка Ollama
        await self._check_ollama_availability()

        # Проверка OpenAI
        await self._check_openai_availability()

        # Установка активной модели
        self._set_initial_active_model()

        self._initialized = True
        logger.info("LLM Manager initialized successfully")
        logger.info(f"  - Active source: {self.active_source}")
        logger.info(f"  - Active model: {self.active_model or 'default'}")
        logger.info(f"  - Ollama available: {self._ollama_available}")
        logger.info(f"  - OpenAI available: {self._openai_available}")

    async def _check_ollama_availability(self):
        """Проверить доступность Ollama"""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.ollama_host}/api/tags")

                if response.status_code == 200:
                    data = response.json()
                    self._available_ollama_models = [
                        model["name"] for model in data.get("models", [])
                    ]
                    self._ollama_available = True
                    logger.info(f"Ollama available with {len(self._available_ollama_models)} models")

                    # Проверить наличие дефолтной модели
                    if self.ollama_model not in self._available_ollama_models:
                        logger.warning(f"Default Ollama model '{self.ollama_model}' not found")
                        if self._available_ollama_models:
                            suggested = self._available_ollama_models[0]
                            logger.info(f"Available models: {', '.join(self._available_ollama_models)}")
                            logger.info(f"Consider using: {suggested}")
                else:
                    logger.warning(f"Ollama responded with status {response.status_code}")
                    self._ollama_available = False

        except httpx.ConnectError:
            logger.warning(f"Cannot connect to Ollama at {self.ollama_host}")
            logger.info("To use Ollama: Install from https://ollama.ai/ and run 'ollama pull llama3.1:8b'")
            self._ollama_available = False
        except Exception as e:
            logger.error(f"Error checking Ollama availability: {e}")
            self._ollama_available = False

    async def _check_openai_availability(self):
        """Проверить доступность OpenAI API"""
        if not self.openai_api_key:
            logger.debug("OpenAI API key not configured")
            self._openai_available = False
            return

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(
                    "https://api.openai.com/v1/models",
                    headers={"Authorization": f"Bearer {self.openai_api_key}"}
                )

                if response.status_code == 200:
                    self._openai_available = True
                    logger.info("OpenAI API available")
                else:
                    logger.warning(f"OpenAI API responded with status {response.status_code}")
                    self._openai_available = False

        except Exception as e:
            logger.debug(f"OpenAI API check failed: {e}")
            self._openai_available = False

    def _set_initial_active_model(self):
        """Установить начальную активную модель"""
        if self.default_source == "ollama" and self._ollama_available:
            self.active_model = self.ollama_model
        elif self.default_source == "openai" and self._openai_available:
            self.active_model = self.openai_model
        elif self._ollama_available:
            # Fallback на Ollama если доступен
            self.active_source = "ollama"
            self.active_model = self.ollama_model
            logger.info(f"Falling back to Ollama (default source unavailable)")
        elif self._openai_available:
            # Fallback на OpenAI если доступен
            self.active_source = "openai"
            self.active_model = self.openai_model
            logger.info(f"Falling back to OpenAI (Ollama unavailable)")
        else:
            logger.warning("No LLM services available!")
            self.active_model = self.ollama_model  # Keep default for later

    def is_available(self) -> bool:
        """Проверить доступность хотя бы одного LLM сервиса"""
        return self._ollama_available or self._openai_available

    def get_status(self) -> Dict:
        """Получить статус LLM Manager"""
        return {
            "initialized": self._initialized,
            "active_source": self.active_source,
            "active_model": self.active_model,
            "ollama_available": self._ollama_available,
            "openai_available": self._openai_available,
            "available_ollama_models": self._available_ollama_models
        }

    def set_active_model(self, source: str, model: str):
        """Установить активную модель"""
        if source == "ollama" and not self._ollama_available:
            raise ValueError("Ollama is not available")
        if source == "openai" and not self._openai_available:
            raise ValueError("OpenAI is not available")

        self.active_source = source
        self.active_model = model
        logger.info(f"Active model changed to: {source}/{model}")

    def get_active_model(self) -> Dict:
        """Получить информацию об активной модели"""
        return {
            "source": self.active_source,
            "model": self.active_model or (
                self.ollama_model if self.active_source == "ollama" else self.openai_model
            )
        }

    async def generate_response(
            self,
            prompt: str,
            model_source: Optional[str] = None,
            model_name: Optional[str] = None,
            temperature: float = 0.7,
            max_tokens: int = 2000,
            conversation_history: List[Dict] = None
    ) -> Dict:
        """Генерация ответа (без streaming)"""
        source = model_source or self.active_source

        if source == "ollama":
            return await self._generate_ollama(
                prompt=prompt,
                model=model_name or self.ollama_model,
                temperature=temperature,
                max_tokens=max_tokens,
                conversation_history=conversation_history
            )
        elif source == "openai":
            return await self._generate_openai(
                prompt=prompt,
                model=model_name or self.openai_model,
                temperature=temperature,
                max_tokens=max_tokens,
                conversation_history=conversation_history
            )
        else:
            raise ValueError(f"Unknown model source: {source}")

    async def generate_response_stream(
            self,
            prompt: str,
            model_source: Optional[str] = None,
            model_name: Optional[str] = None,
            temperature: float = 0.7,
            max_tokens: int = 2000,
            conversation_history: List[Dict] = None
    ) -> AsyncGenerator[str, None]:
        """Генерация ответа с потоковой передачей (streaming)"""
        source = model_source or self.active_source

        if source == "ollama":
            async for chunk in self._generate_ollama_stream(
                    prompt=prompt,
                    model=model_name or self.ollama_model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    conversation_history=conversation_history
            ):
                yield chunk
        elif source == "openai":
            async for chunk in self._generate_openai_stream(
                    prompt=prompt,
                    model=model_name or self.openai_model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    conversation_history=conversation_history
            ):
                yield chunk
        else:
            raise ValueError(f"Unknown model source: {source}")

    async def _generate_ollama(
            self,
            prompt: str,
            model: str,
            temperature: float,
            max_tokens: int,
            conversation_history: List[Dict] = None
    ) -> Dict:
        """Генерация через Ollama (без streaming)"""
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                payload = {
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "num_predict": max_tokens
                    }
                }

                logger.debug(f"Sending request to Ollama: {self.ollama_host}/api/generate")

                response = await client.post(
                    f"{self.ollama_host}/api/generate",
                    json=payload
                )
                response.raise_for_status()

                data = response.json()

                return {
                    "response": data.get("response", ""),
                    "model": model,
                    "tokens_used": data.get("eval_count", 0),
                    "generation_time": data.get("total_duration", 0) / 1e9
                }

        except httpx.ConnectError as e:
            logger.error(f"Ollama connection failed: {e}")
            raise Exception(f"Cannot connect to Ollama at {self.ollama_host}. Make sure Ollama is running.")
        except httpx.HTTPStatusError as e:
            logger.error(f"Ollama HTTP error: {e}")
            raise Exception(f"Ollama API error: {e.response.status_code}")
        except Exception as e:
            logger.error(f"Ollama request failed: {e}")
            raise Exception(f"Failed to get response from Ollama: {e}")

    async def _generate_ollama_stream(
            self,
            prompt: str,
            model: str,
            temperature: float,
            max_tokens: int,
            conversation_history: List[Dict] = None
    ) -> AsyncGenerator[str, None]:
        """Генерация через Ollama с потоковой передачей"""
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                payload = {
                    "model": model,
                    "prompt": prompt,
                    "stream": True,
                    "options": {
                        "temperature": temperature,
                        "num_predict": max_tokens
                    }
                }

                logger.debug(f"Starting Ollama stream: {self.ollama_host}/api/generate")

                async with client.stream(
                        "POST",
                        f"{self.ollama_host}/api/generate",
                        json=payload
                ) as response:
                    response.raise_for_status()

                    async for line in response.aiter_lines():
                        if line:
                            try:
                                data = json.loads(line)
                                if "response" in data:
                                    yield data["response"]
                            except json.JSONDecodeError:
                                continue

        except httpx.ConnectError as e:
            logger.error(f"Ollama streaming connection failed: {e}")
            raise Exception(f"Cannot connect to Ollama at {self.ollama_host}")
        except Exception as e:
            logger.error(f"Ollama streaming failed: {e}")
            raise Exception(f"Failed to get streaming response from Ollama: {e}")

    async def _generate_openai(
            self,
            prompt: str,
            model: str,
            temperature: float,
            max_tokens: int,
            conversation_history: List[Dict] = None
    ) -> Dict:
        """Генерация через OpenAI (без streaming)"""
        if not self.openai_api_key:
            raise Exception("OpenAI API key not configured")

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
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

                response = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    json=payload,
                    headers={"Authorization": f"Bearer {self.openai_api_key}"}
                )
                response.raise_for_status()

                data = response.json()

                return {
                    "response": data["choices"][0]["message"]["content"],
                    "model": model,
                    "tokens_used": data["usage"]["total_tokens"],
                    "generation_time": 0
                }

        except Exception as e:
            logger.error(f"OpenAI request failed: {e}")
            raise Exception(f"Failed to get response from OpenAI: {e}")

    async def _generate_openai_stream(
            self,
            prompt: str,
            model: str,
            temperature: float,
            max_tokens: int,
            conversation_history: List[Dict] = None
    ) -> AsyncGenerator[str, None]:
        """Генерация через OpenAI с потоковой передачей"""
        if not self.openai_api_key:
            raise Exception("OpenAI API key not configured")

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
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

                async with client.stream(
                        "POST",
                        "https://api.openai.com/v1/chat/completions",
                        json=payload,
                        headers={"Authorization": f"Bearer {self.openai_api_key}"}
                ) as response:
                    response.raise_for_status()

                    async for line in response.aiter_lines():
                        if line.startswith("data: "):
                            line = line[6:]

                            if line == "[DONE]":
                                break

                            try:
                                data = json.loads(line)
                                if "choices" in data and len(data["choices"]) > 0:
                                    delta = data["choices"][0].get("delta", {})
                                    if "content" in delta:
                                        yield delta["content"]
                            except json.JSONDecodeError:
                                continue

        except Exception as e:
            logger.error(f"OpenAI streaming failed: {e}")
            raise Exception(f"Failed to get streaming response from OpenAI: {e}")

    async def list_available_models(self, source: str) -> List[str]:
        """Получить список доступных моделей"""
        if source == "ollama":
            if not self._ollama_available:
                return []
            return self._available_ollama_models
        elif source == "openai":
            # OpenAI models are predefined
            return ["gpt-4", "gpt-4-turbo-preview", "gpt-3.5-turbo", "gpt-3.5-turbo-16k"]
        return []


# Глобальный экземпляр
llm_manager = LLMManager()
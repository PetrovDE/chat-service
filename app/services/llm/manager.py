# app/services/llm/manager.py
import logging
import httpx
import json
from typing import Optional, Dict, Any, List, AsyncGenerator
from app.core.config import settings

logger = logging.getLogger(__name__)


class LLMManager:
    def __init__(self):
        self.ollama_url = str(settings.EMBEDDINGS_BASEURL).rstrip('/')
        self.ollama_model = settings.EMBEDDINGS_MODEL
        self.openai_api_key = settings.OPENAI_API_KEY
        self.openai_model = settings.OPENAI_MODEL
        self.default_source = settings.DEFAULT_MODEL_SOURCE

        # ИСПРАВЛЕНО: увеличен timeout для больших моделей
        self.timeout = httpx.Timeout(120.0, connect=10.0, read=120.0)

        logger.info(f"LLMManager initialized with default source: {self.default_source}")
        logger.info(f"Ollama URL: {self.ollama_url}, Model: {self.ollama_model}")

    async def get_available_models(self, source: str = "ollama") -> List[str]:
        """Get list of available models from the specified source"""
        if source == "ollama":
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.get(
                        f"{self.ollama_url}/api/tags"
                    )
                    response.raise_for_status()
                    data = response.json()
                    return [model["name"] for model in data.get("models", [])]
            except Exception as e:
                logger.error(f"Failed to fetch Ollama models: {e}")
                return []
        elif source == "openai":
            return ["gpt-3.5-turbo", "gpt-4", "gpt-4-turbo-preview"]
        else:
            return []

    async def generate_response(
            self,
            prompt: str,
            model_source: Optional[str] = None,
            model_name: Optional[str] = None,
            temperature: float = 0.7,
            max_tokens: int = 2000,
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Generate a complete response (non-streaming)"""
        source = model_source or self.default_source
        model = model_name or (self.ollama_model if source == "ollama" else self.openai_model)

        logger.info(f"🔧 Generating response: source={source}, model={model}")

        if source == "ollama":
            return await self._ollama_generate(
                prompt, model, temperature, max_tokens, conversation_history
            )
        elif source == "openai":
            return await self._openai_generate(
                prompt, model, temperature, max_tokens, conversation_history
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
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> AsyncGenerator[str, None]:
        """Generate a streaming response"""
        source = model_source or self.default_source
        model = model_name or (self.ollama_model if source == "ollama" else self.openai_model)

        logger.info(f"🔧 Streaming response: source={source}, model={model}")

        if source == "ollama":
            async for chunk in self._ollama_stream(
                    prompt, model, temperature, max_tokens, conversation_history
            ):
                yield chunk
        elif source == "openai":
            async for chunk in self._openai_stream(
                    prompt, model, temperature, max_tokens, conversation_history
            ):
                yield chunk
        else:
            raise ValueError(f"Unknown model source: {source}")

    async def _ollama_generate(
            self,
            prompt: str,
            model: str,
            temperature: float,
            max_tokens: int,
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Generate response using Ollama"""
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
                logger.info(f"📡 Sending request to Ollama: {self.ollama_url}/api/chat")
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

    async def _ollama_stream(
            self,
            prompt: str,
            model: str,
            temperature: float,
            max_tokens: int,
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> AsyncGenerator[str, None]:
        """Stream response using Ollama"""
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
                url = f"{self.ollama_url}/api/chat"
                logger.info(f"📡 Starting stream to: {url}")
                logger.info(f"📦 Payload: model={model}, temp={temperature}, max_tokens={max_tokens}")

                async with client.stream(
                        "POST",
                        url,
                        json=payload
                ) as response:
                    response.raise_for_status()

                    async for line in response.aiter_lines():
                        if line:
                            try:
                                data = json.loads(line)
                                if "message" in data and "content" in data["message"]:
                                    content = data["message"]["content"]
                                    if content:  # Проверяем что контент не пустой
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

    async def _openai_generate(
            self,
            prompt: str,
            model: str,
            temperature: float,
            max_tokens: int,
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> Dict[str, Any]:
        """Generate response using OpenAI"""
        if not self.openai_api_key:
            raise ValueError("OpenAI API key is not configured")

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

        headers = {
            "Authorization": f"Bearer {self.openai_api_key}",
            "Content-Type": "application/json"
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    "https://api.openai.com/v1/chat/completions",
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
            logger.error(f"OpenAI generation error: {e}")
            raise

    async def _openai_stream(
            self,
            prompt: str,
            model: str,
            temperature: float,
            max_tokens: int,
            conversation_history: Optional[List[Dict[str, str]]] = None
    ) -> AsyncGenerator[str, None]:
        """Stream response using OpenAI"""
        if not self.openai_api_key:
            raise ValueError("OpenAI API key is not configured")

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

        headers = {
            "Authorization": f"Bearer {self.openai_api_key}",
            "Content-Type": "application/json"
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                async with client.stream(
                        "POST",
                        "https://api.openai.com/v1/chat/completions",
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
            logger.error(f"OpenAI streaming error: {e}")
            raise


# Create singleton instance
llm_manager = LLMManager()

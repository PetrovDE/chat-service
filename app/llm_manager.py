"""
LLM Manager - handles interactions with different LLM providers
"""

import httpx
import json
from typing import Optional, List, Dict, AsyncGenerator
from enum import Enum
import logging
from datetime import datetime

from .config import settings

logger = logging.getLogger(__name__)


class ModelSource(Enum):
    """Available model sources"""
    OLLAMA = "ollama"
    OPENAI = "openai"


class LLMResponse:
    """LLM response wrapper"""
    def __init__(self, response: str, model: str, tokens_used: int = 0, generation_time: float = 0):
        self.response = response
        self.model = model
        self.tokens_used = tokens_used
        self.generation_time = generation_time


class LLMManager:
    """Manager for LLM interactions"""

    def __init__(self):
        self.active_source = ModelSource.OLLAMA
        self.ollama_host = settings.OLLAMA_HOST
        self.ollama_model = settings.OLLAMA_MODEL
        self.openai_api_key = settings.OPENAI_API_KEY
        self.openai_model = settings.OPENAI_MODEL

    async def initialize(self):
        """Initialize LLM manager"""
        logger.info(f"Initializing LLM Manager with {self.active_source.value}")

        # Set default source
        if settings.DEFAULT_MODEL_SOURCE == "openai" and self.openai_api_key:
            self.active_source = ModelSource.OPENAI
        else:
            self.active_source = ModelSource.OLLAMA

        # Test connection
        try:
            await self.test_connection()
            logger.info(f"Successfully connected to {self.active_source.value}")
        except Exception as e:
            logger.error(f"Failed to connect to {self.active_source.value}: {e}")

    async def test_connection(self) -> bool:
        """Test connection to active LLM source"""
        if self.active_source == ModelSource.OLLAMA:
            return await self._test_ollama_connection()
        elif self.active_source == ModelSource.OPENAI:
            return await self._test_openai_connection()
        return False

    async def _test_ollama_connection(self) -> bool:
        """Test Ollama connection"""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(f"{self.ollama_host}/api/tags")
                return response.status_code == 200
        except Exception as e:
            logger.error(f"Ollama connection test failed: {e}")
            return False

    async def _test_openai_connection(self) -> bool:
        """Test OpenAI connection"""
        if not self.openai_api_key:
            return False

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    "https://api.openai.com/v1/models",
                    headers={"Authorization": f"Bearer {self.openai_api_key}"}
                )
                return response.status_code == 200
        except Exception as e:
            logger.error(f"OpenAI connection test failed: {e}")
            return False

    def get_current_model_name(self) -> str:
        """Get current model name"""
        if self.active_source == ModelSource.OLLAMA:
            return self.ollama_model
        elif self.active_source == ModelSource.OPENAI:
            return self.openai_model
        return "unknown"

    async def get_response(
        self,
        message: str,
        temperature: float = 0.7,
        max_tokens: int = 2048,
        context: Optional[List[Dict]] = None
    ) -> LLMResponse:
        """Get response from LLM"""
        if self.active_source == ModelSource.OLLAMA:
            return await self._get_ollama_response(message, temperature, max_tokens, context)
        elif self.active_source == ModelSource.OPENAI:
            return await self._get_openai_response(message, temperature, max_tokens, context)
        else:
            raise ValueError(f"Unsupported model source: {self.active_source}")

    async def _get_ollama_response(
        self,
        message: str,
        temperature: float,
        max_tokens: int,
        context: Optional[List[Dict]] = None
    ) -> LLMResponse:
        """Get response from Ollama"""
        start_time = datetime.now()

        messages = []
        if context:
            messages.extend(context)
        messages.append({"role": "user", "content": message})

        payload = {
            "model": self.ollama_model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens
            }
        }

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    f"{self.ollama_host}/api/chat",
                    json=payload
                )
                response.raise_for_status()

                data = response.json()
                generation_time = (datetime.now() - start_time).total_seconds()

                return LLMResponse(
                    response=data["message"]["content"],
                    model=self.ollama_model,
                    tokens_used=data.get("eval_count", 0),
                    generation_time=generation_time
                )

        except Exception as e:
            logger.error(f"Ollama request failed: {e}")
            raise Exception(f"Failed to get response from Ollama: {str(e)}")

    async def _get_openai_response(
        self,
        message: str,
        temperature: float,
        max_tokens: int,
        context: Optional[List[Dict]] = None
    ) -> LLMResponse:
        """Get response from OpenAI"""
        start_time = datetime.now()

        messages = []
        if context:
            messages.extend(context)
        messages.append({"role": "user", "content": message})

        payload = {
            "model": self.openai_model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.openai_api_key}",
                        "Content-Type": "application/json"
                    },
                    json=payload
                )
                response.raise_for_status()

                data = response.json()
                generation_time = (datetime.now() - start_time).total_seconds()

                return LLMResponse(
                    response=data["choices"][0]["message"]["content"],
                    model=self.openai_model,
                    tokens_used=data["usage"]["total_tokens"],
                    generation_time=generation_time
                )

        except Exception as e:
            logger.error(f"OpenAI request failed: {e}")
            raise Exception(f"Failed to get response from OpenAI: {str(e)}")

    async def stream_response(
        self,
        message: str,
        temperature: float = 0.7,
        max_tokens: int = 2048
    ) -> AsyncGenerator[str, None]:
        """Stream response from LLM"""
        if self.active_source == ModelSource.OLLAMA:
            async for chunk in self._stream_ollama_response(message, temperature, max_tokens):
                yield chunk
        else:
            raise NotImplementedError("Streaming not implemented for OpenAI yet")

    async def _stream_ollama_response(
        self,
        message: str,
        temperature: float,
        max_tokens: int
    ) -> AsyncGenerator[str, None]:
        """Stream response from Ollama"""
        payload = {
            "model": self.ollama_model,
            "messages": [{"role": "user", "content": message}],
            "stream": True,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens
            }
        }

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                async with client.stream(
                    "POST",
                    f"{self.ollama_host}/api/chat",
                    json=payload
                ) as response:
                    response.raise_for_status()

                    async for line in response.aiter_lines():
                        if line:
                            try:
                                data = json.loads(line)
                                if "message" in data and "content" in data["message"]:
                                    yield data["message"]["content"]
                            except json.JSONDecodeError:
                                continue

        except Exception as e:
            logger.error(f"Ollama streaming failed: {e}")
            raise Exception(f"Failed to stream from Ollama: {str(e)}")

    async def get_available_models(self) -> List[Dict]:
        """Get list of available models"""
        if self.active_source == ModelSource.OLLAMA:
            return await self._get_ollama_models()
        elif self.active_source == ModelSource.OPENAI:
            return await self._get_openai_models()
        return []

    async def _get_ollama_models(self) -> List[Dict]:
        """Get Ollama models"""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(f"{self.ollama_host}/api/tags")
                response.raise_for_status()

                data = response.json()
                return [
                    {
                        "name": model["name"],
                        "source": "ollama",
                        "size": model.get("size"),
                        "modified": model.get("modified_at")
                    }
                    for model in data.get("models", [])
                ]
        except Exception as e:
            logger.error(f"Failed to get Ollama models: {e}")
            return []

    async def _get_openai_models(self) -> List[Dict]:
        """Get OpenAI models"""
        if not self.openai_api_key:
            return []

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    "https://api.openai.com/v1/models",
                    headers={"Authorization": f"Bearer {self.openai_api_key}"}
                )
                response.raise_for_status()

                data = response.json()
                return [
                    {
                        "name": model["id"],
                        "source": "openai"
                    }
                    for model in data.get("data", [])
                    if "gpt" in model["id"]
                ]
        except Exception as e:
            logger.error(f"Failed to get OpenAI models: {e}")
            return []

    def switch_model(self, source: str, model_name: str):
        """Switch active model"""
        if source == "ollama":
            self.active_source = ModelSource.OLLAMA
            self.ollama_model = model_name
            logger.info(f"Switched to Ollama model: {model_name}")
        elif source == "openai":
            self.active_source = ModelSource.OPENAI
            self.openai_model = model_name
            logger.info(f"Switched to OpenAI model: {model_name}")
        else:
            raise ValueError(f"Unknown source: {source}")

    async def process_uploaded_file(
        self,
        file_content: bytes,
        filename: str,
        file_extension: str
    ) -> Dict:
        """Process uploaded file"""
        # Simple text extraction
        try:
            if file_extension in ['.txt', '.csv', '.json']:
                content = file_content.decode('utf-8')
            else:
                content = f"File: {filename} ({file_extension})"

            preview = content[:500] if len(content) > 500 else content

            return {
                "content": content,
                "preview": preview,
                "success": True
            }
        except Exception as e:
            logger.error(f"File processing failed: {e}")
            return {
                "content": "",
                "preview": f"Failed to process file: {str(e)}",
                "success": False
            }

    async def analyze_file_content(
        self,
        content: str,
        analysis_type: str,
        custom_prompt: Optional[str] = None
    ) -> str:
        """Analyze file content using LLM"""
        prompts = {
            "summarize": f"Please summarize the following content:\n\n{content[:4000]}",
            "extract": f"Extract key information from:\n\n{content[:4000]}",
            "analyze": f"Analyze the following content:\n\n{content[:4000]}"
        }

        prompt = custom_prompt or prompts.get(analysis_type, prompts["summarize"])

        response = await self.get_response(prompt)
        return response.response


# Global LLM manager instance
llm_manager = LLMManager()

"""
Corporate API LLM Service Module
Supports various corporate LLM APIs (OpenAI-compatible, Claude, etc.)
"""

import asyncio
import logging
from typing import List, AsyncGenerator, Dict, Any, Optional
from datetime import datetime
import httpx
import aiohttp
import json
from .models import ChatMessage, ChatRequest, ChatResponse

logger = logging.getLogger(__name__)


class CorporateAPIService:
    """Service for corporate LLM API interactions"""

    def __init__(self):
        self.api_url: Optional[str] = None
        self.api_key: Optional[str] = None
        self.api_type: str = "openai"  # openai, claude, custom
        self.model_name: Optional[str] = None
        self.is_configured = False
        self.timeout = 60  # seconds

    def configure(self, api_url: str, api_key: str, model_name: str, api_type: str = "openai"):
        """Configure API connection"""
        self.api_url = api_url
        self.api_key = api_key
        self.model_name = model_name
        self.api_type = api_type
        self.is_configured = True
        logger.info(f"Configured API service: {api_type} at {api_url}")

    async def test_connection(self) -> bool:
        """Test API connection"""
        if not self.is_configured:
            return False

        try:
            # Simple test request
            test_request = ChatRequest(
                message="Hello",
                temperature=0.1,
                max_tokens=10
            )
            response = await self.generate_response(test_request)
            return bool(response.response)
        except Exception as e:
            logger.error(f"API connection test failed: {e}")
            return False

    def _build_request_body(self, request: ChatRequest) -> Dict[str, Any]:
        """Build request body based on API type"""

        messages = []

        # Add conversation history
        if request.conversation_history:
            for msg in request.conversation_history:
                messages.append({
                    "role": msg.role,
                    "content": msg.content
                })

        # Add current message
        messages.append({
            "role": "user",
            "content": request.message
        })

        if self.api_type == "openai":
            return {
                "model": self.model_name,
                "messages": messages,
                "temperature": request.temperature,
                "max_tokens": request.max_tokens,
                "stream": False
            }
        elif self.api_type == "claude":
            # Claude API format
            return {
                "model": self.model_name,
                "messages": messages,
                "max_tokens": request.max_tokens,
                "temperature": request.temperature
            }
        else:
            # Custom format - assumes OpenAI-like
            return {
                "model": self.model_name,
                "messages": messages,
                "temperature": request.temperature,
                "max_tokens": request.max_tokens
            }

    async def generate_response(self, request: ChatRequest) -> ChatResponse:
        """Generate response from API"""
        if not self.is_configured:
            raise RuntimeError("API service not configured")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        # Add API-specific headers
        if self.api_type == "claude":
            headers["anthropic-version"] = "2023-06-01"

        request_body = self._build_request_body(request)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            try:
                response = await client.post(
                    self.api_url,
                    json=request_body,
                    headers=headers
                )
                response.raise_for_status()

                data = response.json()

                # Extract response based on API type
                if self.api_type == "openai":
                    content = data["choices"][0]["message"]["content"]
                    tokens = data.get("usage", {}).get("total_tokens")
                elif self.api_type == "claude":
                    content = data["content"][0]["text"]
                    tokens = data.get("usage", {}).get("output_tokens")
                else:
                    # Try common patterns
                    content = (data.get("choices", [{}])[0].get("message", {}).get("content") or
                              data.get("response") or
                              data.get("text") or
                              str(data))
                    tokens = None

                return ChatResponse(
                    response=content,
                    model=self.model_name,
                    timestamp=datetime.now(),
                    tokens_used=tokens
                )

            except httpx.HTTPStatusError as e:
                logger.error(f"API request failed: {e.response.status_code} - {e.response.text}")
                raise RuntimeError(f"API request failed: {e}")
            except Exception as e:
                logger.error(f"API request error: {e}")
                raise RuntimeError(f"API error: {e}")

    async def generate_streaming_response(
        self,
        request: ChatRequest
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Generate streaming response from API"""
        if not self.is_configured:
            yield {"type": "error", "content": "API service not configured"}
            return

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        if self.api_type == "claude":
            headers["anthropic-version"] = "2023-06-01"

        request_body = self._build_request_body(request)
        request_body["stream"] = True  # Enable streaming

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.api_url,
                    json=request_body,
                    headers=headers
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        yield {
                            "type": "error",
                            "content": f"API error: {response.status} - {error_text}"
                        }
                        return

                    full_response = ""
                    async for line in response.content:
                        if not line:
                            continue

                        line_text = line.decode('utf-8').strip()
                        if not line_text or not line_text.startswith('data:'):
                            continue

                        if line_text == 'data: [DONE]':
                            yield {
                                "type": "done",
                                "content": full_response,
                                "model": self.model_name,
                                "timestamp": datetime.now().isoformat()
                            }
                            break

                        try:
                            json_str = line_text[6:]  # Remove 'data: ' prefix
                            data = json.loads(json_str)

                            # Extract token based on API type
                            token = None
                            if self.api_type == "openai":
                                delta = data.get("choices", [{}])[0].get("delta", {})
                                token = delta.get("content", "")
                            elif self.api_type == "claude":
                                if data.get("type") == "content_block_delta":
                                    token = data.get("delta", {}).get("text", "")
                            else:
                                # Try common patterns
                                token = (data.get("choices", [{}])[0].get("delta", {}).get("content") or
                                        data.get("token") or
                                        data.get("text", ""))

                            if token:
                                full_response += token
                                yield {
                                    "type": "token",
                                    "content": token,
                                    "timestamp": datetime.now().isoformat()
                                }

                        except json.JSONDecodeError:
                            continue

        except Exception as e:
            logger.error(f"Streaming error: {e}")
            yield {"type": "error", "content": f"Streaming failed: {e}"}

    def get_info(self) -> Dict[str, Any]:
        """Get current API configuration info"""
        return {
            "configured": self.is_configured,
            "api_type": self.api_type,
            "model": self.model_name,
            "api_url": self.api_url if self.api_url else None
        }


# Global instance
api_service = CorporateAPIService()
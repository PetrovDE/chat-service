"""
LLM Manager - Unified interface for multiple LLM sources
Manages both local Ollama and Corporate API models
"""

import logging
from typing import Dict, Any, List, AsyncGenerator, Optional
from enum import Enum
from .llm_service import llm_service
from .api_llm_service import api_service
from .models import ChatRequest, ChatResponse

logger = logging.getLogger(__name__)


class ModelSource(Enum):
    LOCAL = "local"
    API = "api"


class LLMManager:
    """Manages multiple LLM sources and provides unified interface"""

    def __init__(self):
        self.active_source = ModelSource.LOCAL
        self.local_service = llm_service
        self.api_service = api_service

    async def initialize(self):
        """Initialize the manager and available services"""
        logger.info("Initializing LLM Manager...")

        # Try to initialize local service
        try:
            await self.local_service.initialize()
            logger.info("Local Ollama service initialized")
        except Exception as e:
            logger.warning(f"Local service initialization failed: {e}")

    def set_active_source(self, source: str):
        """Switch between local and API sources"""
        if source == "local":
            self.active_source = ModelSource.LOCAL
        elif source == "api":
            self.active_source = ModelSource.API
        else:
            raise ValueError(f"Invalid source: {source}")

        logger.info(f"Switched to {self.active_source.value} source")

    def configure_api(self, api_url: str, api_key: str, model_name: str, api_type: str = "openai"):
        """Configure the API service"""
        self.api_service.configure(api_url, api_key, model_name, api_type)

    async def get_available_models(self) -> Dict[str, Any]:
        """Get available models based on active source"""
        result = {
            "source": self.active_source.value,
            "models": [],
            "current_model": None
        }

        if self.active_source == ModelSource.LOCAL:
            try:
                # Get local Ollama models
                if self.local_service.ollama_client:
                    models_response = self.local_service.ollama_client.list()
                    model_names = []

                    if hasattr(models_response, 'models'):
                        for model in models_response.models:
                            if hasattr(model, 'model'):
                                model_names.append(model.model)

                    result["models"] = model_names
                    result["current_model"] = self.local_service.model_name
            except Exception as e:
                logger.error(f"Error getting local models: {e}")

        elif self.active_source == ModelSource.API:
            # For API, return configured model
            if self.api_service.is_configured:
                result["models"] = [self.api_service.model_name]
                result["current_model"] = self.api_service.model_name

        return result

    async def get_local_models(self) -> Dict[str, Any]:
        """Get detailed info about local models"""
        try:
            if not self.local_service.ollama_client:
                # Try to initialize if not done
                if not self.local_service.is_initialized:
                    await self.local_service.initialize()

                if not self.local_service.ollama_client:
                    return {"models": [], "error": "Ollama not connected"}

            models_response = self.local_service.ollama_client.list()
            model_list = []

            if hasattr(models_response, 'models'):
                for model in models_response.models:
                    model_info = {
                        "name": getattr(model, 'model', 'unknown'),
                        "size": getattr(model, 'size', None),
                        "modified": getattr(model, 'modified_at', None)
                    }
                    model_list.append(model_info)

            return {
                "models": model_list,
                "current": self.local_service.model_name
            }

        except Exception as e:
            logger.error(f"Error getting local models: {e}")
            return {"models": [], "error": str(e)}

    async def switch_local_model(self, model_name: str) -> bool:
        """Switch to a different local model"""
        try:
            # Update the local service model
            self.local_service.model_name = model_name

            # Re-initialize the LangChain LLM with new model
            from langchain_ollama import OllamaLLM
            self.local_service.langchain_llm = OllamaLLM(
                model=model_name,
                temperature=0.7,
                num_predict=1000,
                top_p=0.9,
                top_k=40
            )

            logger.info(f"Switched to local model: {model_name}")
            return True

        except Exception as e:
            logger.error(f"Error switching local model: {e}")
            return False

    async def generate_response(self, request: ChatRequest) -> ChatResponse:
        """Generate response using active source"""
        if self.active_source == ModelSource.LOCAL:
            if not self.local_service.is_initialized:
                await self.local_service.initialize()
            return await self.local_service.generate_response(request)

        elif self.active_source == ModelSource.API:
            if not self.api_service.is_configured:
                raise RuntimeError("API service not configured")
            return await self.api_service.generate_response(request)

    async def generate_streaming_response(
        self,
        request: ChatRequest
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Generate streaming response using active source"""
        if self.active_source == ModelSource.LOCAL:
            if not self.local_service.is_initialized:
                await self.local_service.initialize()
            async for chunk in self.local_service.generate_streaming_response(request):
                yield chunk

        elif self.active_source == ModelSource.API:
            if not self.api_service.is_configured:
                yield {"type": "error", "content": "API service not configured"}
                return
            async for chunk in self.api_service.generate_streaming_response(request):
                yield chunk

    async def process_uploaded_file(
        self,
        file_content: bytes,
        filename: str,
        file_extension: str
    ) -> Dict[str, Any]:
        """Process uploaded file - works with local service"""
        # File processing uses local service as it doesn't require LLM
        return await self.local_service.process_uploaded_file(
            file_content, filename, file_extension
        )

    async def analyze_file_content(
        self,
        content: str,
        analysis_type: str,
        custom_prompt: str = None
    ) -> str:
        """Analyze file content using active source"""
        if self.active_source == ModelSource.LOCAL:
            return await self.local_service.analyze_file_content(
                content, analysis_type, custom_prompt
            )
        else:
            # For API, create a custom request
            prompt = self._build_analysis_prompt(content, analysis_type, custom_prompt)
            request = ChatRequest(
                message=prompt,
                temperature=0.7,
                max_tokens=2000
            )
            response = await self.api_service.generate_response(request)
            return response.response

    def _build_analysis_prompt(
        self,
        content: str,
        analysis_type: str,
        custom_prompt: str = None
    ) -> str:
        """Build analysis prompt for API requests"""
        if analysis_type == "summary":
            return f"Please provide a concise summary of the following content:\n\n{content}"
        elif analysis_type == "extract_data":
            return f"Please extract and list the key data points from:\n\n{content}"
        elif analysis_type == "qa":
            return f"Analyze this content and answer potential questions:\n\n{content}"
        elif analysis_type == "custom" and custom_prompt:
            return f"{custom_prompt}\n\nContent:\n{content}"
        else:
            return f"Please analyze the following:\n\n{content}"

    def get_current_model_name(self) -> str:
        """Get current active model name"""
        if self.active_source == ModelSource.LOCAL:
            return self.local_service.model_name
        else:
            return self.api_service.model_name or "Unknown"

    def get_status(self) -> Dict[str, Any]:
        """Get current manager status"""
        status = {
            "active_source": self.active_source.value,
            "local": {
                "initialized": self.local_service.is_initialized,
                "model": self.local_service.model_name if hasattr(self.local_service, 'model_name') else None
            },
            "api": {
                "configured": self.api_service.is_configured,
                "model": self.api_service.model_name if self.api_service.model_name else None,
                "type": self.api_service.api_type
            }
        }
        return status

    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check"""
        health = {
            "status": "degraded",
            "active_source": self.active_source.value,
            "local_status": "unavailable",
            "api_status": "unconfigured"
        }

        # Check local service
        try:
            if self.local_service.is_initialized:
                local_health = await self.local_service.health_check()
                health["local_status"] = local_health.status
                health["ollama_status"] = local_health.ollama_status
                health["model_available"] = local_health.model_available
        except Exception as e:
            health["local_status"] = f"error: {str(e)}"

        # Check API service
        if self.api_service.is_configured:
            try:
                if await self.api_service.test_connection():
                    health["api_status"] = "healthy"
                else:
                    health["api_status"] = "unhealthy"
            except Exception as e:
                health["api_status"] = f"error: {str(e)}"

        # Determine overall status
        if self.active_source == ModelSource.LOCAL:
            if health["local_status"] == "healthy":
                health["status"] = "healthy"
        elif self.active_source == ModelSource.API:
            if health["api_status"] == "healthy":
                health["status"] = "healthy"

        return health


# Global manager instance
llm_manager = LLMManager()
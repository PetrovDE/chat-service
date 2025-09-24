import asyncio
import logging
from typing import List, AsyncGenerator, Dict, Any
from datetime import datetime
import ollama
from langchain_ollama import OllamaLLM
from langchain.schema import HumanMessage, AIMessage, SystemMessage
import json
import csv
import io
import openpyxl
import pandas as pd
import tempfile
import os
from .models import ChatMessage, ChatRequest, ChatResponse, HealthResponse

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OllamaLLMService:
    """Service for managing Ollama LLM interactions using LangChain"""

    def __init__(self):
        self.model_name = "llama3.1:8b"
        self.ollama_client = None
        self.langchain_llm = None
        self.is_initialized = False

    async def initialize(self) -> bool:
        """Initialize the service and ensure model is available"""
        try:
            logger.info("Initializing Ollama LLM Service...")

            # Initialize Ollama client
            self.ollama_client = ollama.Client()

            # Check if Ollama is running
            await self._check_ollama_connection()

            # Try to ensure model is available (but don't fail if this has issues)
            try:
                await self._ensure_model_available()
            except Exception as e:
                logger.warning(f"Model availability check failed, but continuing: {str(e)}")
                logger.info("Will try to use the model anyway - it might still work")

            # Initialize LangChain LLM
            self.langchain_llm = OllamaLLM(
                model=self.model_name,
                temperature=0.7,
                num_predict=1000,
                top_p=0.9,
                top_k=40
            )

            # Test the LLM with a simple query
            try:
                logger.info("Testing LLM with simple query...")
                test_response = await asyncio.to_thread(
                    self.langchain_llm.invoke,
                    "Hello"
                )
                logger.info(f"LLM test successful. Response: {test_response[:50]}...")
            except ollama.ResponseError as e:
                logger.error(f"LLM test failed: {str(e)}")
                raise RuntimeError(f"LLM is not working: {str(e)}")

            self.is_initialized = True
            logger.info(f"Service initialized successfully with model: {self.model_name}")
            return True

        except (ConnectionError, ollama.ResponseError) as e:
            logger.error(f"Failed to initialize service: {str(e)}")
            self.is_initialized = False
            return False

    async def _check_ollama_connection(self):
        """Check if Ollama is running and accessible"""
        try:
            # Try to list models to check connection
            self.ollama_client.list()
            logger.info("Successfully connected to Ollama")
        except ollama.ResponseError as e:
            raise ConnectionError(f"Cannot connect to Ollama. Is it running? Error: {str(e)}")

    async def _ensure_model_available(self):
        """Ensure Llama 3.1 8b model is available, pull if necessary"""
        try:
            # Get models list
            models_response = self.ollama_client.list()
            logger.info(f"Checking models... (response type: {type(models_response).__name__})")

            # Extract model names from the response
            model_names = []
            for model in models_response.models:
                model_names.append(model.model)
                logger.info(f"Found model: {model.model}")

            if self.model_name not in model_names:
                logger.info(f"Model {self.model_name} not found. Available: {model_names}")
                logger.info(f"Pulling model {self.model_name}...")
                self.ollama_client.pull(self.model_name)
                logger.info(f"Successfully pulled model: {self.model_name}")
            else:
                logger.info(f"Model {self.model_name} is already available")

        except ollama.ResponseError as e:
            logger.error(f"Error ensuring model availability: {str(e)}")
            raise RuntimeError(f"Failed to ensure model availability: {str(e)}")

    def _build_conversation_context(self, request: ChatRequest) -> str:
        """Build conversation context from request"""
        conversation_context = ""

        # Add conversation history if provided
        if request.conversation_history:
            for msg in request.conversation_history:
                role_prefix = "Human" if msg.role == "user" else "Assistant"
                conversation_context += f"{role_prefix}: {msg.content}\n"

        # Add the current message
        conversation_context += f"Human: {request.message}\nAssistant:"
        return conversation_context

    async def generate_response(self, request: ChatRequest) -> ChatResponse:
        """Generate a response using the LLM"""
        if not self.is_initialized:
            await self.initialize()

        if not self.is_initialized:
            raise RuntimeError("Service not properly initialized")

        try:
            # Update LLM parameters for this request
            self.langchain_llm.temperature = request.temperature
            self.langchain_llm.num_predict = request.max_tokens

            # Build the full conversation context
            conversation_context = self._build_conversation_context(request)

            # Generate response
            logger.info(f"Generating response for message: {request.message[:50]}...")
            start_time = datetime.now()

            response_text = await asyncio.to_thread(
                self.langchain_llm.invoke,
                conversation_context
            )

            end_time = datetime.now()
            generation_time = (end_time - start_time).total_seconds()

            logger.info(f"Response generated in {generation_time:.2f} seconds")

            # Create response object
            return ChatResponse(
                response=response_text.strip(),
                model=self.model_name,
                timestamp=end_time,
                tokens_used=None,
                conversation_id=None
            )

        except ollama.ResponseError as e:
            logger.error(f"Error generating response: {str(e)}")
            raise RuntimeError(f"Failed to generate response: {str(e)}")

    async def generate_streaming_response(self, request: ChatRequest) -> AsyncGenerator[Dict[str, Any], None]:
        """Generate a streaming response using the LLM"""
        if not self.is_initialized:
            await self.initialize()

        if not self.is_initialized:
            yield {"type": "error", "content": "Service not properly initialized"}
            return

        try:
            # Build conversation context
            conversation_context = self._build_conversation_context(request)

            logger.info(f"Starting streaming generation for: {request.message[:50]}...")

            # Use Ollama's streaming API directly for real-time response
            stream = self.ollama_client.generate(
                model=self.model_name,
                prompt=conversation_context,
                stream=True,
                options={
                    'temperature': request.temperature,
                    'num_predict': request.max_tokens,
                    'top_p': 0.9,
                    'top_k': 40
                }
            )

            full_response = ""
            for chunk in stream:
                if 'response' in chunk:
                    token = chunk['response']
                    full_response += token
                    yield {
                        "type": "token",
                        "content": token,
                        "timestamp": datetime.now().isoformat()
                    }

                if chunk.get('done', False):
                    yield {
                        "type": "done",
                        "content": full_response,
                        "model": self.model_name,
                        "timestamp": datetime.now().isoformat()
                    }
                    break

        except ollama.ResponseError as e:
            logger.error(f"Error in streaming generation: {str(e)}")
            yield {"type": "error", "content": f"Failed to generate response: {str(e)}"}

    async def process_uploaded_file(self, file_content: bytes, filename: str, file_extension: str) -> Dict[str, Any]:
        """Process uploaded file and return content with analysis suggestions"""
        try:
            content = ""
            file_type = file_extension.lower()

            if file_type == '.txt':
                content = file_content.decode('utf-8')

            elif file_type == '.json':
                content = file_content.decode('utf-8')
                # Pretty format JSON
                try:
                    json_data = json.loads(content)
                    content = json.dumps(json_data, indent=2)
                except json.JSONDecodeError:
                    pass

            elif file_type == '.csv':
                content = file_content.decode('utf-8')
                # Parse CSV for better preview
                try:
                    csv_reader = csv.reader(io.StringIO(content))
                    rows = list(csv_reader)
                    if len(rows) > 0:
                        headers = rows[0]
                        sample_rows = rows[1:6] if len(rows) > 1 else []
                        content = f"CSV Headers: {', '.join(headers)}\n\n"
                        content += "Sample data:\n"
                        for i, row in enumerate(sample_rows, 1):
                            content += f"Row {i}: {', '.join(row)}\n"
                        if len(rows) > 6:
                            content += f"... and {len(rows) - 6} more rows"
                except (csv.Error, UnicodeDecodeError):
                    pass

            elif file_type in ['.xlsx', '.xls']:
                # Process Excel file
                try:
                    # Save to temporary file to read with pandas
                    with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as tmp_file:
                        tmp_file.write(file_content)
                        tmp_file_path = tmp_file.name

                    # Read Excel with pandas for better data handling
                    excel_data = pd.read_excel(tmp_file_path, sheet_name=None)

                    content = f"Excel file with {len(excel_data)} sheet(s):\n\n"

                    for sheet_name, df in excel_data.items():
                        content += f"=== Sheet: {sheet_name} ===\n"
                        content += f"Shape: {df.shape[0]} rows × {df.shape[1]} columns\n"
                        content += f"Columns: {', '.join(df.columns.tolist())}\n"

                        # Add preview of first 5 rows
                        if not df.empty:
                            content += f"\nFirst 5 rows:\n"
                            preview_df = df.head(5)
                            content += preview_df.to_string(max_cols=10)
                            content += "\n"

                        # Basic statistics for numeric columns
                        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                        if numeric_cols:
                            content += f"\nNumeric columns: {', '.join(numeric_cols)}\n"
                            stats = df[numeric_cols].describe()
                            content += f"Basic statistics:\n{stats.to_string()}\n"

                        content += "\n"

                    # Clean up temp file
                    os.unlink(tmp_file_path)

                except Exception as e:
                    logger.error(f"Error processing Excel file: {e}")
                    content = f"[Excel file uploaded - Error processing: {str(e)}]"

            elif file_type == '.pdf':
                content = "[PDF file uploaded - PDF text extraction would require additional libraries like PyPDF2]"

            else:
                content = f"[File type {file_type} uploaded - processing not implemented]"

            # Generate preview (first 500 characters)
            preview = content[:500] + "..." if len(content) > 500 else content

            # Generate analysis suggestions based on file type
            suggestions = self._generate_analysis_suggestions(file_type)

            return {
                "content": content,
                "preview": preview,
                "suggestions": suggestions
            }

        except (UnicodeDecodeError, MemoryError) as e:
            logger.error(f"Error processing file {filename}: {str(e)}")
            raise RuntimeError(f"Failed to process file: {str(e)}")

    @staticmethod
    def _generate_analysis_suggestions(file_type: str) -> List[str]:
        """Generate analysis suggestions based on file type"""
        if file_type == '.txt':
            return [
                "Summarize this document",
                "Extract key points",
                "Identify main themes",
                "Find important names and dates",
                "Analyze sentiment"
            ]
        elif file_type == '.csv':
            return [
                "Analyze this data table",
                "Summarize the dataset",
                "Find patterns in the data",
                "Calculate statistics",
                "Identify trends"
            ]
        elif file_type in ['.xlsx', '.xls']:
            return [
                "Analyze this spreadsheet data",
                "Summarize key findings",
                "Identify trends and patterns",
                "Calculate advanced statistics",
                "Compare data across sheets",
                "Find anomalies or outliers",
                "Generate insights from the data"
            ]
        elif file_type == '.json':
            return [
                "Explain this JSON structure",
                "Summarize the data",
                "Extract specific fields",
                "Validate data format",
                "Find nested information"
            ]
        elif file_type == '.pdf':
            return [
                "Summarize content",
                "Extract key information",
                "Analyze structure",
                "Find specific data"
            ]
        else:
            return [
                "Analyze this content",
                "Provide a summary",
                "Extract key information"
            ]

    async def analyze_file_content(self, content: str, analysis_type: str, custom_prompt: str = None) -> str:
        """Analyze file content using the LLM"""
        if not self.is_initialized:
            await self.initialize()

        if not self.is_initialized:
            raise RuntimeError("Service not properly initialized")

        try:
            # Build analysis prompt based on type
            if analysis_type == "summary":
                prompt = f"Please provide a concise summary of the following content:\n\n{content}"
            elif analysis_type == "extract_data":
                prompt = f"Please extract and list the key data points, numbers, names, and important information from the following content:\n\n{content}"
            elif analysis_type == "qa":
                prompt = f"Please analyze the following content and provide answers to potential questions someone might have about it:\n\n{content}"
            elif analysis_type == "custom" and custom_prompt:
                prompt = f"{custom_prompt}\n\nContent to analyze:\n{content}"
            else:
                prompt = f"Please analyze the following content:\n\n{content}"

            # Generate response
            logger.info(f"Analyzing content with type: {analysis_type}")
            response = await asyncio.to_thread(
                self.langchain_llm.invoke,
                prompt
            )

            return response.strip()

        except ollama.ResponseError as e:
            logger.error(f"Error analyzing file content: {str(e)}")
            raise RuntimeError(f"Failed to analyze content: {str(e)}")

    async def health_check(self) -> HealthResponse:
        """Check the health of the service and Ollama connection"""
        try:
            # Check Ollama connection
            await self._check_ollama_connection()
            ollama_status = "connected"

            # Check if model is available
            models_response = self.ollama_client.list()
            model_available = False

            # Simple check - look through models for our model
            for model in models_response.models:
                if model.model == self.model_name:
                    model_available = True
                    break

            return HealthResponse(
                status="healthy" if (ollama_status == "connected" and model_available) else "degraded",
                ollama_status=ollama_status,
                model_available=model_available
            )

        except (ConnectionError, ollama.ResponseError) as e:
            logger.error(f"Health check failed: {str(e)}")
            return HealthResponse(
                status="unhealthy",
                ollama_status=f"error: {str(e)}",
                model_available=False
            )


# Global service instance (singleton pattern)
llm_service = OllamaLLMService()
from fastapi import FastAPI, HTTPException, status, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import logging
from pathlib import Path
import json
from datetime import datetime
from typing import Dict, Any

from .models import (
    ChatRequest, ChatResponse, HealthResponse,
    FileAnalysisRequest, FileAnalysisResponse
)
from .llm_manager import llm_manager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Manage application lifespan events"""
    # Startup
    logger.info("Starting Llama 3.1 8B Chat Service...")

    try:
        # Initialize the LLM manager
        await llm_manager.initialize()
        logger.info("LLM manager initialized successfully")
    except Exception as e:
        logger.error(f"Error during startup: {str(e)}")

    yield

    # Shutdown
    logger.info("Shutting down Llama 3.1 8B Chat Service...")


# Create FastAPI app with lifespan
app = FastAPI(
    title="Llama 3.1 8B Chat Service",
    description="A service for chatting with Llama 3.1 8B using Ollama and Corporate APIs",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files directory
static_path = Path(__file__).parent / "static"
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")


@app.get("/", response_class=HTMLResponse)
async def read_root():
    """Serve the main HTML interface"""
    html_file = Path(__file__).parent / "static" / "index.html"

    if html_file.exists():
        with open(html_file, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="""
        <html>
            <head><title>Llama Chat Service</title></head>
            <body>
                <h1>Llama Chat Service</h1>
                <p>Service is running! Static HTML file not found.</p>
                <p>Visit <a href="/docs">/docs</a> for API documentation.</p>
            </body>
        </html>
        """)


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """Send a message to the LLM and get a response"""
    try:
        logger.info(f"Received chat request: {request.message[:50]}...")
        response = await llm_manager.generate_response(request)
        logger.info("Successfully generated response")
        return response

    except Exception as e:
        logger.error(f"Error in chat endpoint: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate response: {str(e)}"
        )


@app.post("/chat/stream")
async def chat_stream(request: ChatRequest):
    """Stream a response from the LLM in real-time"""

    async def generate_stream():
        try:
            logger.info(f"Starting streaming response for: {request.message[:50]}...")

            async for chunk in llm_manager.generate_streaming_response(request):
                yield f"data: {json.dumps(chunk)}\n\n"

        except Exception as e:
            logger.error(f"Error in streaming chat: {str(e)}")
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"

    return StreamingResponse(
        generate_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@app.get("/health")
async def health_check():
    """Check the health of all services"""
    try:
        health = await llm_manager.health_check()
        return health
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {
            "status": "error",
            "error": str(e)
        }


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """Upload and process a file for LLM analysis"""
    try:
        logger.info(f"Processing uploaded file: {file.filename}")

        # Check file type
        file_extension = Path(file.filename).suffix.lower()
        supported_types = ['.txt', '.pdf', '.csv', '.xlsx', '.xls', '.json']

        if file_extension not in supported_types:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: {file_extension}. Supported: {supported_types}"
            )

        # Read file content
        file_content = await file.read()

        # Process file
        processed_content = await llm_manager.process_uploaded_file(
            file_content, file.filename, file_extension
        )

        return {
            "filename": file.filename,
            "file_type": file_extension,
            "size": len(file_content),
            "content_preview": processed_content["preview"],
            "full_content": processed_content["content"],
            "analysis_suggestions": processed_content["suggestions"]
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing uploaded file: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process file: {str(e)}"
        )


@app.post("/analyze-file", response_model=FileAnalysisResponse)
async def analyze_file(request: FileAnalysisRequest):
    """Analyze uploaded file content using LLM"""
    try:
        logger.info(f"Analyzing file with type: {request.analysis_type}")

        result = await llm_manager.analyze_file_content(
            content=request.content,
            analysis_type=request.analysis_type,
            custom_prompt=request.custom_prompt
        )

        return FileAnalysisResponse(
            filename=request.filename,
            analysis_type=request.analysis_type,
            result=result,
            model=llm_manager.get_current_model_name(),
            timestamp=datetime.now()
        )

    except Exception as e:
        logger.error(f"Error analyzing file: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to analyze file: {str(e)}"
        )


@app.post("/api/source")
async def set_model_source(request: Dict[str, Any]):
    """Switch between local and API model sources"""
    try:
        source = request.get("source")
        if not source:
            raise HTTPException(status_code=400, detail="Source is required")

        # If switching to API, configure it first
        if source == "api":
            api_config = request.get("api_config")
            if not api_config:
                raise HTTPException(
                    status_code=400,
                    detail="API configuration required for API source"
                )

            llm_manager.configure_api(
                api_url=api_config.get("api_url"),
                api_key=api_config.get("api_key"),
                model_name=api_config.get("model_name"),
                api_type=api_config.get("api_type", "openai")
            )

            # Test API connection
            from .models import ChatRequest
            test_request = ChatRequest(message="Test", temperature=0.1, max_tokens=10)
            try:
                await llm_manager.api_service.generate_response(test_request)
            except Exception as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"API connection failed: {str(e)}"
                )

        # Switch source
        llm_manager.set_active_source(source)

        return {
            "success": True,
            "active_source": source,
            "message": f"Switched to {source} source"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error switching model source: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/source")
async def get_model_source():
    """Get current model source and configuration"""
    try:
        status = llm_manager.get_status()
        models = await llm_manager.get_available_models()

        return {
            "active_source": status["active_source"],
            "models": models["models"],
            "current_model": models["current_model"],
            "local": status["local"],
            "api": status["api"]
        }

    except Exception as e:
        logger.error(f"Error getting model source: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/models/local")
async def get_local_models():
    """Get all locally installed Ollama models"""
    try:
        models = await llm_manager.get_local_models()
        return models

    except Exception as e:
        logger.error(f"Error listing local models: {e}")
        return {"models": [], "error": str(e)}


@app.post("/api/models/local/switch")
async def switch_local_model(request: Dict[str, str]):
    """Switch to a different local Ollama model"""
    try:
        model_name = request.get("model")
        if not model_name:
            raise HTTPException(status_code=400, detail="Model name is required")

        success = await llm_manager.switch_local_model(model_name)

        if success:
            return {
                "success": True,
                "model": model_name,
                "message": f"Switched to {model_name}"
            }
        else:
            raise HTTPException(status_code=400, detail="Failed to switch model")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error switching local model: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/models")
async def list_models():
    """List available models for current source"""
    try:
        return await llm_manager.get_available_models()
    except Exception as e:
        logger.error(f"Error listing models: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    # Run the server
    uvicorn.run(
        "app.main:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
        log_level="info"
    )
"""
Main FastAPI application
"""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import os
from dotenv import load_dotenv

from .database import init_db
from .config import settings

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events"""
    # Startup
    logger.info("Starting up Llama Chat application...")

    # Initialize database
    try:
        await init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")

    # Initialize LLM manager
    try:
        from .llm_manager import llm_manager
        await llm_manager.initialize()
        logger.info("LLM manager initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize LLM manager: {e}")

    yield

    # Shutdown
    logger.info("Shutting down Llama Chat application...")


# Create FastAPI app
app = FastAPI(
    title="Llama Chat API",
    description="Chat interface for Llama models",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import and include routers
from .routers import (
    auth_router,
    chat_router,
    conversations_router,
    files_router,
    models_router,
    stats_router
)

app.include_router(auth_router)
app.include_router(chat_router)
app.include_router(conversations_router)
app.include_router(files_router)
app.include_router(models_router)
app.include_router(stats_router)

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")


# Root endpoint - serve index.html
@app.get("/")
async def root():
    """Serve the main application page"""
    return FileResponse("app/static/index.html")


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    from .llm_manager import llm_manager

    return {
        "status": "healthy",
        "version": "1.0.0",
        "model_info": {
            "active_source": llm_manager.active_source.value,
            "model_name": llm_manager.get_current_model_name(),
            "available_sources": ["ollama", "openai"]
        }
    }


# Info endpoint
@app.get("/info")
async def info():
    """Get application information"""
    return {
        "name": "Llama Chat",
        "version": "1.0.0",
        "description": "Chat interface for Llama models"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
# app/main.py
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import logging

from app.api.v1.router import api_router
from app.core.config import settings
from app.core.logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Server starting...")
    yield
    logger.info("👋 Server stopping...")

app = FastAPI(title="LLaMA Service API", version="1.0.0", lifespan=lifespan)

# Request logging
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"➡️  {request.method} {request.url.path}")
    try:
        response = await call_next(request)
        logger.info(f"⬅️  {request.method} {request.url.path} → {response.status_code}")
        return response
    except Exception as e:
        logger.error(f"❌ {request.method} {request.url.path} → ERROR: {e}")
        raise

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check (БЕЗ /api/v1 префикса!)
@app.get("/health")
async def health_check():
    logger.info("✓ Health check")
    return {"status": "healthy", "timestamp": __import__('time').time()}

# API routes
app.include_router(api_router, prefix="/api/v1")

# Static files
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")

logger.info("✅ Application configured")

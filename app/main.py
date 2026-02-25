# app/main.py
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.v1.router import api_router
from app.core.config import settings
from app.core.logging import setup_logging

# Optional: middleware for X-Request-Id (safe even if missing)
try:
    from app.observability.middleware import RequestIdMiddleware
except Exception:  # if you didn't add the module yet
    RequestIdMiddleware = None

setup_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Server starting")
    yield
    logger.info("Server stopping")


app = FastAPI(title="LLaMA Service API", version="1.0.0", lifespan=lifespan)

# Correlation id middleware (optional, but recommended)
if RequestIdMiddleware is not None:
    app.add_middleware(RequestIdMiddleware)

# Request logging (minimal, no noise)
@app.middleware("http")
async def log_requests(request: Request, call_next):
    try:
        response = await call_next(request)
        logger.info("%s %s -> %s", request.method, request.url.path, response.status_code)
        return response
    except Exception:
        logger.exception("%s %s -> ERROR", request.method, request.url.path)
        raise


# CORS
# your settings.allowed_origins is comma-separated string
origins = ["*"]
try:
    if settings.allowed_origins:
        origins = [o.strip() for o in settings.allowed_origins.split(",") if o.strip()]
except Exception:
    origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check (no /api/v1 prefix)
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": time.time()}

# API routes (as in your original project)
app.include_router(api_router, prefix="/api/v1")

# Static files (as in your original project)
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")

logger.info("Application configured")

# app/main.py
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles

from app.api.v1.router import api_router
from app.core.config import settings
from app.core.error_handlers import register_error_handlers
from app.core.logging import setup_logging
from app.observability.metrics import inc_counter, observe_ms, render_prometheus_metrics
from app.services.file import shutdown_file_processing_worker

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
    await shutdown_file_processing_worker()
    logger.info("Server stopping")


app = FastAPI(title="LLaMA Service API", version="1.0.0", lifespan=lifespan)
register_error_handlers(app)

# Correlation id middleware (optional, but recommended)
if RequestIdMiddleware is not None:
    app.add_middleware(RequestIdMiddleware)

# Request logging (minimal, no noise)
@app.middleware("http")
async def log_requests(request: Request, call_next):
    started = time.perf_counter()
    try:
        response = await call_next(request)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        inc_counter("http_requests_total", method=request.method, path=request.url.path, status=response.status_code)
        observe_ms("http_request_duration_ms", elapsed_ms, method=request.method, path=request.url.path)
        logger.info("%s %s -> %s", request.method, request.url.path, response.status_code)
        return response
    except Exception:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        inc_counter("http_requests_total", method=request.method, path=request.url.path, status="error")
        observe_ms("http_request_duration_ms", elapsed_ms, method=request.method, path=request.url.path)
        logger.exception("%s %s -> ERROR", request.method, request.url.path)
        raise


# CORS
origins = settings.allowed_origins_list

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


@app.get("/metrics", include_in_schema=False)
async def prometheus_metrics():
    return PlainTextResponse(
        render_prometheus_metrics(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )

# API routes (as in your original project)
app.include_router(api_router, prefix="/api/v1")

# Static files (as in your original project)
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")

logger.info("Application configured")

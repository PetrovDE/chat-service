# app/main.py
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy import text
from app.config import settings
from app.llm_manager import llm_manager
from app.database.database import engine
import logging
import time

# Настройка логирования
logging.basicConfig(
    level=settings.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Создание приложения
app = FastAPI(
    title="Llama Chat API",
    description="Modern chat interface for LLM models",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Логирование всех запросов"""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    logger.debug(f"{request.method} {request.url.path} - {response.status_code} - {process_time:.3f}s")
    return response


# Импорт роутеров (только один раз каждый!)
from app.routers import auth, chat, conversations, files, models_management, stats

# Подключение роутеров (БЕЗ ДУБЛИРОВАНИЯ!)
app.include_router(auth.router, prefix="/auth", tags=["Authentication"])
app.include_router(chat.router, prefix="/chat", tags=["Chat"])
app.include_router(conversations.router, prefix="/conversations", tags=["Conversations"])
app.include_router(files.router, prefix="/files", tags=["Files"])
app.include_router(models_management.router, prefix="/models", tags=["Models"])
app.include_router(stats.router, prefix="/stats", tags=["Statistics"])

# Статические файлы (в самом конце!)
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/", include_in_schema=False)
async def read_root():
    """Главная страница"""
    return FileResponse("app/static/index.html")


@app.get("/health", tags=["System"])
async def health_check():
    """Проверка состояния приложения"""
    health_status = {
        "status": "unknown",
        "database": "unknown",
        "llm_available": False,
        "timestamp": time.time()
    }

    # Проверка БД
    try:
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        health_status["database"] = "connected"
    except Exception as e:
        logger.error(f"Database check failed: {e}")
        health_status["database"] = "disconnected"

    # Проверка LLM
    try:
        health_status["llm_available"] = llm_manager.is_available()
    except Exception as e:
        logger.error(f"LLM check failed: {e}")

    # Определить общий статус
    if health_status["database"] == "connected" and health_status["llm_available"]:
        health_status["status"] = "healthy"
    elif health_status["database"] == "connected":
        health_status["status"] = "degraded"
    else:
        health_status["status"] = "unhealthy"

    return health_status


@app.get("/info", tags=["System"])
async def app_info():
    """Информация о приложении"""
    return {
        "name": "Llama Chat",
        "version": "1.0.0",
        "status": "running"
    }


@app.on_event("startup")
async def startup_event():
    """Действия при запуске"""
    logger.info("=" * 70)
    logger.info("🦙 Llama Chat Starting...")
    logger.info("=" * 70)

    try:
        await llm_manager.initialize()
        logger.info("✅ Application started successfully!")
    except Exception as e:
        logger.error(f"Startup error: {e}")

    logger.info("=" * 70)


@app.on_event("shutdown")
async def shutdown_event():
    """Действия при остановке"""
    logger.info("👋 Llama Chat Shutting Down...")
    try:
        await engine.dispose()
    except Exception as e:
        logger.error(f"Shutdown error: {e}")


@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    """404 handler"""
    if request.url.path.startswith(("/auth", "/chat", "/conversations", "/files", "/models", "/stats")):
        return JSONResponse(status_code=404, content={"error": "Not Found"})
    return FileResponse("app/static/index.html")
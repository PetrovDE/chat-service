import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from app.core.config import settings
from app.db.base import Base  # ИСПРАВЛЕНО
# Импортируем модели
import app.db.models  # Это загрузит все модели из __init__.py


async def init_db():
    """Инициализация базы данных"""
    print(f"🔗 Подключение к БД: {settings.DATABASE_URL}")

    engine = create_async_engine(settings.DATABASE_URL, echo=True)

    async with engine.begin() as conn:
        print("🗑️  Удаление старых таблиц...")
        await conn.run_sync(Base.metadata.drop_all)

        print("📦 Создание новых таблиц...")
        await conn.run_sync(Base.metadata.create_all)

    await engine.dispose()
    print("✅ База данных инициализирована успешно!")


if __name__ == "__main__":
    asyncio.run(init_db())

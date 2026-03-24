"""
Deprecated helper for direct metadata create/drop.

For normal schema lifecycle use Alembic:
  alembic revision --autogenerate
  alembic upgrade head
"""

import asyncio

from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import settings
from app.db.base import Base
import app.db.models  # noqa: F401


async def init_db() -> None:
    """Direct drop/create from SQLAlchemy metadata (not Alembic-managed)."""
    print("WARNING: scripts/init_db.py bypasses Alembic revision history.")
    print("Use only for throwaway local experiments.")
    print(f"Connecting to DB: {settings.DATABASE_URL}")

    engine = create_async_engine(settings.DATABASE_URL, echo=True)

    async with engine.begin() as conn:
        print("Dropping existing tables...")
        await conn.run_sync(Base.metadata.drop_all)

        print("Creating tables from metadata...")
        await conn.run_sync(Base.metadata.create_all)

    await engine.dispose()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(init_db())

#!/usr/bin/env python3
# scripts/init_db.py
import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import engine
from app.db.base import Base
from app.db.models import *  # Import all models


async def init_db():
    """Initialize database with all tables"""
    async with engine.begin() as conn:
        # Drop all tables (BE CAREFUL!)
        # await conn.run_sync(Base.metadata.drop_all)
        
        # Create all tables
        await conn.run_sync(Base.metadata.create_all)
        print("✅ Database tables created successfully!")


async def main():
    await init_db()


if __name__ == "__main__":
    asyncio.run(main())

#!/usr/bin/env python3
# scripts/create_admin.py
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import AsyncSessionLocal
from app.crud import crud_user
from app.schemas.user import UserCreate


async def create_admin():
    """Create admin user"""
    async with AsyncSessionLocal() as db:
        # Check if admin exists
        admin = await crud_user.get_by_username(db, username="admin")
        if admin:
            print("❌ Admin already exists")
            return
        
        # Create admin
        admin_data = UserCreate(
            username="admin",
            email="admin@llama-service.local",
            password="admin123456",  # Change this!
            fullname="System Administrator"
        )
        
        admin = await crud_user.create(db, obj_in=admin_data)
        admin.is_admin = True
        await db.commit()
        
        print("✅ Admin user created!")
        print(f"Username: admin")
        print(f"Password: admin123456")
        print("⚠️ Please change the password after first login!")


if __name__ == "__main__":
    asyncio.run(create_admin())

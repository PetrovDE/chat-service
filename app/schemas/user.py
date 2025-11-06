# app/schemas/user.py
import uuid
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, EmailStr, Field


class UserBase(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    fullname: Optional[str] = Field(None, max_length=100)


class UserCreate(UserBase):
    password: str = Field(..., min_length=8, max_length=200)


class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    fullname: Optional[str] = Field(None, max_length=100)


class UserInDB(UserBase):
    id: uuid.UUID
    password_hash: str
    is_active: bool
    is_admin: bool
    created_at: datetime

    class Config:
        from_attributes = True


class UserResponse(UserBase):
    id: uuid.UUID
    is_active: bool
    is_admin: bool
    created_at: datetime

    class Config:
        from_attributes = True

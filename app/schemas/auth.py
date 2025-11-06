# app/schemas/auth.py
from typing import Optional
from pydantic import BaseModel, Field


class UserLogin(BaseModel):
    username: str
    password: str = Field(..., max_length=200)


class PasswordChange(BaseModel):
    old_password: str = Field(..., max_length=200)
    new_password: str = Field(..., min_length=8, max_length=200)


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    username: Optional[str] = None
    user_id: Optional[str] = None

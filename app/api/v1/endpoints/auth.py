# app/api/v1/endpoints/auth.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import timedelta

from app.db.session import get_db
from app.schemas import UserCreate, UserResponse, UserLogin, Token, PasswordChange
from app.api.dependencies import get_current_user
from app.core import security
from app.crud import crud_user
from app.db.models import User

router = APIRouter()


@router.post("/register", response_model=UserResponse)
async def register(
        user_in: UserCreate,
        db: AsyncSession = Depends(get_db)
):
    """Register new user"""
    user = await crud_user.get_by_email(db, email=user_in.email)
    if user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    user = await crud_user.get_by_username(db, username=user_in.username)
    if user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already taken"
        )

    user = await crud_user.create(db, obj_in=user_in)
    return user


@router.post("/login", response_model=Token)
async def login(
        form_data: UserLogin,
        db: AsyncSession = Depends(get_db)
):
    """Login and get access token"""
    user = await crud_user.authenticate(
        db, username=form_data.username, password=form_data.password
    )
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(minutes=security.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = security.create_access_token(
        subject=user.username, expires_delta=access_token_expires
    )
    return Token(access_token=access_token)


@router.get("/me", response_model=UserResponse)
async def read_users_me(current_user: User = Depends(get_current_user)):
    """Get current user info"""
    return current_user


@router.post("/change-password")
async def change_password(
        password_data: PasswordChange,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Change user password"""
    if not security.verify_password(password_data.old_password, current_user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Incorrect password"
        )

    current_user.password_hash = security.get_password_hash(password_data.new_password)
    await db.commit()

    return {"message": "Password updated successfully"}

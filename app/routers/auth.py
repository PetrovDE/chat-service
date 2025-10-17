# app/routers/auth.py
from fastapi import APIRouter, Depends, HTTPException, status, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
from app.database import get_db, crud
from app.database.models import User
from app import models, auth
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

# Security scheme
security = HTTPBearer(auto_error=False)


async def get_current_user(
        credentials: HTTPAuthorizationCredentials = Depends(security),
        db: AsyncSession = Depends(get_db)
) -> User:
    """
    Получить текущего пользователя (обязательная аутентификация)
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    # Декодировать токен
    payload = auth.decode_access_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    username: str = payload.get("sub")
    if username is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Получить пользователя из БД
    user = await crud.get_user_by_username(db, username)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user"
        )

    return user


async def get_current_user_optional(
        credentials: HTTPAuthorizationCredentials = Depends(security),
        db: AsyncSession = Depends(get_db)
) -> Optional[User]:
    """
    Получить текущего пользователя (опциональная аутентификация)
    Возвращает User если токен валидный, иначе None
    """
    if not credentials:
        return None

    try:
        token = credentials.credentials

        # Декодировать токен
        payload = auth.decode_access_token(token)
        if not payload:
            return None

        username: str = payload.get("sub")
        if username is None:
            return None

        # Получить пользователя из БД
        user = await crud.get_user_by_username(db, username)
        if user is None or not user.is_active:
            return None

        return user

    except Exception as e:
        logger.warning(f"Optional auth failed: {e}")
        return None


@router.post("/register", response_model=dict)
async def register(
        user_data: models.UserRegister,
        db: AsyncSession = Depends(get_db)
):
    """Регистрация нового пользователя"""
    try:
        # Проверка существования пользователя
        existing_user = await crud.get_user_by_username(db, user_data.username)
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username already registered"
            )

        existing_email = await crud.get_user_by_email(db, user_data.email)
        if existing_email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )

        # Создание пользователя
        user = await crud.create_user(
            db=db,
            username=user_data.username,
            email=user_data.email,
            password=user_data.password,
            full_name=user_data.full_name
        )

        # Создание токена
        access_token = auth.create_access_token(
            data={"sub": user.username, "user_id": str(user.id)}
        )

        logger.info(f"User registered successfully: {user.username}")

        return {
            "success": True,
            "message": "User registered successfully",
            "access_token": access_token,
            "token_type": "bearer",
            "user": {
                "id": str(user.id),
                "username": user.username,
                "email": user.email,
                "full_name": user.full_name
            }
        }

    except HTTPException:
        raise
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Registration error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Registration failed"
        )


@router.post("/login", response_model=dict)
async def login(
        user_data: models.UserLogin,
        db: AsyncSession = Depends(get_db)
):
    """Вход пользователя"""
    try:
        # Получить пользователя
        user = await crud.get_user_by_username(db, user_data.username)

        if not user or not auth.verify_password(user_data.password, user.hashed_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password"
            )

        if not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User account is disabled"
            )

        # Создание токена
        access_token = auth.create_access_token(
            data={"sub": user.username, "user_id": str(user.id)}
        )

        logger.info(f"User logged in: {user.username}")

        return {
            "success": True,
            "access_token": access_token,
            "token_type": "bearer",
            "user": {
                "id": str(user.id),
                "username": user.username,
                "email": user.email,
                "full_name": user.full_name
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Login failed"
        )


@router.get("/me", response_model=models.UserResponse)
async def get_current_user_info(
        current_user: User = Depends(get_current_user)
):
    """Получить информацию о текущем пользователе"""
    return current_user


@router.put("/me", response_model=models.UserResponse)
async def update_current_user(
        user_update: models.UserUpdate,
        current_user: User = Depends(get_current_user),
        db: AsyncSession = Depends(get_db)
):
    """Обновить профиль текущего пользователя"""
    try:
        updated_user = await crud.update_user(
            db=db,
            user_id=current_user.id,
            email=user_update.email,
            full_name=user_update.full_name
        )

        logger.info(f"User profile updated: {current_user.username}")
        return updated_user

    except Exception as e:
        logger.error(f"Profile update error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Profile update failed"
        )


@router.post("/change-password")
async def change_password(
        password_data: models.PasswordChange,
        current_user: User = Depends(get_current_user),
        db: AsyncSession = Depends(get_db)
):
    """Изменить пароль текущего пользователя"""
    try:
        # Проверить старый пароль
        if not auth.verify_password(password_data.old_password, current_user.hashed_password):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Incorrect old password"
            )

        # Проверить длину нового пароля
        new_password = password_data.new_password
        if len(new_password.encode('utf-8')) > 72:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="New password is too long (maximum 72 bytes)"
            )

        # Изменить пароль
        await crud.update_user_password(
            db=db,
            user_id=current_user.id,
            new_password=new_password
        )

        logger.info(f"Password changed for user: {current_user.username}")

        return {"success": True, "message": "Password changed successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Password change error: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Password change failed"
        )


@router.post("/logout")
async def logout(
        current_user: User = Depends(get_current_user)
):
    """
    Выход из системы
    Примечание: JWT токены без состояния, поэтому фактический выход происходит на клиенте
    """
    logger.info(f"User logged out: {current_user.username}")
    return {"success": True, "message": "Logged out successfully"}
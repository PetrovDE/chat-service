# app/api/dependencies.py
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from app.db.session import get_db
from app.db.models import User
from app.core import security
from app.crud.user import crud_user

# Security scheme
security_bearer = HTTPBearer(auto_error=False)


async def get_current_user(
        credentials: HTTPAuthorizationCredentials = Depends(security_bearer),
        db: AsyncSession = Depends(get_db)
) -> User:
    """
    Get current authenticated user (required authentication)
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    # Decode token
    payload = security.decode_access_token(token)
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

    # Get user from database
    user = await crud_user.get_by_username(db, username=username)
    if user is None:
        # ИСПРАВЛЕНО: 401 вместо 404 для консистентности
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
        credentials: HTTPAuthorizationCredentials = Depends(security_bearer),
        db: AsyncSession = Depends(get_db)
) -> Optional[User]:
    """
    Get current user if authenticated (optional authentication)
    """
    if not credentials:
        return None

    try:
        return await get_current_user(credentials=credentials, db=db)
    except HTTPException:
        return None


async def get_current_admin_user(
        current_user: User = Depends(get_current_user)
) -> User:
    """
    Get current admin user
    """
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough permissions"
        )

    return current_user

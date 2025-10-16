"""
Authentication router
"""

from fastapi import APIRouter, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
import logging
import re

from ..models import UserRegister, UserLogin, TokenResponse, UserResponse, UserUpdate, PasswordChange
from ..database import get_db, crud
from ..auth import (
    get_password_hash,
    create_access_token,
    verify_password,
    get_current_user,
    get_current_active_user
)
from ..database.models import User

router = APIRouter(prefix="/auth", tags=["authentication"])
logger = logging.getLogger(__name__)


def validate_email(email: str) -> bool:
    """Validate email format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def validate_username(username: str) -> bool:
    """Validate username (alphanumeric, underscore, dash)"""
    pattern = r'^[a-zA-Z0-9_-]{3,50}$'
    return re.match(pattern, username) is not None


@router.post("/register", response_model=TokenResponse, status_code=status.HTTP_201_CREATED)
async def register(
    user_data: UserRegister,
    db: AsyncSession = Depends(get_db)
):
    """Register a new user"""
    try:
        # Validate username
        if not validate_username(user_data.username):
            raise HTTPException(
                status_code=400,
                detail="Username must be 3-50 characters and contain only letters, numbers, underscore, or dash"
            )

        # Validate email
        if not validate_email(user_data.email):
            raise HTTPException(
                status_code=400,
                detail="Invalid email format"
            )

        # Check if username already exists
        existing_user = await crud.get_user_by_username(db, user_data.username)
        if existing_user:
            raise HTTPException(
                status_code=400,
                detail="Username already registered"
            )

        # Check if email already exists
        existing_email = await crud.get_user_by_email(db, user_data.email)
        if existing_email:
            raise HTTPException(
                status_code=400,
                detail="Email already registered"
            )

        # Hash password
        hashed_password = get_password_hash(user_data.password)

        # Create user
        user = await crud.create_user(
            db=db,
            username=user_data.username,
            email=user_data.email,
            hashed_password=hashed_password,
            full_name=user_data.full_name
        )

        # Create access token
        access_token = create_access_token(data={"sub": str(user.id)})

        logger.info(f"New user registered: {user.username}")

        return TokenResponse(
            access_token=access_token,
            token_type="bearer",
            user=UserResponse(
                id=str(user.id),
                username=user.username,
                email=user.email,
                full_name=user.full_name,
                is_active=user.is_active,
                is_admin=user.is_admin,
                created_at=user.created_at
            )
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Registration error: {e}")
        raise HTTPException(
            status_code=500,
            detail="Registration failed"
        )


@router.post("/login", response_model=TokenResponse)
async def login(
    credentials: UserLogin,
    db: AsyncSession = Depends(get_db)
):
    """Login user and return JWT token"""
    try:
        # Authenticate user
        user = await crud.authenticate_user(
            db=db,
            username=credentials.username,
            password=credentials.password
        )

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Create access token
        access_token = create_access_token(data={"sub": str(user.id)})

        logger.info(f"User logged in: {user.username}")

        return TokenResponse(
            access_token=access_token,
            token_type="bearer",
            user=UserResponse(
                id=str(user.id),
                username=user.username,
                email=user.email,
                full_name=user.full_name,
                is_active=user.is_active,
                is_admin=user.is_admin,
                created_at=user.created_at
            )
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login error: {e}")
        raise HTTPException(
            status_code=500,
            detail="Login failed"
        )


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: User = Depends(get_current_active_user)
):
    """Get current authenticated user information"""
    return UserResponse(
        id=str(current_user.id),
        username=current_user.username,
        email=current_user.email,
        full_name=current_user.full_name,
        is_active=current_user.is_active,
        is_admin=current_user.is_admin,
        created_at=current_user.created_at
    )


@router.put("/me", response_model=UserResponse)
async def update_current_user(
    user_update: UserUpdate,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """Update current user information"""
    try:
        # Validate username if provided
        if user_update.username and not validate_username(user_update.username):
            raise HTTPException(
                status_code=400,
                detail="Username must be 3-50 characters and contain only letters, numbers, underscore, or dash"
            )

        # Validate email if provided
        if user_update.email and not validate_email(user_update.email):
            raise HTTPException(
                status_code=400,
                detail="Invalid email format"
            )

        # Check if new username is taken
        if user_update.username and user_update.username != current_user.username:
            existing = await crud.get_user_by_username(db, user_update.username)
            if existing:
                raise HTTPException(status_code=400, detail="Username already taken")

        # Check if new email is taken
        if user_update.email and user_update.email != current_user.email:
            existing = await crud.get_user_by_email(db, user_update.email)
            if existing:
                raise HTTPException(status_code=400, detail="Email already taken")

        # Update user
        updated_user = await crud.update_user(
            db=db,
            user_id=current_user.id,
            username=user_update.username,
            email=user_update.email,
            full_name=user_update.full_name
        )

        if not updated_user:
            raise HTTPException(status_code=404, detail="User not found")

        logger.info(f"User updated: {updated_user.username}")

        return UserResponse(
            id=str(updated_user.id),
            username=updated_user.username,
            email=updated_user.email,
            full_name=updated_user.full_name,
            is_active=updated_user.is_active,
            is_admin=updated_user.is_admin,
            created_at=updated_user.created_at
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"User update error: {e}")
        raise HTTPException(status_code=500, detail="Update failed")


@router.post("/change-password")
async def change_password(
    password_data: PasswordChange,
    current_user: User = Depends(get_current_active_user),
    db: AsyncSession = Depends(get_db)
):
    """Change user password"""
    try:
        # Verify current password
        if not verify_password(password_data.current_password, current_user.hashed_password):
            raise HTTPException(
                status_code=400,
                detail="Current password is incorrect"
            )

        # Hash new password
        new_hashed_password = get_password_hash(password_data.new_password)

        # Update password
        await crud.update_user_password(
            db=db,
            user_id=current_user.id,
            hashed_password=new_hashed_password
        )

        logger.info(f"Password changed for user: {current_user.username}")

        return {"message": "Password changed successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Password change error: {e}")
        raise HTTPException(status_code=500, detail="Password change failed")


@router.post("/logout")
async def logout(current_user: User = Depends(get_current_active_user)):
    """Logout user (client should delete token)"""
    logger.info(f"User logged out: {current_user.username}")
    return {"message": "Logged out successfully"}
"""
CRUD operations for database models
"""

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, update, func, desc
from typing import List, Optional, Dict
from datetime import datetime, timedelta
import uuid
import logging

from .models import User, Conversation, Message, File, SystemSetting, APIUsageLog

logger = logging.getLogger(__name__)


# ==================== USER CRUD ====================

async def get_user(db: AsyncSession, user_id: uuid.UUID) -> Optional[User]:
    """Get user by ID"""
    return await db.get(User, user_id)


async def get_user_by_email(db: AsyncSession, email: str) -> Optional[User]:
    """Get user by email"""
    result = await db.execute(
        select(User).where(User.email == email)
    )
    return result.scalar_one_or_none()


async def get_user_by_username(db: AsyncSession, username: str) -> Optional[User]:
    """Get user by username"""
    result = await db.execute(
        select(User).where(User.username == username)
    )
    return result.scalar_one_or_none()


async def create_user(
        db: AsyncSession,
        username: str,
        email: str,
        hashed_password: str,
        full_name: Optional[str] = None
) -> User:
    """Create a new user"""
    user = User(
        username=username,
        email=email,
        hashed_password=hashed_password,
        full_name=full_name,
        is_active=True,
        is_admin=False
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    logger.info(f"Created user {user.username} ({user.id})")
    return user


async def update_user(
        db: AsyncSession,
        user_id: uuid.UUID,
        username: Optional[str] = None,
        email: Optional[str] = None,
        full_name: Optional[str] = None,
        is_active: Optional[bool] = None
) -> Optional[User]:
    """Update user information"""
    user = await get_user(db, user_id)
    if not user:
        return None

    if username is not None:
        user.username = username
    if email is not None:
        user.email = email
    if full_name is not None:
        user.full_name = full_name
    if is_active is not None:
        user.is_active = is_active

    user.updated_at = datetime.utcnow()

    await db.commit()
    await db.refresh(user)
    logger.info(f"Updated user {user_id}")
    return user


async def update_user_password(
        db: AsyncSession,
        user_id: uuid.UUID,
        hashed_password: str
) -> Optional[User]:
    """Update user password"""
    user = await get_user(db, user_id)
    if not user:
        return None

    user.hashed_password = hashed_password
    user.updated_at = datetime.utcnow()

    await db.commit()
    await db.refresh(user)
    logger.info(f"Updated password for user {user_id}")
    return user


async def delete_user(db: AsyncSession, user_id: uuid.UUID) -> bool:
    """Delete user (soft delete - set inactive)"""
    user = await get_user(db, user_id)
    if not user:
        return False

    user.is_active = False
    user.updated_at = datetime.utcnow()

    await db.commit()
    logger.info(f"Deactivated user {user_id}")
    return True


async def authenticate_user(
        db: AsyncSession,
        username: str,
        password: str
) -> Optional[User]:
    """Authenticate user by username and password"""
    from ..auth import verify_password

    user = await get_user_by_username(db, username)
    if not user:
        return None

    if not verify_password(password, user.hashed_password):
        return None

    if not user.is_active:
        return None

    return user


# ==================== CONVERSATION CRUD ====================

async def create_conversation(
        db: AsyncSession,
        user_id: Optional[uuid.UUID],
        model_source: str,
        model_name: str,
        title: Optional[str] = None
) -> Conversation:
    """Create a new conversation"""
    conversation = Conversation(
        user_id=user_id,
        model_source=model_source,
        model_name=model_name,
        title=title or f"Conversation {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}",
        message_count=0
    )
    db.add(conversation)
    await db.commit()
    await db.refresh(conversation)
    logger.info(f"Created conversation {conversation.id} for user {user_id}")
    return conversation


async def get_conversation(db: AsyncSession, conversation_id: uuid.UUID) -> Optional[Conversation]:
    """Get a conversation by ID"""
    return await db.get(Conversation, conversation_id)


async def get_user_conversations(
        db: AsyncSession,
        user_id: Optional[uuid.UUID],
        skip: int = 0,
        limit: int = 50,
        include_archived: bool = False
) -> List[Conversation]:
    """Get all conversations for a user"""
    query = select(Conversation)

    # Filter by user
    if user_id is not None:
        query = query.where(Conversation.user_id == user_id)
    else:
        query = query.where(Conversation.user_id.is_(None))

    if not include_archived:
        query = query.where(Conversation.is_archived == False)

    query = query.order_by(desc(Conversation.updated_at)).offset(skip).limit(limit)

    result = await db.execute(query)
    return result.scalars().all()


async def update_conversation(
        db: AsyncSession,
        conversation_id: uuid.UUID,
        title: Optional[str] = None,
        is_archived: Optional[bool] = None
) -> Optional[Conversation]:
    """Update conversation"""
    conversation = await get_conversation(db, conversation_id)
    if not conversation:
        return None

    if title is not None:
        conversation.title = title
    if is_archived is not None:
        conversation.is_archived = is_archived

    conversation.updated_at = datetime.utcnow()

    await db.commit()
    await db.refresh(conversation)
    logger.info(f"Updated conversation {conversation_id}")
    return conversation


async def delete_conversation(db: AsyncSession, conversation_id: uuid.UUID) -> bool:
    """Delete a conversation and all its messages"""
    conversation = await get_conversation(db, conversation_id)
    if not conversation:
        return False

    await db.delete(conversation)
    await db.commit()
    logger.info(f"Deleted conversation {conversation_id}")
    return True


# ==================== MESSAGE CRUD ====================

async def create_message(
        db: AsyncSession,
        conversation_id: uuid.UUID,
        role: str,
        content: str,
        model_name: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        tokens_used: Optional[int] = None,
        generation_time: Optional[float] = None,
        embedding: Optional[List[float]] = None,
        extra_metadata: Optional[Dict] = None
) -> Message:
    """Create a new message"""
    message = Message(
        conversation_id=conversation_id,
        role=role,
        content=content,
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
        tokens_used=tokens_used,
        generation_time=generation_time,
        embedding=embedding,
        extra_metadata=extra_metadata
    )
    db.add(message)

    # Update conversation message count and updated_at
    conversation = await get_conversation(db, conversation_id)
    if conversation:
        conversation.message_count = conversation.message_count + 1
        conversation.updated_at = datetime.utcnow()

    await db.commit()
    await db.refresh(message)
    logger.info(f"Created message in conversation {conversation_id}")
    return message


async def get_conversation_messages(
        db: AsyncSession,
        conversation_id: uuid.UUID,
        skip: int = 0,
        limit: int = 100
) -> List[Message]:
    """Get all messages in a conversation"""
    query = (
        select(Message)
        .where(Message.conversation_id == conversation_id)
        .order_by(Message.created_at)
        .offset(skip)
        .limit(limit)
    )
    result = await db.execute(query)
    return result.scalars().all()


# ==================== FILE CRUD ====================

async def create_file_record(
        db: AsyncSession,
        user_id: Optional[uuid.UUID],
        filename: str,
        original_filename: str,
        file_path: str,
        file_type: str,
        file_size: int,
        content_preview: Optional[str] = None,
        full_content: Optional[str] = None
) -> File:
    """Create a file record"""
    file_record = File(
        user_id=user_id,
        filename=filename,
        original_filename=original_filename,
        file_path=file_path,
        file_type=file_type,
        file_size=file_size,
        content_preview=content_preview,
        full_content=full_content
    )
    db.add(file_record)
    await db.commit()
    await db.refresh(file_record)
    logger.info(f"Created file record {file_record.id}")
    return file_record


# ==================== SYSTEM SETTINGS CRUD ====================

async def get_setting(db: AsyncSession, key: str) -> Optional[SystemSetting]:
    """Get a system setting by key"""
    result = await db.execute(
        select(SystemSetting).where(SystemSetting.key == key)
    )
    return result.scalar_one_or_none()


async def set_setting(
        db: AsyncSession,
        key: str,
        value: str,
        description: Optional[str] = None
) -> SystemSetting:
    """Set or update a system setting"""
    setting = await get_setting(db, key)

    if setting:
        setting.value = value
        if description:
            setting.description = description
        setting.updated_at = datetime.utcnow()
    else:
        setting = SystemSetting(
            key=key,
            value=value,
            description=description
        )
        db.add(setting)

    await db.commit()
    await db.refresh(setting)
    logger.info(f"Set setting {key}")
    return setting


# ==================== API USAGE LOG CRUD ====================

async def log_api_usage(
        db: AsyncSession,
        user_id: Optional[uuid.UUID],
        conversation_id: Optional[uuid.UUID],
        model_source: str,
        model_name: str,
        endpoint: str,
        tokens_prompt: Optional[int] = None,
        tokens_completion: Optional[int] = None,
        tokens_total: Optional[int] = None,
        response_time: Optional[float] = None,
        status: str = "success",
        error_message: Optional[str] = None,
        extra_metadata: Optional[Dict] = None
) -> APIUsageLog:
    """Log API usage"""
    log_entry = APIUsageLog(
        user_id=user_id,
        conversation_id=conversation_id,
        model_source=model_source,
        model_name=model_name,
        endpoint=endpoint,
        tokens_prompt=tokens_prompt,
        tokens_completion=tokens_completion,
        tokens_total=tokens_total,
        response_time=response_time,
        status=status,
        error_message=error_message,
        extra_metadata=extra_metadata
    )
    db.add(log_entry)
    await db.commit()
    return log_entry


async def get_usage_stats(
        db: AsyncSession,
        user_id: Optional[uuid.UUID],
        start_date: datetime,
        end_date: datetime
) -> Dict:
    """Get usage statistics"""
    query = select(APIUsageLog).where(
        APIUsageLog.created_at >= start_date,
        APIUsageLog.created_at <= end_date
    )

    if user_id:
        query = query.where(APIUsageLog.user_id == user_id)

    result = await db.execute(query)
    logs = result.scalars().all()

    total_requests = len(logs)
    total_tokens = sum(log.tokens_total or 0 for log in logs)
    avg_response_time = sum(log.response_time or 0 for log in logs) / total_requests if total_requests > 0 else 0

    return {
        "total_requests": total_requests,
        "total_tokens": total_tokens,
        "average_response_time": avg_response_time,
        "successful_requests": sum(1 for log in logs if log.status == "success"),
        "failed_requests": sum(1 for log in logs if log.status == "error")
    }

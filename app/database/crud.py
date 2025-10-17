# app/database/crud.py
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, delete, func
from sqlalchemy.orm import selectinload
from app.database.models import User, Conversation, Message, File, SystemSetting, APIUsageLog
from app.auth import get_password_hash
from typing import Optional, List
import uuid
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# USER CRUD
# ============================================================================

async def get_user_by_username(db: AsyncSession, username: str) -> Optional[User]:
    """Получить пользователя по имени"""
    result = await db.execute(
        select(User).where(User.username == username)
    )
    return result.scalar_one_or_none()


async def get_user_by_email(db: AsyncSession, email: str) -> Optional[User]:
    """Получить пользователя по email"""
    result = await db.execute(
        select(User).where(User.email == email)
    )
    return result.scalar_one_or_none()


async def get_user_by_id(db: AsyncSession, user_id: uuid.UUID) -> Optional[User]:
    """Получить пользователя по ID"""
    result = await db.execute(
        select(User).where(User.id == user_id)
    )
    return result.scalar_one_or_none()


async def create_user(
    db: AsyncSession,
    username: str,
    email: str,
    password: str,  # Принимаем plain password
    full_name: Optional[str] = None
) -> User:
    """Создать нового пользователя"""
    # Хешировать пароль
    hashed_password = get_password_hash(password)
    
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
    
    logger.info(f"Created user: {username} (ID: {user.id})")
    return user


async def update_user(
    db: AsyncSession,
    user_id: uuid.UUID,
    email: Optional[str] = None,
    full_name: Optional[str] = None
) -> Optional[User]:
    """Обновить данные пользователя"""
    user = await get_user_by_id(db, user_id)
    if not user:
        return None
    
    if email is not None:
        user.email = email
    if full_name is not None:
        user.full_name = full_name
    
    user.updated_at = datetime.utcnow()
    
    await db.commit()
    await db.refresh(user)
    
    logger.info(f"Updated user: {user.username}")
    return user


async def update_user_password(
    db: AsyncSession,
    user_id: uuid.UUID,
    new_password: str
) -> Optional[User]:
    """Обновить пароль пользователя"""
    user = await get_user_by_id(db, user_id)
    if not user:
        return None
    
    user.hashed_password = get_password_hash(new_password)
    user.updated_at = datetime.utcnow()
    
    await db.commit()
    await db.refresh(user)
    
    logger.info(f"Updated password for user: {user.username}")
    return user


# ============================================================================
# CONVERSATION CRUD
# ============================================================================

async def get_user_conversations(
    db: AsyncSession,
    user_id: Optional[uuid.UUID],
    include_archived: bool = False
) -> List[Conversation]:
    """Получить беседы пользователя"""
    query = select(Conversation).where(Conversation.user_id == user_id)
    
    if not include_archived:
        query = query.where(Conversation.is_archived == False)
    
    query = query.order_by(Conversation.updated_at.desc())
    
    result = await db.execute(query)
    conversations = result.scalars().all()
    
    return list(conversations)


async def get_conversation(
    db: AsyncSession,
    conversation_id: uuid.UUID
) -> Optional[Conversation]:
    """Получить беседу по ID"""
    result = await db.execute(
        select(Conversation).where(Conversation.id == conversation_id)
    )
    return result.scalar_one_or_none()


async def create_conversation(
    db: AsyncSession,
    user_id: Optional[uuid.UUID],
    title: str,
    model_source: str = "ollama",
    model_name: str = "llama3.1:8b"
) -> Conversation:
    """Создать новую беседу"""
    conversation = Conversation(
        user_id=user_id,
        title=title,
        model_source=model_source,
        model_name=model_name,
        is_archived=False,
        message_count=0
    )
    
    db.add(conversation)
    await db.commit()
    await db.refresh(conversation)
    
    logger.info(f"Created conversation {conversation.id} for user {user_id}")
    return conversation


async def update_conversation(
    db: AsyncSession,
    conversation_id: uuid.UUID,
    title: Optional[str] = None,
    is_archived: Optional[bool] = None
) -> Optional[Conversation]:
    """Обновить беседу"""
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


async def delete_conversation(
    db: AsyncSession,
    conversation_id: uuid.UUID
) -> bool:
    """Удалить беседу"""
    conversation = await get_conversation(db, conversation_id)
    if not conversation:
        return False
    
    await db.delete(conversation)
    await db.commit()
    
    logger.info(f"Deleted conversation {conversation_id}")
    return True


async def increment_conversation_message_count(
    db: AsyncSession,
    conversation_id: uuid.UUID
):
    """Увеличить счётчик сообщений в беседе"""
    await db.execute(
        update(Conversation)
        .where(Conversation.id == conversation_id)
        .values(
            message_count=Conversation.message_count + 1,
            updated_at=datetime.utcnow()
        )
    )
    await db.commit()


# ============================================================================
# MESSAGE CRUD
# ============================================================================

async def get_conversation_messages(
    db: AsyncSession,
    conversation_id: uuid.UUID,
    limit: Optional[int] = None
) -> List[Message]:
    """Получить сообщения беседы"""
    query = select(Message).where(
        Message.conversation_id == conversation_id
    ).order_by(Message.created_at.asc())
    
    if limit:
        query = query.limit(limit)
    
    result = await db.execute(query)
    messages = result.scalars().all()
    
    return list(messages)


async def create_message(
    db: AsyncSession,
    conversation_id: uuid.UUID,
    role: str,
    content: str,
    model_name: Optional[str] = None,
    temperature: Optional[float] = None,
    max_tokens: Optional[int] = None,
    tokens_used: Optional[int] = None,
    generation_time: Optional[float] = None
) -> Message:
    """Создать сообщение"""
    message = Message(
        conversation_id=conversation_id,
        role=role,
        content=content,
        model_name=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
        tokens_used=tokens_used,
        generation_time=generation_time
    )
    
    db.add(message)
    
    # Увеличить счётчик сообщений
    await increment_conversation_message_count(db, conversation_id)
    
    await db.commit()
    await db.refresh(message)
    
    logger.info(f"Created message in conversation {conversation_id}")
    return message


# ============================================================================
# FILE CRUD
# ============================================================================

async def create_file(
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
    """Создать запись о файле"""
    file = File(
        user_id=user_id,
        filename=filename,
        original_filename=original_filename,
        file_path=file_path,
        file_type=file_type,
        file_size=file_size,
        content_preview=content_preview,
        full_content=full_content
    )
    
    db.add(file)
    await db.commit()
    await db.refresh(file)
    
    logger.info(f"Created file record: {filename}")
    return file


async def get_file(
    db: AsyncSession,
    file_id: uuid.UUID
) -> Optional[File]:
    """Получить файл по ID"""
    result = await db.execute(
        select(File).where(File.id == file_id)
    )
    return result.scalar_one_or_none()


async def get_user_files(
    db: AsyncSession,
    user_id: Optional[uuid.UUID]
) -> List[File]:
    """Получить файлы пользователя"""
    result = await db.execute(
        select(File)
        .where(File.user_id == user_id)
        .order_by(File.created_at.desc())
    )
    return list(result.scalars().all())


async def delete_file(
    db: AsyncSession,
    file_id: uuid.UUID
) -> bool:
    """Удалить файл"""
    file = await get_file(db, file_id)
    if not file:
        return False
    
    await db.delete(file)
    await db.commit()
    
    logger.info(f"Deleted file {file_id}")
    return True


# ============================================================================
# SYSTEM SETTINGS CRUD
# ============================================================================

async def get_setting(
    db: AsyncSession,
    key: str
) -> Optional[SystemSetting]:
    """Получить настройку"""
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
    """Установить настройку"""
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
    
    return setting


# ============================================================================
# API USAGE LOG CRUD
# ============================================================================

async def create_api_usage_log(
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
    error_message: Optional[str] = None
) -> APIUsageLog:
    """Создать лог использования API"""
    log = APIUsageLog(
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
        error_message=error_message
    )
    
    db.add(log)
    await db.commit()
    await db.refresh(log)
    
    return log


async def get_api_usage_logs(
    db: AsyncSession,
    user_id: Optional[uuid.UUID] = None,
    limit: int = 100
) -> List[APIUsageLog]:
    """Получить логи использования API"""
    query = select(APIUsageLog)
    
    if user_id:
        query = query.where(APIUsageLog.user_id == user_id)
    
    query = query.order_by(APIUsageLog.created_at.desc()).limit(limit)
    
    result = await db.execute(query)
    return list(result.scalars().all())


# ============================================================================
# STATISTICS
# ============================================================================

async def get_user_stats(
    db: AsyncSession,
    user_id: uuid.UUID
) -> dict:
    """Получить статистику пользователя"""
    # Количество бесед
    conv_result = await db.execute(
        select(func.count(Conversation.id)).where(Conversation.user_id == user_id)
    )
    conversation_count = conv_result.scalar() or 0
    
    # Количество сообщений
    msg_result = await db.execute(
        select(func.count(Message.id))
        .join(Conversation)
        .where(Conversation.user_id == user_id)
    )
    message_count = msg_result.scalar() or 0
    
    # Общее количество токенов
    tokens_result = await db.execute(
        select(func.sum(Message.tokens_used))
        .join(Conversation)
        .where(Conversation.user_id == user_id)
    )
    total_tokens = tokens_result.scalar() or 0
    
    # Среднее время генерации
    time_result = await db.execute(
        select(func.avg(Message.generation_time))
        .join(Conversation)
        .where(Conversation.user_id == user_id)
        .where(Message.generation_time.isnot(None))
    )
    avg_time = time_result.scalar() or 0
    
    return {
        "conversation_count": conversation_count,
        "message_count": message_count,
        "total_tokens": int(total_tokens),
        "average_response_time": float(avg_time) if avg_time else None
    }


async def get_global_stats(db: AsyncSession) -> dict:
    """Получить глобальную статистику"""
    # Общее количество пользователей
    users_result = await db.execute(select(func.count(User.id)))
    user_count = users_result.scalar() or 0
    
    # Общее количество бесед
    conv_result = await db.execute(select(func.count(Conversation.id)))
    conversation_count = conv_result.scalar() or 0
    
    # Общее количество сообщений
    msg_result = await db.execute(select(func.count(Message.id)))
    message_count = msg_result.scalar() or 0
    
    # Общее количество токенов
    tokens_result = await db.execute(select(func.sum(Message.tokens_used)))
    total_tokens = tokens_result.scalar() or 0
    
    # Среднее время генерации
    time_result = await db.execute(
        select(func.avg(Message.generation_time))
        .where(Message.generation_time.isnot(None))
    )
    avg_time = time_result.scalar() or 0
    
    return {
        "user_count": user_count,
        "conversation_count": conversation_count,
        "message_count": message_count,
        "total_tokens": int(total_tokens),
        "average_response_time": float(avg_time) if avg_time else None
    }
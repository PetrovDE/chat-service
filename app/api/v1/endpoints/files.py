"""
File management endpoints
Эндпоинты для работы с файлами: загрузка, обработка, удаление
"""
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status, Query, Form
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
from uuid import UUID, uuid4
import aiofiles
import os
from pathlib import Path
import logging

from app.db.session import get_db
from app.db.models import User
from app.schemas import FileUploadResponse, FileInfo
from app.api.dependencies import get_current_user
from app.crud import crud_file
from app.core.config import settings

logger = logging.getLogger(__name__)
router = APIRouter()

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)


@router.post("/upload", response_model=FileUploadResponse)
async def upload_file(
        file: UploadFile = File(...),
        conversation_id: UUID = Form(...),
        embedding_mode: str = Form("local"),
        embedding_model: Optional[str] = Form(None),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """
    Upload and process file, associating it with a specific conversation

    Args:
        file: Файл для загрузки
        conversation_id: ID диалога для привязки файла
        embedding_mode: Режим генерации эмбеддингов ('local' или 'corporate')
        embedding_model: Модель для эмбеддингов (опционально)
        db: Сессия БД
        current_user: Текущий пользователь

    Returns:
        FileUploadResponse: Информация о загруженном файле
    """
    # Валидация типа файла
    if not settings.is_file_supported(file.filename):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"File type not supported. Supported: {settings.supported_filetypes}"
        )

    # Валидация размера файла
    if file.size and file.size > settings.MAX_FILESIZE_MB * 1024 * 1024:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File too large (max {settings.MAX_FILESIZE_MB}MB)"
        )

    # Валидация режима эмбеддингов (поддерживаем оба варианта названия)
    valid_modes = ["local", "corporate", "aihub"]
    if embedding_mode not in valid_modes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid embedding_mode. Must be 'local' or 'corporate'"
        )

    logger.info(f"📤 Uploading file: {file.filename}, mode: {embedding_mode}, model: {embedding_model}")

    # Сохранение файла на диск
    file_id = uuid4()
    file_path = UPLOAD_DIR / f"{current_user.id}" / f"{file_id}_{file.filename}"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        async with aiofiles.open(file_path, 'wb') as f:
            content = await file.read()
            await f.write(content)

        logger.info(f"✅ File saved to disk: {file_path}")
    except Exception as e:
        logger.error(f"❌ Failed to save file: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to save file: {str(e)}"
        )

    # Определение типа файла
    file_type = file.filename.split('.')[-1].lower()

    # Предпросмотр содержимого для текстовых файлов
    content_preview = None
    if file_type in ['txt', 'md']:
        try:
            content_preview = content[:500].decode('utf-8', errors='ignore')
        except Exception as e:
            logger.warning(f"⚠️ Could not create preview: {e}")

    # Сохранение записи в БД
    try:
        file_record = await crud_file.create_file(
            db,
            user_id=current_user.id,
            filename=f"{file_id}_{file.filename}",
            original_filename=file.filename,
            path=str(file_path),
            file_type=file_type,
            file_size=len(content),
            content_preview=content_preview
        )

        logger.info(f"✅ File record created in DB: {file_record.id}")
    except Exception as e:
        logger.error(f"❌ Failed to create file record: {e}")
        # Удаляем файл с диска если не удалось создать запись
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create file record: {str(e)}"
        )

    # Привязка файла к диалогу
    try:
        await crud_file.add_file_to_conversation(
            db,
            file_id=file_record.id,
            conversation_id=conversation_id
        )

        logger.info(f"✅ File associated with conversation: {conversation_id}")
    except Exception as e:
        logger.error(f"❌ Failed to associate file with conversation: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to associate file with conversation: {str(e)}"
        )

    # Запуск асинхронной обработки файла
    try:
        from app.services.file import process_file_async
        await process_file_async(
            file_id=file_record.id,
            file_path=file_path,
            embedding_mode=embedding_mode,
            embedding_model=embedding_model
        )

        logger.info(f"🚀 File processing started: {file_record.id}")
    except Exception as e:
        logger.error(f"❌ Failed to start file processing: {e}")
        # Не выбрасываем исключение, файл уже загружен, обработка будет в фоне

    return FileUploadResponse(
        file_id=file_record.id,
        filename=file_record.filename,
        original_filename=file_record.original_filename,
        file_type=file_record.file_type,
        file_size=file_record.file_size,
        content_preview=file_record.content_preview,
        is_processed=file_record.is_processed,
        chunks_count=file_record.chunks_count,
        uploaded_at=file_record.uploaded_at
    )


@router.post("/process/{file_id}")
async def process_file(
        file_id: UUID,
        embedding_mode: str = Form("local"),
        embedding_model: Optional[str] = Form(None),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """
    Manually trigger file processing (if it failed or needs reprocessing)

    Args:
        file_id: ID файла
        embedding_mode: Режим генерации эмбеддингов ('local' или 'corporate')
        embedding_model: Модель для эмбеддингов (опционально)
    """
    file = await crud_file.get_user_file(
        db,
        file_id=file_id,
        user_id=current_user.id
    )

    if not file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )

    if not os.path.exists(file.path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Physical file not found"
        )

    # Валидация режима
    valid_modes = ["local", "corporate", "aihub"]
    if embedding_mode not in valid_modes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid embedding_mode. Must be 'local' or 'corporate'"
        )

    try:
        from app.services.file import process_file_async
        await process_file_async(
            file_id=file_id,
            file_path=Path(file.path),
            embedding_mode=embedding_mode,
            embedding_model=embedding_model
        )

        return {
            "success": True,
            "message": "File processing started",
            "file_id": str(file_id),
            "embedding_mode": embedding_mode
        }
    except Exception as e:
        logger.error(f"❌ Failed to start file processing: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start file processing: {str(e)}"
        )


@router.get("/", response_model=List[FileInfo])
async def get_files(
        skip: int = 0,
        limit: int = 100,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Get all files for current user"""
    files = await crud_file.get_user_files(
        db,
        user_id=current_user.id,
        skip=skip,
        limit=limit
    )

    # Обогащаем файлы информацией о диалогах
    from sqlalchemy import select
    from app.db.models.conversation_file import ConversationFile

    result = []
    for file in files:
        # Получаем все диалоги для этого файла
        query = select(ConversationFile.conversation_id).where(
            ConversationFile.file_id == file.id
        )
        conv_result = await db.execute(query)
        conversation_ids = conv_result.scalars().all()

        file_info = FileInfo(
            id=file.id,
            filename=file.filename,
            original_filename=file.original_filename,
            file_type=file.file_type,
            file_size=file.file_size,
            is_processed=file.is_processed,
            chunks_count=file.chunks_count,
            uploaded_at=file.uploaded_at,
            processed_at=file.processed_at,
            conversation_ids=list(conversation_ids)
        )
        result.append(file_info)

    return result


@router.get("/processed", response_model=List[FileInfo])
async def get_processed_files(
        conversation_id: UUID = Query(None, description="Optional: filter by conversation"),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Get processed files for current user, optionally filtered by conversation"""

    if conversation_id:
        # Получаем файлы для конкретного диалога
        files = await crud_file.get_conversation_files(
            db,
            conversation_id=conversation_id,
            user_id=current_user.id
        )
    else:
        # Получаем все обработанные файлы
        files = await crud_file.get_processed_files(
            db,
            user_id=current_user.id
        )

    # Обогащаем файлы информацией о диалогах
    from sqlalchemy import select
    from app.db.models.conversation_file import ConversationFile

    result = []
    for file in files:
        # Получаем все диалоги для этого файла
        query = select(ConversationFile.conversation_id).where(
            ConversationFile.file_id == file.id
        )
        conv_result = await db.execute(query)
        conversation_ids = conv_result.scalars().all()

        file_info = FileInfo(
            id=file.id,
            filename=file.filename,
            original_filename=file.original_filename,
            file_type=file.file_type,
            file_size=file.file_size,
            is_processed=file.is_processed,
            chunks_count=file.chunks_count,
            uploaded_at=file.uploaded_at,
            processed_at=file.processed_at,
            conversation_ids=list(conversation_ids)
        )
        result.append(file_info)

    return result


@router.get("/{file_id}", response_model=FileInfo)
async def get_file(
        file_id: UUID,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Get file info with associated conversations"""
    file = await crud_file.get_user_file(
        db,
        file_id=file_id,
        user_id=current_user.id
    )

    if not file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )

    # Получаем связанные диалоги
    from sqlalchemy import select
    from app.db.models.conversation_file import ConversationFile

    query = select(ConversationFile.conversation_id).where(
        ConversationFile.file_id == file_id
    )
    conv_result = await db.execute(query)
    conversation_ids = conv_result.scalars().all()

    return FileInfo(
        id=file.id,
        filename=file.filename,
        original_filename=file.original_filename,
        file_type=file.file_type,
        file_size=file.file_size,
        is_processed=file.is_processed,
        chunks_count=file.chunks_count,
        uploaded_at=file.uploaded_at,
        processed_at=file.processed_at,
        conversation_ids=list(conversation_ids)
    )


@router.delete("/{file_id}")
async def delete_file(
        file_id: UUID,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user)
):
    """Delete file and all associated embeddings"""
    file = await crud_file.get_user_file(
        db,
        file_id=file_id,
        user_id=current_user.id
    )

    if not file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found"
        )

    try:
        # 1. Удаляем эмбеддинги из ChromaDB
        await delete_file_from_chroma(str(file_id))
        logger.info(f"✅ Deleted embeddings from ChromaDB for file {file_id}")

        # 2. Удаляем эмбеддинги из PostgreSQL (если существуют)
        await delete_file_from_postgres(db, str(file_id))
        logger.info(f"✅ Deleted embeddings from PostgreSQL for file {file_id}")

        # 3. Удаляем физический файл
        if os.path.exists(file.path):
            try:
                os.remove(file.path)
                logger.info(f"✅ Deleted physical file: {file.path}")
            except OSError as e:
                logger.warning(f"⚠️ Could not delete physical file {file.path}: {e}")
                # Продолжаем даже если файл не удалился - может быть заблокирован

        # 4. Удаляем из базы данных
        await crud_file.remove(db, id=file_id)
        logger.info(f"✅ Deleted file record from database: {file_id}")

        return {
            "success": True,
            "message": "File and all embeddings deleted successfully",
            "file_id": str(file_id)
        }
    except Exception as e:
        logger.error(f"❌ Error deleting file {file_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting file: {str(e)}"
        )


async def delete_file_from_chroma(file_id: str):
    """Delete all embeddings for a file from ChromaDB"""
try:
        from app.rag.vector_store import vectorstore_manager
        
        logger.info(f"🗑️ Deleting ChromaDB embeddings for file: {file_id}")
        
        # Удаляем по фильтру метаданных из всех коллекций
        deleted_count = vectorstore_manager.delete_by_metadata(
            filter_dict={"file_id": file_id}
        )
        
        logger.info(f"✅ Successfully deleted {deleted_count} embeddings for file: {file_id}")
        
        return deleted_count
        
    except Exception as e:
        logger.error(f"❌ Error deleting from ChromaDB: {e}", exc_info=True)
        raise

async def delete_file_from_postgres(db: AsyncSession, file_id: str):
    """Delete all embeddings for a file from PostgreSQL"""
    try:
        from sqlalchemy import text

        # Проверяем существование таблицы document_embeddings
        check_query = text("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = 'document_embeddings')
        """)
        result = await db.execute(check_query)
        table_exists = result.scalar()

        if table_exists:
            delete_query = text("""
                DELETE FROM document_embeddings
                WHERE metadata ->> 'file_id' = :file_id
            """)
            await db.execute(delete_query, {"file_id": file_id})
            await db.commit()
            logger.info(f"✅ Successfully deleted PostgreSQL embeddings for file: {file_id}")
        else:
            logger.info("ℹ️ document_embeddings table does not exist, skipping PostgreSQL cleanup")
    except Exception as e:
        logger.error(f"❌ Error deleting from PostgreSQL: {e}")
        # Не выбрасываем исключение - PostgreSQL эмбеддинги опциональны
        pass

# app/routers/files.py
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File as FastAPIFile, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db, crud
from app.database.models import User
from app.routers.auth import get_current_user_optional
from app import models
import logging
import uuid
import os
from pathlib import Path

router = APIRouter()  # <-- БЕЗ prefix="/files"
logger = logging.getLogger(__name__)

# Директория для загрузки файлов
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)


@router.post("/upload", response_model=models.FileUploadResponse)
async def upload_file(
    file: UploadFile = FastAPIFile(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user_optional)
):
    """Загрузить файл"""
    try:
        user_id = current_user.id if current_user else None
        
        # Генерировать уникальное имя файла
        file_id = uuid.uuid4()
        file_extension = Path(file.filename).suffix
        unique_filename = f"{file_id}{file_extension}"
        file_path = UPLOAD_DIR / unique_filename
        
        # Сохранить файл
        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)
        
        file_size = len(content)
        
        # Создать preview (первые 500 символов для текстовых файлов)
        content_preview = None
        full_content = None
        
        if file.content_type and file.content_type.startswith('text/'):
            try:
                text_content = content.decode('utf-8')
                content_preview = text_content[:500]
                full_content = text_content
            except:
                pass
        
        # Сохранить в БД
        db_file = await crud.create_file(
            db=db,
            user_id=user_id,
            filename=unique_filename,
            original_filename=file.filename,
            file_path=str(file_path),
            file_type=file.content_type or "application/octet-stream",
            file_size=file_size,
            content_preview=content_preview,
            full_content=full_content
        )
        
        logger.info(f"File uploaded: {file.filename} ({file_size} bytes)")
        
        return models.FileUploadResponse(
            file_id=db_file.id,
            filename=db_file.filename,
            original_filename=db_file.original_filename,
            file_type=db_file.file_type,
            file_size=db_file.file_size,
            content_preview=content_preview
        )
        
    except Exception as e:
        logger.error(f"File upload error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload file: {str(e)}"
        )


@router.get("/{file_id}", response_model=models.FileInfo)
async def get_file_info(
    file_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user_optional)
):
    """Получить информацию о файле"""
    try:
        file = await crud.get_file(db, file_id)
        
        if not file:
            raise HTTPException(status_code=404, detail="File not found")
        
        user_id = current_user.id if current_user else None
        if file.user_id != user_id:
            raise HTTPException(status_code=403, detail="Access denied")
        
        return file
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get file error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get file")


@router.delete("/{file_id}")
async def delete_file(
    file_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user_optional)
):
    """Удалить файл"""
    try:
        file = await crud.get_file(db, file_id)
        
        if not file:
            raise HTTPException(status_code=404, detail="File not found")
        
        user_id = current_user.id if current_user else None
        if file.user_id != user_id:
            raise HTTPException(status_code=403, detail="Access denied")
        
        # Удалить физический файл
        if os.path.exists(file.file_path):
            os.remove(file.file_path)
        
        # Удалить из БД
        await crud.delete_file(db, file_id)
        
        logger.info(f"File deleted: {file.filename}")
        return {"success": True, "message": "File deleted"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete file error: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete file")
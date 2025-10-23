# app/routers/files.py

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File as FastAPIFile, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from pathlib import Path
import logging
import uuid

from app.database import get_db, crud
from app.database.models import User
from app.routers.auth import get_current_user_optional
from app import models

router = APIRouter()
logger = logging.getLogger(__name__)

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)


@router.post("/upload", response_model=models.FileUploadResponse)
async def upload_file(
        file: UploadFile = FastAPIFile(...),
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user_optional),
        background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    ⚡ БЫСТРАЯ загрузка файла (< 1 сек)

    Поля ответа (из FileUploadResponse):
    - file_id
    - filename
    - original_filename
    - file_type
    - file_size
    - content_preview (опционально)
    """
    try:
        user_id = current_user.id if current_user else None

        # === ЭТАП 1: БЫСТРАЯ ЗАГРУЗКА ===
        file_id = uuid.uuid4()
        file_extension = Path(file.filename).suffix
        unique_filename = f"{file_id}{file_extension}"
        file_path = UPLOAD_DIR / unique_filename

        # Сохраняем файл
        content = await file.read()
        with open(file_path, 'wb') as f:
            f.write(content)

        file_size = len(content)

        # Создаем preview для текстовых файлов
        content_preview = None
        if file.content_type and file.content_type.startswith('text'):
            try:
                text_content = content.decode('utf-8')
                content_preview = text_content[:500]
            except:
                pass

        # === ЭТАП 2: СОХРАНЯЕМ МЕТАДАННЫЕ В БД ===
        db_file = await crud.create_file(
            db,
            user_id=user_id,
            filename=unique_filename,
            original_filename=file.filename,
            file_path=str(file_path),
            file_type=file.content_type or "application/octet-stream",
            file_size=file_size,
            content_preview=content_preview
        )

        logger.info(f"✅ File uploaded: {file.filename} ({file_size} bytes)")

        # ✅ ВОЗВРАЩАЕМ ТОЛЬКО РЕАЛЬНЫЕ ПОЛЯ FileUploadResponse
        return models.FileUploadResponse(
            file_id=db_file.id,
            filename=db_file.filename,
            original_filename=db_file.original_filename,
            file_type=db_file.file_type,
            file_size=db_file.file_size,
            content_preview=content_preview
        )

    except Exception as e:
        logger.error(f"Upload error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload file: {str(e)}"
        )


@router.get("/list")
async def list_files(
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user_optional)
):
    """
    Получить список загруженных файлов пользователя
    """
    try:
        user_id = current_user.id if current_user else None

        files = await crud.get_user_files(db, user_id)

        return {
            "files": [
                {
                    "file_id": f.id,
                    "filename": f.filename,
                    "original_filename": f.original_filename,
                    "file_size": f.file_size,
                    "file_type": f.file_type,
                    "created_at": f.created_at
                }
                for f in files
            ]
        }
    except Exception as e:
        logger.error(f"Error listing files: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete("/delete/{file_id}")
async def delete_file(
        file_id: str,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user_optional)
):
    """
    Удалить файл
    """
    try:
        user_id = current_user.id if current_user else None

        file = await crud.get_file(db, file_id)
        if not file:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="File not found")

        if file.user_id != user_id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

        # Удалить файл с диска
        file_path = Path(file.file_path)
        if file_path.exists():
            file_path.unlink()

        # Удалить из БД
        await crud.delete_file(db, file_id)

        logger.info(f"✅ File deleted: {file_id}")

        return {"status": "deleted", "file_id": file_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

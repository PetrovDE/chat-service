# app/routers/files.py
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File as FastAPIFile, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.database import get_db, crud
from app.database.models import User
from app.routers.auth import get_current_user_optional
from app import models
from app.llm_manager import llm_manager
from app.rag.retriever import rag_retriever  # 🆕 Импорт RAG
import logging
import uuid
import os
from pathlib import Path

router = APIRouter()
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
    """Загрузить файл и автоматически векторизовать для RAG"""
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

        # 🆕 АВТОМАТИЧЕСКАЯ ВЕКТОРИЗАЦИЯ ДЛЯ RAG
        try:
            logger.info(f"🤖 Starting RAG processing for {file.filename}...")

            rag_result = await rag_retriever.process_and_store_db_file(db_file)

            if rag_result.get('success'):
                logger.info(
                    f"✅ RAG processing completed: {rag_result.get('chunks_count')} chunks created"
                )
            else:
                logger.warning(f"⚠️ RAG processing failed: {rag_result.get('error')}")

        except Exception as e:
            # Не падаем если RAG не сработал - файл уже загружен
            logger.error(f"❌ RAG processing error (non-critical): {e}")

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


@router.post("/analyze")
async def analyze_file(
        file_request: models.FileAnalysisRequest,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user_optional)
):
    """Анализировать загруженный файл используя RAG"""
    try:
        user_id = current_user.id if current_user else None

        # Получить файл из БД
        file = await crud.get_file(db, file_request.file_id)

        if not file:
            raise HTTPException(status_code=404, detail="File not found")

        if file.user_id != user_id:
            raise HTTPException(status_code=403, detail="Access denied")

        logger.info(f"📊 Analyzing file with RAG: {file.original_filename}")

        # 🆕 ИСПОЛЬЗОВАНИЕ RAG ДЛЯ АНАЛИЗА
        try:
            # Попробовать RAG анализ
            if file_request.query:
                # Конкретный вопрос
                analysis = await rag_retriever.analyze_file(
                    str(file.id),
                    query=file_request.query
                )
            else:
                # Общий анализ
                analysis = await rag_retriever.analyze_file(str(file.id))

            logger.info(f"✅ RAG analysis completed for {file.original_filename}")

            return models.FileAnalysisResponse(
                file_id=file.id,
                analysis=analysis,
                extracted_text=file.content_preview
            )

        except Exception as rag_error:
            logger.warning(f"⚠️ RAG analysis failed, falling back to direct LLM: {rag_error}")

            # 🔄 FALLBACK: Старый метод (если RAG не работает)
            if not file.full_content:
                try:
                    with open(file.file_path, 'r', encoding='utf-8') as f:
                        file.full_content = f.read()
                except Exception as e:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Cannot read file content: {str(e)}"
                    )

            # Создать промпт для анализа (старый метод)
            if file_request.query:
                prompt = f"""Проанализируй следующий файл и ответь на вопрос.

Имя файла: {file.original_filename}
Тип файла: {file.file_type}

Содержимое файла:
{file.full_content[:5000]}

Вопрос: {file_request.query}

Дай подробный ответ на основе содержимого файла."""
            else:
                prompt = f"""Проанализируй следующий файл и предоставь краткое резюме.

Имя файла: {file.original_filename}
Тип файла: {file.file_type}

Содержимое файла:
{file.full_content[:5000]}

Предоставь:
1. Краткое описание содержимого
2. Основные темы или данные
3. Структуру файла (если применимо)
4. Полезные выводы"""

            # Отправить запрос в LLM
            result = await llm_manager.generate_response(
                prompt=prompt,
                temperature=0.3,
                max_tokens=2000
            )

            logger.info(f"✅ Fallback analysis completed: {file.original_filename}")

            return models.FileAnalysisResponse(
                file_id=file.id,
                analysis=result["response"],
                extracted_text=file.full_content[:1000] if file.full_content else None
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"File analysis error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to analyze file: {str(e)}"
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
    """Удалить файл и его данные из RAG"""
    try:
        file = await crud.get_file(db, file_id)

        if not file:
            raise HTTPException(status_code=404, detail="File not found")

        user_id = current_user.id if current_user else None
        if file.user_id != user_id:
            raise HTTPException(status_code=403, detail="Access denied")

        # 🆕 УДАЛЕНИЕ ИЗ RAG VECTOR STORE
        try:
            logger.info(f"🗑️ Removing file from RAG: {file.original_filename}")
            chunks_removed = rag_retriever.remove_file_from_store(str(file_id))
            logger.info(f"✅ Removed {chunks_removed} chunks from vector store")
        except Exception as e:
            logger.warning(f"⚠️ Failed to remove from RAG (non-critical): {e}")

        # Удалить физический файл
        if os.path.exists(file.file_path):
            os.remove(file.file_path)

        # Удалить из БД
        await crud.delete_file(db, file_id)

        logger.info(f"✅ File deleted: {file.filename}")
        return {"success": True, "message": "File deleted"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete file error: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete file")


# 🆕 НОВЫЙ ENDPOINT: RAG статистика
@router.get("/rag/stats")
async def get_rag_stats(
        current_user: User = Depends(get_current_user_optional)
):
    """Получить статистику RAG системы"""
    try:
        stats = rag_retriever.get_stats()
        return stats
    except Exception as e:
        logger.error(f"Error getting RAG stats: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get RAG stats: {str(e)}"
        )
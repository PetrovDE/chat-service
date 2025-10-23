# app/routers/files.py
# ⭐ ПОЛНОСТЬЮ ИСПРАВЛЕННЫЙ - ФАЙЛЫ + RAG + ASYNC ОБРАБОТКА ⭐

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File as FastAPIFile, status, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from pathlib import Path
import logging
import uuid
import asyncio

from app.database import get_db, crud
from app.database.models import User
from app.routers.auth import get_current_user_optional
from app import models
from app.rag.retriever import rag_retriever

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
    ⚡ Загрузить файл и автоматически векторизовать для RAG
    """
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

        # Создать preview
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

        logger.info(f"✅ File uploaded: {file.filename} ({file_size} bytes)")

        # 🆕 ЗАПУСТИТЬ RAG ОБРАБОТКУ В ФОНЕ
        async def process_rag():
            try:
                logger.info(f"🤖 Starting RAG processing for {file.filename}...")
                rag_result = await asyncio.to_thread(
                    rag_retriever.process_and_store_db_file,
                    db_file
                )
                if rag_result.get('success'):
                    logger.info(f"✅ RAG: {rag_result.get('chunks_count')} chunks created")
                else:
                    logger.warning(f"⚠️ RAG failed: {rag_result.get('error')}")
            except Exception as e:
                logger.error(f"❌ RAG error: {e}")

        # Добавить задачу в background
        background_tasks.add_task(process_rag)

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


@router.post("/analyze")  # ✅ ПРАВИЛЬНЫЙ URL!
async def analyze_file(
        file_request: models.FileAnalysisRequest,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user_optional)
):
    """
    📊 Анализировать загруженный файл используя RAG
    """
    try:
        user_id = current_user.id if current_user else None

        # Получить файл из БД
        file = await crud.get_file(db, file_request.file_id)
        if not file:
            raise HTTPException(status_code=404, detail="File not found")
        if file.user_id != user_id:
            raise HTTPException(status_code=403, detail="Access denied")

        logger.info(f"📊 Analyzing: {file.original_filename}")

        # 🆕 ИСПОЛЬЗОВАНИЕ RAG ДЛЯ АНАЛИЗА
        try:
            if file_request.query:
                # ✅ ИСПРАВЛЕНО: asyncio.to_thread для синхронного метода
                analysis = await asyncio.to_thread(
                    rag_retriever.analyze_file,
                    str(file.id),
                    query=file_request.query
                )
            else:
                analysis = await asyncio.to_thread(
                    rag_retriever.analyze_file,
                    str(file.id)
                )

            logger.info(f"✅ RAG analysis completed")

            return models.FileAnalysisResponse(
                file_id=file.id,
                analysis=analysis,
                extracted_text=file.content_preview
            )

        except Exception as rag_error:
            logger.warning(f"⚠️ RAG failed, using fallback: {rag_error}")

            # FALLBACK к прямому LLM
            from app.llm_manager import llm_manager

            if not file.full_content:
                try:
                    with open(file.file_path, 'r', encoding='utf-8') as f:
                        file.full_content = f.read()
                except Exception as e:
                    raise HTTPException(status_code=400, detail=f"Cannot read file: {str(e)}")

            if file_request.query:
                prompt = f"""Проанализируй файл и ответь на вопрос.

Файл: {file.original_filename}
Содержимое: {file.full_content[:5000]}

Вопрос: {file_request.query}

Ответ:"""
            else:
                prompt = f"""Проанализируй файл и дай краткое резюме.

Файл: {file.original_filename}
Содержимое: {file.full_content[:5000]}

Резюме:"""

            result = await llm_manager.generate_response(
                prompt=prompt,
                temperature=0.3,
                max_tokens=2000
            )

            return models.FileAnalysisResponse(
                file_id=file.id,
                analysis=result["response"],
                extracted_text=file.content_preview
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Analysis error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/list")
async def list_files(
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user_optional)
):
    """Список файлов пользователя"""
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
        logger.error(f"List files error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{file_id}", response_model=models.FileInfo)
async def get_file_info(
        file_id: uuid.UUID,
        db: AsyncSession = Depends(get_db),
        current_user: User = Depends(get_current_user_optional)
):
    """Информация о файле"""
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
        raise HTTPException(status_code=500, detail=str(e))


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

        # Удалить из RAG
        try:
            logger.info(f"🗑️ Removing from RAG: {file.original_filename}")
            await asyncio.to_thread(
                rag_retriever.remove_file_from_store,
                str(file_id)
            )
            logger.info(f"✅ Removed from vector store")
        except Exception as e:
            logger.warning(f"⚠️ RAG removal failed: {e}")

        # Удалить физический файл
        import os
        if os.path.exists(file.file_path):
            os.remove(file.file_path)

        # Удалить из БД
        await crud.delete_file(db, file_id)

        logger.info(f"✅ File deleted: {file.filename}")
        return {"success": True, "message": "File deleted"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rag/stats")
async def get_rag_stats(
        current_user: User = Depends(get_current_user_optional)
):
    """RAG статистика"""
    try:
        stats = await asyncio.to_thread(rag_retriever.get_stats)
        return stats
    except Exception as e:
        logger.error(f"RAG stats error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

"""
Files router - handles file uploads and processing
"""

from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from pathlib import Path
from datetime import datetime
import logging

from ..models import FileAnalysisRequest, FileAnalysisResponse
from ..llm_manager import llm_manager
from ..database import get_db, crud
from ..auth import get_optional_user
from ..database.models import User

router = APIRouter(prefix="/files", tags=["files"])
logger = logging.getLogger(__name__)


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_optional_user)
):
    """Upload and process a file for LLM analysis"""
    try:
        logger.info(f"Processing uploaded file: {file.filename}")

        # Check file type
        file_extension = Path(file.filename).suffix.lower()
        supported_types = ['.txt', '.pdf', '.csv', '.xlsx', '.xls', '.json']

        if file_extension not in supported_types:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type: {file_extension}. Supported: {supported_types}"
            )

        # Read file content
        file_content = await file.read()

        # Process file
        processed_content = await llm_manager.process_uploaded_file(
            file_content, file.filename, file_extension
        )

        # Get user_id
        user_id = current_user.id if current_user else None

        # Save file record to database
        file_record = await crud.create_file_record(
            db=db,
            user_id=user_id,
            filename=file.filename,
            original_filename=file.filename,
            file_path="",
            file_type=file_extension,
            file_size=len(file_content),
            content_preview=processed_content["preview"],
            full_content=processed_content["content"]
        )

        return {
            "file_id": str(file_record.id),
            "filename": file.filename,
            "file_type": file_extension,
            "file_size": len(file_content),
            "preview": processed_content["preview"],
            "success": processed_content["success"]
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process file: {str(e)}"
        )


@router.post("/analyze-file", response_model=FileAnalysisResponse)
async def analyze_file(
    request: FileAnalysisRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_optional_user)
):
    """Analyze uploaded file content using LLM"""
    try:
        logger.info(f"Analyzing file with type: {request.analysis_type}")

        result = await llm_manager.analyze_file_content(
            content=request.content,
            analysis_type=request.analysis_type,
            custom_prompt=request.custom_prompt
        )

        return FileAnalysisResponse(
            filename=request.filename,
            analysis_type=request.analysis_type,
            result=result,
            model=llm_manager.get_current_model_name(),
            timestamp=datetime.now()
        )

    except Exception as e:
        logger.error(f"Error analyzing file: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to analyze file: {str(e)}"
        )
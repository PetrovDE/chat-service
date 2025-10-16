"""
Statistics router - handles usage statistics
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta
import logging

from ..models import UsageStatsResponse
from ..database import get_db, crud
from ..auth import get_optional_user
from ..database.models import User

router = APIRouter(prefix="/stats", tags=["statistics"])
logger = logging.getLogger(__name__)


@router.get("/usage", response_model=UsageStatsResponse)
async def get_usage_statistics(
    days: int = 7,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_optional_user)
):
    """Get usage statistics for the last N days"""
    try:
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)

        # Get user_id
        user_id = current_user.id if current_user else None

        stats = await crud.get_usage_stats(
            db=db,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date
        )

        return UsageStatsResponse(
            **stats,
            period_start=start_date,
            period_end=end_date
        )
    except Exception as e:
        logger.error(f"Error getting usage stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

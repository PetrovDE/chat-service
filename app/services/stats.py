# app/stats.py

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from app.db.models.system import APIUsageLog
from app.core.config import settings

logger = logging.getLogger(__name__)

class StatsManager:
    def __init__(self):
        logger.info("StatsManager initialized")

    async def usage_by_user(self, db: AsyncSession, user_id: str, days: int = 7) -> Dict[str, Any]:
        start_date = datetime.utcnow() - timedelta(days=days)
        stmt = (
            select(
                APIUsageLog.endpoint,
                func.sum(APIUsageLog.tokens_used),
                func.avg(APIUsageLog.response_time)
            )
            .where(APIUsageLog.user_id == user_id)
            .where(APIUsageLog.timestamp >= start_date)
            .group_by(APIUsageLog.endpoint)
        )
        rows = await db.execute(stmt)
        results = []
        for endpoint, tokens, avg_resp_time in rows.fetchall():
            results.append({
                "endpoint": endpoint,
                "tokens_used": tokens,
                "avg_response_time": avg_resp_time,
            })
        return {"by_endpoint": results}

    async def global_usage(self, db: AsyncSession, days: int = 7) -> Dict[str, Any]:
        start_date = datetime.utcnow() - timedelta(days=days)
        stmt = (
            select(
                APIUsageLog.endpoint,
                func.sum(APIUsageLog.tokens_used),
                func.avg(APIUsageLog.response_time)
            )
            .where(APIUsageLog.timestamp >= start_date)
            .group_by(APIUsageLog.endpoint)
        )
        rows = await db.execute(stmt)
        results = []
        for endpoint, tokens, avg_resp_time in rows.fetchall():
            results.append({
                "endpoint": endpoint,
                "tokens_used": tokens,
                "avg_response_time": avg_resp_time,
            })
        return {"global_by_endpoint": results}

    async def add_usage_log(self, db: AsyncSession, user_id: Optional[str], endpoint: str, tokens_used: int, response_time: float):
        log = APIUsageLog(
            user_id=user_id,
            endpoint=endpoint,
            tokens_used=tokens_used,
            response_time=response_time,
            timestamp=datetime.utcnow()
        )
        db.add(log)
        await db.commit()
        logger.info(f"Stats log added: {endpoint} user={user_id} tokens={tokens_used}")

stats_manager = StatsManager()

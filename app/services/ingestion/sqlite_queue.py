from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Optional

from app.services.ingestion.contracts import (
    IngestionEnqueueResult,
    IngestionJobPayload,
    IngestionLeasedJob,
    IngestionQueueAdapter,
    IngestionQueueStats,
)
from app.services.ingestion.sqlite_queue_runtime import (
    STATUS_COMPLETED,
    STATUS_DEAD_LETTER,
    STATUS_PROCESSING,
    STATUS_QUEUED,
    TERMINAL_STATUSES,
    acquire_sync as runtime_acquire_sync,
    enqueue_sync as runtime_enqueue_sync,
    get_stats_sync as runtime_get_stats_sync,
    heartbeat_sync as runtime_heartbeat_sync,
    init_db_sync as runtime_init_db_sync,
    mark_completed_sync as runtime_mark_completed_sync,
    mark_dead_letter_sync as runtime_mark_dead_letter_sync,
    mark_retry_sync as runtime_mark_retry_sync,
    release_lease_sync as runtime_release_lease_sync,
    requeue_expired_leases_sync as runtime_requeue_expired_leases_sync,
)


class SqliteIngestionQueueAdapter(IngestionQueueAdapter):
    def __init__(self, db_path: Path):
        self._db_path = Path(db_path)
        self._init_lock = asyncio.Lock()
        self._initialized = False

    async def enqueue(
        self,
        *,
        payload: IngestionJobPayload,
        idempotency_key: str,
        max_retries: int,
        allow_requeue_terminal: bool,
    ) -> IngestionEnqueueResult:
        await self._ensure_initialized()
        return await asyncio.to_thread(
            self._enqueue_sync,
            payload,
            idempotency_key,
            int(max_retries),
            bool(allow_requeue_terminal),
        )

    async def acquire(
        self,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> Optional[IngestionLeasedJob]:
        await self._ensure_initialized()
        return await asyncio.to_thread(
            self._acquire_sync,
            worker_id,
            float(lease_seconds),
        )

    async def mark_completed(self, job_id: str) -> None:
        await self._ensure_initialized()
        await asyncio.to_thread(self._mark_completed_sync, job_id)

    async def mark_retry(self, *, job_id: str, error_message: str, delay_seconds: float) -> None:
        await self._ensure_initialized()
        await asyncio.to_thread(
            self._mark_retry_sync,
            job_id,
            str(error_message),
            float(delay_seconds),
        )

    async def mark_dead_letter(self, *, job_id: str, error_message: str) -> None:
        await self._ensure_initialized()
        await asyncio.to_thread(self._mark_dead_letter_sync, job_id, str(error_message))

    async def release_lease(self, *, job_id: str, delay_seconds: float, error_message: str) -> None:
        await self._ensure_initialized()
        await asyncio.to_thread(
            self._release_lease_sync,
            job_id,
            float(delay_seconds),
            str(error_message),
        )

    async def requeue_expired_leases(self) -> int:
        await self._ensure_initialized()
        return await asyncio.to_thread(self._requeue_expired_leases_sync)

    async def heartbeat(self, *, worker_id: str) -> None:
        await self._ensure_initialized()
        await asyncio.to_thread(self._heartbeat_sync, worker_id)

    async def get_stats(self, *, worker_id: Optional[str]) -> IngestionQueueStats:
        await self._ensure_initialized()
        return await asyncio.to_thread(self._get_stats_sync, worker_id)

    async def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            await asyncio.to_thread(self._init_db_sync)
            self._initialized = True

    def _init_db_sync(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        runtime_init_db_sync(connect_fn=self._connect)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    def _enqueue_sync(
        self,
        payload: IngestionJobPayload,
        idempotency_key: str,
        max_retries: int,
        allow_requeue_terminal: bool,
    ) -> IngestionEnqueueResult:
        return runtime_enqueue_sync(
            connect_fn=self._connect,
            payload=payload,
            idempotency_key=idempotency_key,
            max_retries=max_retries,
            allow_requeue_terminal=allow_requeue_terminal,
        )

    def _acquire_sync(self, worker_id: str, lease_seconds: float) -> Optional[IngestionLeasedJob]:
        return runtime_acquire_sync(
            connect_fn=self._connect,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
        )

    def _mark_completed_sync(self, job_id: str) -> None:
        runtime_mark_completed_sync(connect_fn=self._connect, job_id=job_id)

    def _mark_retry_sync(self, job_id: str, error_message: str, delay_seconds: float) -> None:
        runtime_mark_retry_sync(
            connect_fn=self._connect,
            job_id=job_id,
            error_message=error_message,
            delay_seconds=delay_seconds,
        )

    def _mark_dead_letter_sync(self, job_id: str, error_message: str) -> None:
        runtime_mark_dead_letter_sync(
            connect_fn=self._connect,
            job_id=job_id,
            error_message=error_message,
        )

    def _release_lease_sync(self, job_id: str, delay_seconds: float, error_message: str) -> None:
        runtime_release_lease_sync(
            connect_fn=self._connect,
            job_id=job_id,
            delay_seconds=delay_seconds,
            error_message=error_message,
        )

    def _requeue_expired_leases_sync(self) -> int:
        return runtime_requeue_expired_leases_sync(connect_fn=self._connect)

    def _heartbeat_sync(self, worker_id: str) -> None:
        runtime_heartbeat_sync(connect_fn=self._connect, worker_id=worker_id)

    def _get_stats_sync(self, worker_id: Optional[str]) -> IngestionQueueStats:
        return runtime_get_stats_sync(connect_fn=self._connect, worker_id=worker_id)

from __future__ import annotations

import sqlite3
import time
from typing import Callable, Optional
from uuid import uuid4

from app.services.ingestion.contracts import (
    IngestionEnqueueResult,
    IngestionJobPayload,
    IngestionLeasedJob,
    IngestionQueueStats,
)

STATUS_QUEUED = "queued"
STATUS_PROCESSING = "processing"
STATUS_COMPLETED = "completed"
STATUS_DEAD_LETTER = "dead_letter"
TERMINAL_STATUSES = {STATUS_COMPLETED, STATUS_DEAD_LETTER}


def init_db_sync(*, connect_fn) -> None:
    conn = connect_fn()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS ingestion_jobs (
                job_id TEXT PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                file_id TEXT NOT NULL,
                file_path TEXT NOT NULL,
                embedding_mode TEXT NOT NULL,
                embedding_model TEXT NOT NULL,
                request_id TEXT,
                processing_id TEXT,
                pipeline_version TEXT,
                parser_version TEXT,
                artifact_version TEXT,
                chunking_strategy TEXT,
                retrieval_profile TEXT,
                status TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                next_run_at REAL NOT NULL,
                lease_owner TEXT,
                lease_until REAL,
                last_error TEXT,
                dedup_hits INTEGER NOT NULL DEFAULT 0,
                run_count INTEGER NOT NULL DEFAULT 1,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                completed_at REAL
            );

            CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_status_next_run
                ON ingestion_jobs(status, next_run_at, created_at);

            CREATE INDEX IF NOT EXISTS idx_ingestion_jobs_lease
                ON ingestion_jobs(status, lease_until);

            CREATE TABLE IF NOT EXISTS ingestion_workers (
                worker_id TEXT PRIMARY KEY,
                heartbeat_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            """
        )
        columns = {str(r["name"]) for r in conn.execute("PRAGMA table_info(ingestion_jobs)").fetchall()}
        if "processing_id" not in columns:
            conn.execute("ALTER TABLE ingestion_jobs ADD COLUMN processing_id TEXT")
        if "request_id" not in columns:
            conn.execute("ALTER TABLE ingestion_jobs ADD COLUMN request_id TEXT")
        if "pipeline_version" not in columns:
            conn.execute("ALTER TABLE ingestion_jobs ADD COLUMN pipeline_version TEXT")
        if "parser_version" not in columns:
            conn.execute("ALTER TABLE ingestion_jobs ADD COLUMN parser_version TEXT")
        if "artifact_version" not in columns:
            conn.execute("ALTER TABLE ingestion_jobs ADD COLUMN artifact_version TEXT")
        if "chunking_strategy" not in columns:
            conn.execute("ALTER TABLE ingestion_jobs ADD COLUMN chunking_strategy TEXT")
        if "retrieval_profile" not in columns:
            conn.execute("ALTER TABLE ingestion_jobs ADD COLUMN retrieval_profile TEXT")
        conn.commit()
    finally:
        conn.close()


def enqueue_sync(
    *,
    connect_fn,
    payload: IngestionJobPayload,
    idempotency_key: str,
    max_retries: int,
    allow_requeue_terminal: bool,
) -> IngestionEnqueueResult:
    now = time.time()
    conn = connect_fn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            """
            SELECT job_id, status, attempt
            FROM ingestion_jobs
            WHERE idempotency_key = ?
            """,
            (idempotency_key,),
        ).fetchone()

        if existing is not None:
            job_id = str(existing["job_id"])
            status = str(existing["status"])
            attempt = int(existing["attempt"] or 0)
            if status in {STATUS_QUEUED, STATUS_PROCESSING}:
                conn.execute(
                    """
                    UPDATE ingestion_jobs
                    SET dedup_hits = dedup_hits + 1, updated_at = ?
                    WHERE job_id = ?
                    """,
                    (now, job_id),
                )
                conn.commit()
                return IngestionEnqueueResult(
                    job_id=job_id,
                    deduplicated=True,
                    status=status,
                    attempt=attempt,
                )

            if allow_requeue_terminal and status in TERMINAL_STATUSES:
                conn.execute(
                    """
                    UPDATE ingestion_jobs
                    SET status = ?,
                        attempt = 0,
                        next_run_at = ?,
                        lease_owner = NULL,
                        lease_until = NULL,
                        last_error = NULL,
                        completed_at = NULL,
                        updated_at = ?,
                        run_count = run_count + 1
                    WHERE job_id = ?
                    """,
                    (STATUS_QUEUED, now, now, job_id),
                )
                conn.commit()
                return IngestionEnqueueResult(
                    job_id=job_id,
                    deduplicated=False,
                    status=STATUS_QUEUED,
                    attempt=0,
                )

            conn.execute(
                """
                UPDATE ingestion_jobs
                SET dedup_hits = dedup_hits + 1, updated_at = ?
                WHERE job_id = ?
                """,
                (now, job_id),
            )
            conn.commit()
            return IngestionEnqueueResult(
                job_id=job_id,
                deduplicated=True,
                status=status,
                attempt=attempt,
            )

        job_id = str(uuid4())
        conn.execute(
            """
            INSERT INTO ingestion_jobs(
                job_id,
                idempotency_key,
                file_id,
                file_path,
                embedding_mode,
                embedding_model,
                request_id,
                processing_id,
                pipeline_version,
                parser_version,
                artifact_version,
                chunking_strategy,
                retrieval_profile,
                status,
                attempt,
                max_retries,
                next_run_at,
                lease_owner,
                lease_until,
                last_error,
                dedup_hits,
                run_count,
                created_at,
                updated_at,
                completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, NULL, NULL, NULL, 0, 1, ?, ?, NULL)
            """,
            (
                job_id,
                idempotency_key,
                payload.file_id,
                payload.file_path,
                payload.embedding_mode,
                payload.embedding_model,
                payload.request_id,
                payload.processing_id,
                payload.pipeline_version,
                payload.parser_version,
                payload.artifact_version,
                payload.chunking_strategy,
                payload.retrieval_profile,
                STATUS_QUEUED,
                max(0, max_retries),
                now,
                now,
                now,
            ),
        )
        conn.commit()
        return IngestionEnqueueResult(
            job_id=job_id,
            deduplicated=False,
            status=STATUS_QUEUED,
            attempt=0,
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def acquire_sync(
    *,
    connect_fn,
    worker_id: str,
    lease_seconds: float,
) -> Optional[IngestionLeasedJob]:
    now = time.time()
    lease_until = now + max(0.0, lease_seconds)
    conn = connect_fn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT job_id
            FROM ingestion_jobs
            WHERE status = ? AND next_run_at <= ?
            ORDER BY next_run_at ASC, created_at ASC
            LIMIT 1
            """,
            (STATUS_QUEUED, now),
        ).fetchone()

        if row is None:
            conn.commit()
            return None

        job_id = str(row["job_id"])
        updated = conn.execute(
            """
            UPDATE ingestion_jobs
            SET status = ?,
                lease_owner = ?,
                lease_until = ?,
                attempt = attempt + 1,
                updated_at = ?
            WHERE job_id = ? AND status = ?
            """,
            (STATUS_PROCESSING, worker_id, lease_until, now, job_id, STATUS_QUEUED),
        )
        if int(updated.rowcount or 0) != 1:
            conn.rollback()
            return None

        acquired = conn.execute(
            """
            SELECT
                job_id,
                idempotency_key,
                file_id,
                file_path,
                embedding_mode,
                embedding_model,
                request_id,
                processing_id,
                pipeline_version,
                parser_version,
                artifact_version,
                chunking_strategy,
                retrieval_profile,
                attempt,
                max_retries,
                next_run_at,
                created_at
            FROM ingestion_jobs
            WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()
        conn.commit()
        if acquired is None:
            return None

        payload = IngestionJobPayload(
            file_id=str(acquired["file_id"]),
            file_path=str(acquired["file_path"]),
            embedding_mode=str(acquired["embedding_mode"]),
            embedding_model=str(acquired["embedding_model"]),
            request_id=(str(acquired["request_id"]) if acquired["request_id"] is not None else None),
            processing_id=(str(acquired["processing_id"]) if acquired["processing_id"] is not None else None),
            pipeline_version=(str(acquired["pipeline_version"]) if acquired["pipeline_version"] is not None else None),
            parser_version=(str(acquired["parser_version"]) if acquired["parser_version"] is not None else None),
            artifact_version=(str(acquired["artifact_version"]) if acquired["artifact_version"] is not None else None),
            chunking_strategy=(str(acquired["chunking_strategy"]) if acquired["chunking_strategy"] is not None else None),
            retrieval_profile=(str(acquired["retrieval_profile"]) if acquired["retrieval_profile"] is not None else None),
        )
        return IngestionLeasedJob(
            job_id=str(acquired["job_id"]),
            idempotency_key=str(acquired["idempotency_key"]),
            payload=payload,
            attempt=int(acquired["attempt"] or 0),
            max_retries=int(acquired["max_retries"] or 0),
            next_run_at=float(acquired["next_run_at"] or now),
            created_at=float(acquired["created_at"] or now),
        )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def mark_completed_sync(*, connect_fn, job_id: str) -> None:
    now = time.time()
    conn = connect_fn()
    try:
        conn.execute(
            """
            UPDATE ingestion_jobs
            SET status = ?,
                lease_owner = NULL,
                lease_until = NULL,
                last_error = NULL,
                completed_at = ?,
                updated_at = ?
            WHERE job_id = ?
            """,
            (STATUS_COMPLETED, now, now, job_id),
        )
        conn.commit()
    finally:
        conn.close()


def mark_retry_sync(*, connect_fn, job_id: str, error_message: str, delay_seconds: float) -> None:
    now = time.time()
    conn = connect_fn()
    try:
        conn.execute(
            """
            UPDATE ingestion_jobs
            SET status = ?,
                next_run_at = ?,
                lease_owner = NULL,
                lease_until = NULL,
                last_error = ?,
                updated_at = ?
            WHERE job_id = ?
            """,
            (STATUS_QUEUED, now + max(0.0, delay_seconds), error_message[:3000], now, job_id),
        )
        conn.commit()
    finally:
        conn.close()


def mark_dead_letter_sync(*, connect_fn, job_id: str, error_message: str) -> None:
    now = time.time()
    conn = connect_fn()
    try:
        conn.execute(
            """
            UPDATE ingestion_jobs
            SET status = ?,
                lease_owner = NULL,
                lease_until = NULL,
                last_error = ?,
                completed_at = ?,
                updated_at = ?
            WHERE job_id = ?
            """,
            (STATUS_DEAD_LETTER, error_message[:3000], now, now, job_id),
        )
        conn.commit()
    finally:
        conn.close()


def release_lease_sync(*, connect_fn, job_id: str, delay_seconds: float, error_message: str) -> None:
    now = time.time()
    conn = connect_fn()
    try:
        conn.execute(
            """
            UPDATE ingestion_jobs
            SET status = ?,
                next_run_at = ?,
                lease_owner = NULL,
                lease_until = NULL,
                last_error = ?,
                updated_at = ?
            WHERE job_id = ?
            """,
            (STATUS_QUEUED, now + max(0.0, delay_seconds), error_message[:3000], now, job_id),
        )
        conn.commit()
    finally:
        conn.close()


def requeue_expired_leases_sync(*, connect_fn) -> int:
    now = time.time()
    conn = connect_fn()
    try:
        updated = conn.execute(
            """
            UPDATE ingestion_jobs
            SET status = ?,
                next_run_at = ?,
                lease_owner = NULL,
                lease_until = NULL,
                updated_at = ?,
                last_error = COALESCE(last_error, 'lease_expired_recovery')
            WHERE status = ? AND lease_until IS NOT NULL AND lease_until < ?
            """,
            (STATUS_QUEUED, now, now, STATUS_PROCESSING, now),
        )
        conn.commit()
        return int(updated.rowcount or 0)
    finally:
        conn.close()


def heartbeat_sync(*, connect_fn, worker_id: str) -> None:
    now = time.time()
    conn = connect_fn()
    try:
        conn.execute(
            """
            INSERT INTO ingestion_workers(worker_id, heartbeat_at, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(worker_id)
            DO UPDATE SET heartbeat_at = excluded.heartbeat_at, updated_at = excluded.updated_at
            """,
            (worker_id, now, now),
        )
        conn.commit()
    finally:
        conn.close()


def get_stats_sync(*, connect_fn, worker_id: Optional[str]) -> IngestionQueueStats:
    now = time.time()
    conn = connect_fn()
    try:
        row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) AS queued,
                SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) AS processing,
                SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN status = ? THEN 1 ELSE 0 END) AS dead_letter
            FROM ingestion_jobs
            """,
            (STATUS_QUEUED, STATUS_PROCESSING, STATUS_COMPLETED, STATUS_DEAD_LETTER),
        ).fetchone()

        min_next_row = conn.execute(
            """
            SELECT MIN(next_run_at) AS min_next
            FROM ingestion_jobs
            WHERE status = ?
            """,
            (STATUS_QUEUED,),
        ).fetchone()

        lag_seconds = 0.0
        if min_next_row is not None and min_next_row["min_next"] is not None:
            lag_seconds = max(0.0, now - float(min_next_row["min_next"]))

        heartbeat_age: Optional[float] = None
        if worker_id:
            hb = conn.execute(
                """
                SELECT heartbeat_at
                FROM ingestion_workers
                WHERE worker_id = ?
                """,
                (worker_id,),
            ).fetchone()
            if hb is not None and hb["heartbeat_at"] is not None:
                heartbeat_age = max(0.0, now - float(hb["heartbeat_at"]))

        return IngestionQueueStats(
            queued=int((row["queued"] if row is not None else 0) or 0),
            processing=int((row["processing"] if row is not None else 0) or 0),
            completed=int((row["completed"] if row is not None else 0) or 0),
            dead_letter=int((row["dead_letter"] if row is not None else 0) or 0),
            lag_seconds=float(lag_seconds),
            last_heartbeat_age_seconds=heartbeat_age,
        )
    finally:
        conn.close()

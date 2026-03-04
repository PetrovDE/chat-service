from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Optional, Tuple

from app.observability.metrics import inc_counter, observe_ms, set_gauge
from app.observability.slo_metrics import observe_ingestion_retry, set_ingestion_queue_snapshot
from app.services.ingestion.contracts import (
    IngestionEnqueueResult,
    IngestionJobPayload,
    IngestionQueueAdapter,
    IngestionQueueStats,
)

logger = logging.getLogger(__name__)

ProcessIngestionJob = Callable[[IngestionJobPayload], Awaitable[Tuple[bool, bool]]]


@dataclass(frozen=True)
class IngestionWorkerConfig:
    worker_id: str
    lease_seconds: float
    poll_interval_seconds: float
    heartbeat_interval_seconds: float
    retry_base_seconds: float
    retry_max_seconds: float


class DurableIngestionWorker:
    def __init__(
        self,
        *,
        queue: IngestionQueueAdapter,
        processor: ProcessIngestionJob,
        config: IngestionWorkerConfig,
    ):
        self._queue = queue
        self._processor = processor
        self._config = config
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._current_job_id: Optional[str] = None
        self._last_stats = IngestionQueueStats(
            queued=0,
            processing=0,
            completed=0,
            dead_letter=0,
            lag_seconds=0.0,
            last_heartbeat_age_seconds=None,
        )

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        recovered = await self._queue.requeue_expired_leases()
        if recovered > 0:
            inc_counter("ingestion_recovered_expired_leases_total", value=int(recovered))
            logger.warning("Recovered expired ingestion leases: %d", recovered)
        self._task = asyncio.create_task(self._run(), name=f"ingestion-worker-{self._config.worker_id}")
        logger.info("Durable ingestion worker started: worker_id=%s", self._config.worker_id)

    async def enqueue(
        self,
        *,
        payload: IngestionJobPayload,
        idempotency_key: str,
        max_retries: int,
        allow_requeue_terminal: bool,
    ) -> IngestionEnqueueResult:
        return await self._queue.enqueue(
            payload=payload,
            idempotency_key=idempotency_key,
            max_retries=max_retries,
            allow_requeue_terminal=allow_requeue_terminal,
        )

    async def stop(self, timeout_seconds: float) -> None:
        task = self._task
        if task is None:
            return

        self._stop_event.set()
        try:
            await asyncio.wait_for(task, timeout=max(0.1, timeout_seconds))
        except asyncio.TimeoutError:
            logger.warning("Durable worker stop timeout. Cancelling worker task.")
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            if self._current_job_id:
                await self._queue.release_lease(
                    job_id=self._current_job_id,
                    delay_seconds=0.0,
                    error_message="worker_shutdown_release",
                )
        finally:
            self._task = None
            self._current_job_id = None
            logger.info("Durable ingestion worker stopped: worker_id=%s", self._config.worker_id)

    def snapshot(self) -> Dict[str, object]:
        running = bool(self._task is not None and not self._task.done())
        heartbeat_age = self._last_stats.last_heartbeat_age_seconds
        healthy = running and (heartbeat_age is None or heartbeat_age <= (self._config.heartbeat_interval_seconds * 3.0))
        return {
            "worker_running": running,
            "worker_id": self._config.worker_id,
            "queue_size": self._last_stats.queued,
            "processing": self._last_stats.processing,
            "completed": self._last_stats.completed,
            "dead_letter": self._last_stats.dead_letter,
            "lag_seconds": round(self._last_stats.lag_seconds, 3),
            "heartbeat_age_seconds": None if heartbeat_age is None else round(float(heartbeat_age), 3),
            "healthy": healthy,
            "lease_seconds": self._config.lease_seconds,
            "poll_interval_seconds": self._config.poll_interval_seconds,
            "retry_base_seconds": self._config.retry_base_seconds,
            "retry_max_seconds": self._config.retry_max_seconds,
        }

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        last_housekeep_ts = 0.0
        housekeep_interval = max(self._config.heartbeat_interval_seconds, 1.0)

        while not self._stop_event.is_set():
            try:
                now_monotonic = loop.time()
                if (now_monotonic - last_housekeep_ts) >= housekeep_interval:
                    last_housekeep_ts = now_monotonic
                    await self._queue.heartbeat(worker_id=self._config.worker_id)
                    recovered = await self._queue.requeue_expired_leases()
                    if recovered > 0:
                        inc_counter("ingestion_recovered_expired_leases_total", value=int(recovered))
                    self._last_stats = await self._queue.get_stats(worker_id=self._config.worker_id)
                    self._publish_queue_gauges(self._last_stats)

                leased = await self._queue.acquire(
                    worker_id=self._config.worker_id,
                    lease_seconds=self._config.lease_seconds,
                )
                if leased is None:
                    await asyncio.sleep(max(0.01, self._config.poll_interval_seconds))
                    continue

                self._current_job_id = leased.job_id
                lag_ms = max(0.0, (time.time() - float(leased.next_run_at)) * 1000.0)
                observe_ms("ingestion_queue_lag_ms", lag_ms)
                inc_counter("ingestion_jobs_started_total")
                started = loop.time()

                ok = False
                retryable = False
                error_message = ""
                try:
                    ok, retryable = await self._processor(leased.payload)
                    if ok:
                        await self._queue.mark_completed(leased.job_id)
                        inc_counter("ingestion_jobs_completed_total")
                    else:
                        error_message = "processor_reported_failure"
                        await self._handle_failed_job(
                            job_id=leased.job_id,
                            attempt=leased.attempt,
                            max_retries=leased.max_retries,
                            retryable=retryable,
                            error_message=error_message,
                        )
                except Exception as exc:
                    error_message = f"{type(exc).__name__}: {exc}"
                    logger.exception("Durable worker job failed: job_id=%s", leased.job_id)
                    await self._handle_failed_job(
                        job_id=leased.job_id,
                        attempt=leased.attempt,
                        max_retries=leased.max_retries,
                        retryable=True,
                        error_message=error_message,
                    )
                finally:
                    observe_ms(
                        "ingestion_job_duration_ms",
                        (loop.time() - started) * 1000.0,
                    )
                    if error_message:
                        logger.warning(
                            "Ingestion job attempt finished with error: job_id=%s attempt=%d/%d retryable=%s error=%s",
                            leased.job_id,
                            leased.attempt,
                            leased.max_retries,
                            retryable,
                            error_message,
                        )
                    self._current_job_id = None
            except asyncio.CancelledError:
                raise
            except Exception:
                inc_counter("ingestion_worker_errors_total")
                logger.exception("Durable ingestion worker loop error")
                await asyncio.sleep(max(0.05, self._config.poll_interval_seconds))

    async def _handle_failed_job(
        self,
        *,
        job_id: str,
        attempt: int,
        max_retries: int,
        retryable: bool,
        error_message: str,
    ) -> None:
        retry_budget = max(1, int(max_retries))
        should_retry = bool(retryable and int(attempt) < retry_budget)
        if should_retry:
            delay_seconds = self._retry_delay(attempt)
            await self._queue.mark_retry(
                job_id=job_id,
                error_message=error_message,
                delay_seconds=delay_seconds,
            )
            inc_counter("ingestion_jobs_retried_total")
            observe_ingestion_retry()
            observe_ms("ingestion_retry_delay_ms", delay_seconds * 1000.0)
            return

        await self._queue.mark_dead_letter(
            job_id=job_id,
            error_message=error_message,
        )
        inc_counter("ingestion_jobs_dead_letter_total")

    def _retry_delay(self, attempt: int) -> float:
        exp = max(0, int(attempt) - 1)
        delay = float(self._config.retry_base_seconds) * (2 ** exp)
        return min(float(self._config.retry_max_seconds), delay)

    def _publish_queue_gauges(self, stats: IngestionQueueStats) -> None:
        set_gauge("ingestion_queue_depth", float(stats.queued))
        set_gauge("ingestion_queue_processing", float(stats.processing))
        set_gauge("ingestion_dead_letter_depth", float(stats.dead_letter))
        set_gauge("ingestion_queue_lag_seconds", float(stats.lag_seconds))
        if stats.last_heartbeat_age_seconds is not None:
            set_gauge("ingestion_worker_heartbeat_age_seconds", float(stats.last_heartbeat_age_seconds))
        set_ingestion_queue_snapshot(
            depth=float(stats.queued),
            processing=float(stats.processing),
            dead_letter_depth=float(stats.dead_letter),
            lag_seconds=float(stats.lag_seconds),
            heartbeat_age_seconds=(
                float(stats.last_heartbeat_age_seconds)
                if stats.last_heartbeat_age_seconds is not None
                else None
            ),
        )

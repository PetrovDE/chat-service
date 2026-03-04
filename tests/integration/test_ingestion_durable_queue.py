import asyncio
import time
from pathlib import Path

from app.services.ingestion import (
    DurableIngestionWorker,
    IngestionJobPayload,
    IngestionWorkerConfig,
    SqliteIngestionQueueAdapter,
)


def test_durable_queue_idempotency_and_terminal_requeue(tmp_path: Path):
    db_path = tmp_path / "ingestion_jobs.sqlite3"

    async def scenario():
        queue = SqliteIngestionQueueAdapter(db_path)
        payload = IngestionJobPayload(
            file_id="f-1",
            file_path=str(tmp_path / "f-1.txt"),
            embedding_mode="local",
            embedding_model="nomic",
        )

        first = await queue.enqueue(
            payload=payload,
            idempotency_key="file:f-1:local:nomic",
            max_retries=3,
            allow_requeue_terminal=True,
        )
        second = await queue.enqueue(
            payload=payload,
            idempotency_key="file:f-1:local:nomic",
            max_retries=3,
            allow_requeue_terminal=True,
        )
        assert first.job_id == second.job_id
        assert second.deduplicated is True

        leased = await queue.acquire(worker_id="worker-a", lease_seconds=1.0)
        assert leased is not None
        await queue.mark_completed(leased.job_id)

        third = await queue.enqueue(
            payload=payload,
            idempotency_key="file:f-1:local:nomic",
            max_retries=3,
            allow_requeue_terminal=True,
        )
        assert third.job_id == first.job_id
        assert third.deduplicated is False
        assert third.status == "queued"

    asyncio.run(scenario())


def test_durable_queue_replay_after_worker_restart(tmp_path: Path):
    db_path = tmp_path / "ingestion_jobs.sqlite3"

    async def scenario():
        queue_a = SqliteIngestionQueueAdapter(db_path)
        payload = IngestionJobPayload(
            file_id="f-2",
            file_path=str(tmp_path / "f-2.txt"),
            embedding_mode="aihub",
            embedding_model="embed-v1",
        )
        await queue_a.enqueue(
            payload=payload,
            idempotency_key="file:f-2:aihub:embed-v1",
            max_retries=3,
            allow_requeue_terminal=True,
        )

        leased_a = await queue_a.acquire(worker_id="worker-a", lease_seconds=0.05)
        assert leased_a is not None

        await asyncio.sleep(0.08)
        queue_b = SqliteIngestionQueueAdapter(db_path)
        recovered = await queue_b.requeue_expired_leases()
        assert recovered == 1

        leased_b = await queue_b.acquire(worker_id="worker-b", lease_seconds=1.0)
        assert leased_b is not None
        assert leased_b.job_id == leased_a.job_id
        assert leased_b.attempt == leased_a.attempt + 1

    asyncio.run(scenario())


def test_durable_worker_retry_then_dead_letter(tmp_path: Path):
    db_path = tmp_path / "ingestion_jobs.sqlite3"
    attempts = {"count": 0}

    async def failing_processor(_payload: IngestionJobPayload):
        attempts["count"] += 1
        return False, True

    async def scenario():
        queue = SqliteIngestionQueueAdapter(db_path)
        worker = DurableIngestionWorker(
            queue=queue,
            processor=failing_processor,
            config=IngestionWorkerConfig(
                worker_id="worker-retry",
                lease_seconds=1.0,
                poll_interval_seconds=0.01,
                heartbeat_interval_seconds=0.05,
                retry_base_seconds=0.01,
                retry_max_seconds=0.02,
            ),
        )
        await worker.start()
        try:
            await worker.enqueue(
                payload=IngestionJobPayload(
                    file_id="f-3",
                    file_path=str(tmp_path / "f-3.txt"),
                    embedding_mode="local",
                    embedding_model="nomic",
                ),
                idempotency_key="file:f-3:local:nomic",
                max_retries=2,
                allow_requeue_terminal=True,
            )

            deadline = time.monotonic() + 2.0
            dead_letter = 0
            while time.monotonic() < deadline:
                stats = await queue.get_stats(worker_id="worker-retry")
                dead_letter = stats.dead_letter
                if dead_letter >= 1:
                    break
                await asyncio.sleep(0.02)

            assert dead_letter == 1
            assert attempts["count"] == 2
        finally:
            await worker.stop(timeout_seconds=1.0)

    asyncio.run(scenario())

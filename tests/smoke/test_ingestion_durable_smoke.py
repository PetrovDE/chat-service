import asyncio
import time
from pathlib import Path

from app.services.ingestion import (
    DurableIngestionWorker,
    IngestionJobPayload,
    IngestionWorkerConfig,
    SqliteIngestionQueueAdapter,
)


def test_durable_worker_smoke_series_files(tmp_path: Path):
    db_path = tmp_path / "ingestion_smoke.sqlite3"
    total_jobs = 24
    processed = []

    async def processor(payload: IngestionJobPayload):
        processed.append(payload.file_id)
        await asyncio.sleep(0.001)
        return True, False

    async def scenario():
        queue = SqliteIngestionQueueAdapter(db_path)
        worker = DurableIngestionWorker(
            queue=queue,
            processor=processor,
            config=IngestionWorkerConfig(
                worker_id="worker-smoke",
                lease_seconds=2.0,
                poll_interval_seconds=0.01,
                heartbeat_interval_seconds=0.05,
                retry_base_seconds=0.01,
                retry_max_seconds=0.05,
            ),
        )

        await worker.start()
        try:
            for idx in range(total_jobs):
                await worker.enqueue(
                    payload=IngestionJobPayload(
                        file_id=f"file-{idx}",
                        file_path=str(tmp_path / f"file-{idx}.txt"),
                        embedding_mode="local",
                        embedding_model="nomic",
                    ),
                    idempotency_key=f"smoke:file-{idx}",
                    max_retries=2,
                    allow_requeue_terminal=True,
                )

            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                stats = await queue.get_stats(worker_id="worker-smoke")
                if len(processed) >= total_jobs and stats.queued == 0 and stats.processing == 0:
                    break
                await asyncio.sleep(0.02)

            final_stats = await queue.get_stats(worker_id="worker-smoke")
            assert len(processed) == total_jobs
            assert final_stats.dead_letter == 0
            assert final_stats.queued == 0
            assert final_stats.processing == 0
        finally:
            await worker.stop(timeout_seconds=1.0)

    asyncio.run(scenario())

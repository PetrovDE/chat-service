# ADR-006: Durable Ingestion Execution via SQLite Queue Adapter

Date: 2026-03-04

## What
- Replaced in-process `asyncio.Queue` ingestion execution with a durable queue/worker runtime.
- Introduced explicit adapter boundary:
  - `IngestionQueueAdapter` interface,
  - `SqliteIngestionQueueAdapter` implementation,
  - `DurableIngestionWorker` orchestration layer.
- Added idempotent enqueue contract via deterministic `idempotency_key`.
- Added retry policy with exponential backoff and dead-letter state.
- Added restart recovery for expired leases (`processing` jobs are re-queued).
- Added queue health/lag/depth telemetry and worker heartbeat metrics.

## Why
- In-memory queue lost pending jobs on API process restart.
- P2 baseline requires durable execution, idempotency, replay safety, and operational metrics.
- SQLite is available in the closed contour and does not require external broker rollout for this phase.

## Trade-offs
- SQLite queue provides durability and single-node simplicity, but lower horizontal throughput than Redis-based brokers.
- Leased processing adds state complexity (lease expiry and replay behavior).
- Idempotency is now centered on job keys; reprocessing terminal jobs intentionally reuses and reopens the same logical key.

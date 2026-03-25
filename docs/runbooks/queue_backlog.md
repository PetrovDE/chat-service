# Runbook: Queue Backlog

## Triggers
- High `llama_service_ingestion_queue_depth`.
- Growing `llama_service_ingestion_queue_lag_seconds`.
- Rising dead-letter depth.

## Diagnostics
1. Inspect worker heartbeat age and `worker_running` status.
2. Check per-stage ingestion timings (`ingestion_stage_ms`, `ingestion_total_ms`).
3. Verify file mix (large PDFs/XLSX) and embedding provider latency.

## Immediate Actions
1. Ensure worker is alive and not stuck.
2. Drain dead-letter root causes (file corruption/auth/provider failures).
3. Temporarily throttle uploads if lag threatens SLA.

## Mitigation
- Increase worker throughput knobs cautiously:
- `INGESTION_WORKER_POLL_INTERVAL_SECONDS`
- `EMBEDDING_CONCURRENCY` / `AIHUB_EMBEDDING_CONCURRENCY`
- `INGESTION_MAX_RETRIES`

## Recovery
- Queue depth trends down.
- Lag and dead-letter return to normal bounds.

# Runbook: RAG Degradation

## Triggers
- Drop in retrieval coverage metrics (`llama_service_retrieval_coverage_ratio`).
- Increase in clarification responses for non-ambiguous queries.
- User reports of missing chunks/rows in answers.

## Diagnostics
1. Capture `rag_debug` for failing requests.
2. Validate filters (`file_ids`, `conversation_id`, `user_id`).
3. Check `retrieval_policy.escalation` and `row_escalation`.
4. Inspect `full_file_limit_hit`, `rows_expected_total`, `row_coverage_ratio`.
5. Verify embeddings model/mode mismatch in mixed datasets.

## Immediate Actions
1. Re-run request with `rag_mode=full_file` and debug enabled.
2. Increase retrieval caps within safe bounds.
3. Reprocess suspect files if embeddings/index corruption suspected.

## Mitigation
- Tune dynamic budget and escalation settings.
- Address metadata/filter bugs before widening limits globally.
- For deterministic tabular questions, prefer SQL path.

## Local Embedded Chroma Restart Failures
Symptoms:
- service restart fails during first vector access after startup;
- logs include `Chroma client initialization failed`.

Operational checks:
1. Verify startup log fields: `mode`, `persist_directory`, `lazy_initialized`, `shared_client_created`.
2. Confirm whether persistent mode is active (`VECTORDB_EPHEMERAL_MODE=false`).
3. Inspect the persist path from logs and check whether `chroma.sqlite3` looks stale/corrupted.

Safe recovery for local development:
1. Stop the service.
2. Backup the persist directory.
3. Remove only the problematic persisted store file(s) (for example `chroma.sqlite3`) if corruption is suspected.
4. Restart the service and verify clean startup logs.

Optional fallback mode (local dev only):
- Set `VECTORDB_EPHEMERAL_MODE=true` to start with in-memory Chroma and bypass persisted local store.
- Default remains persistent mode; do not enable ephemeral mode in production-intended environments unless explicitly desired.

## Recovery Criteria
- Coverage ratios return above baseline thresholds.
- No recurring `silent_row_loss_detected` on target workloads.

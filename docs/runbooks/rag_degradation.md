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

## Recovery Criteria
- Coverage ratios return above baseline thresholds.
- No recurring `silent_row_loss_detected` on target workloads.

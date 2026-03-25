# RAG Debugging

## Enable Debug
- Non-stream: `POST /api/v1/chat?debug=true`
- Stream: `POST /api/v1/chat/stream?debug=true`
- Or payload flag: `rag_debug=true`

## Retrieval Inspection
Check fields:
- `retrieval_mode`
- `intent`
- `retrieval_path` (`vector` / `structured`)
- `filters` / `where`
- `retrieval_hits`
- `retrieved_chunks_total`
- `coverage`

## Top-K Inspection
Use:
- `top_chunks`
- `top_chunks_limit`
- `top_chunks_total`
- `top_similarity_scores`
- `avg_score`

For each chunk inspect:
- `file_id`, `doc_id`, `chunk_id`
- `chunk_type`
- `sheet_name`, `row_start`, `row_end`
- `score`
- `preview`

## Vector Search Diagnostics
- Verify metadata filter shape (`file_ids`, `conversation_id`, `user_id`).
- Check mixed embedding groups (`mixed_embedding_groups`).
- Confirm retrieved chunk count vs expected chunk count.
- In full-file mode, inspect `full_file_limit_hit` and max chunk caps.

## Embedding Diagnostics
- `embedding_mode`
- `embedding_model`
- `provider_debug`
- prompt truncation debug from provider:
- `prompt_chars_before`
- `prompt_chars_after`
- `prompt_truncated`

## Row Coverage Diagnostics (tabular/full_file)
Use:
- `rows_expected_total`
- `rows_retrieved_total`
- `rows_used_map_total`
- `rows_used_reduce_total`
- `row_coverage_ratio`
- `silent_row_loss_detected`

## Fast Failure Triage
1. `requires_clarification=true`: planner blocked ambiguous metric query.
2. `retrieval_hits=0`: filter mismatch or embedding/vector gap.
3. `coverage.ratio` low: escalation not enough; inspect `retrieval_policy.escalation`.
4. `tabular_sql` error code present: guardrails/timeout/result limits triggered.
5. `model_route=ollama_fallback`: AI HUB outage + policy allowed fallback.
6. `retrieval_path=structured`: answer came from deterministic tabular SQL path, not vector search.

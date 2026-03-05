# Changelog

## [Unreleased] - 2026-03-03

### Documentation
- Added architecture study baseline for production LLM chat + file analytics: 
  - `docs/11_llm_file_chat_best_practices_architecture.md`
  - README section `Target architecture direction` with implementation focus points.
- Reworked architecture baseline for closed contour operation:
  - `AI HUB` defined as primary model runtime,
  - `Ollama` restricted to emergency fallback-by-policy,
  - added implementation prompt pack: `docs/12_codex_cursor_prompts_offline_architecture.md`.
- Added P0 architecture artifact:
  - `docs/13_offline_refactor_gap_analysis.md` (as-is map, target gaps, prioritized backlog P0/P1/P2, epic DoD and test strategy).
- Added ADR for P1 routing implementation:
  - `docs/adr/ADR-005-model-router-aihub-first.md`.

### Backend LLM Routing (2026-03-04)
- Implemented strict AI HUB-first `ModelRouter` with policy-aware emergency fallback to `Ollama`.
- Added configurable AI HUB circuit breaker (`closed/open/half-open`) with rolling window thresholds.
- Added fallback policy contract for outage + urgency (`cannot_wait`/`sla_tier=critical`) + restricted policy classes.
- Added route telemetry fields in chat responses and SSE events:
  - `model_route`,
  - `fallback_reason`,
  - `fallback_allowed`,
  - `fallback_policy_version`.
- Added request policy controls in chat schema:
  - `cannot_wait`,
  - `sla_tier`,
  - `policy_class`.
- Updated chat API examples in:
  - `docs/examples/chat.request.json`,
  - `docs/examples/chat.response.json`.
- Switched default model source to `aihub` for new routing baseline.

### Backend Ingestion Durability (2026-03-04)
- Replaced in-process ingestion queue in `app/services/file.py` with durable execution runtime.
- Added adapter-based ingestion queue layer:
  - `app/services/ingestion/contracts.py` (queue/worker contracts),
  - `app/services/ingestion/sqlite_queue.py` (SQLite durable queue),
  - `app/services/ingestion/worker.py` (lease/retry/dead-letter worker).
- Implemented idempotent enqueue and dedup by deterministic ingestion `idempotency_key`.
- Implemented retry with exponential backoff and dead-letter handling in worker runtime.
- Added restart recovery for expired leases and startup replay safety for pending/processing files.
- Added queue/worker observability metrics:
  - depth/lag/heartbeat/dead-letter gauges,
  - enqueue/dedup/retry/dead-letter/recovery counters.
- Added tests for restart/replay/idempotency and durable worker smoke load:
  - `tests/integration/test_ingestion_durable_queue.py`,
  - `tests/smoke/test_ingestion_durable_smoke.py`.
- Added ADR:
  - `docs/adr/ADR-006-durable-ingestion-sqlite-queue.md`.

### Backend Tabular Runtime Migration (2026-03-04)
- Migrated deterministic tabular runtime from per-file SQLite sidecar to shared DuckDB/Parquet adapter.
- Added modular tabular architecture:
  - `app/services/tabular/normalization.py` (ingestion normalization),
  - `app/services/tabular/storage_adapter.py` (shared dataset storage + version catalog),
  - `app/services/tabular/sql_execution.py` (runtime SQL execution/session),
  - `app/services/tabular/sql_guardrails.py` (bounded SQL guardrails).
- Ingestion now stores `custom_metadata.tabular_dataset` with reproducibility metadata:
  - `dataset_id`, `dataset_version`, `dataset_provenance_id`,
  - per-table `table_version`, `provenance_id`, `parquet_path`.
- `app/services/chat/tabular_sql.py` updated to use new runtime while preserving deterministic aggregate/profile paths.
- Added SQL/path telemetry and row-coverage diagnostics for new runtime in tabular debug payload.
- Added migration compatibility fallback for legacy `custom_metadata.tabular_sidecar` SQLite artifacts (read path + cleanup).
- Updated file delete cleanup to remove shared tabular artifacts and legacy sidecars best effort.
- Added tests for migration coverage:
  - `tests/integration/test_tabular_runtime_migration.py` (versioning, reproducibility, legacy compatibility).
- Added ADR:
  - `docs/adr/ADR-007-tabular-runtime-duckdb-parquet.md`.

### Backend Query Planner Split (2026-03-04)
- Added dedicated domain query planner module:
  - `app/domain/chat/query_planner.py`.
- Planner now returns structured decision contract:
  - `route`,
  - `intent`,
  - `confidence`,
  - `requires_clarification`,
  - `reason_codes`.
- `rag_prompt_builder` now uses planner-first routing for deterministic vs narrative path selection.
- Added clarification-only flow for metric-critical ambiguous prompts (no retrieval/SQL guessing path).
- Added orchestrator short-circuit so clarification decisions return directly without LLM generation.
- Added planner observability counters:
  - `query_planner_decision_total`,
  - `query_planner_reason_total`.
- Added tests:
  - `tests/unit/test_query_planner.py`,
  - `tests/integration/test_query_planner_contract.py`,
  - regression update in `tests/integration/test_rag_integration.py`.
- Added ADR:
  - `docs/adr/ADR-008-query-planner-deterministic-vs-narrative.md`.

### Backend Deterministic SQL Guardrails (2026-03-04)
- Hardened deterministic tabular SQL path with explicit guardrail policy and bounded execution.
- Added typed SQL error classification codes for API/debug:
  - `sql_guardrail_blocked`,
  - `sql_scan_limit_exceeded`,
  - `sql_result_limit_exceeded`,
  - `sql_result_size_exceeded`,
  - `sql_timeout`,
  - `sql_execution_failed`.
- Extended SQL policy checks:
  - strict allowlist for `SELECT/WITH`,
  - blocked dangerous SQL keywords/patterns,
  - bounded scanned rows pre-check,
  - bounded result rows/bytes.
- Added deterministic trace fields in `rag_debug.tabular_sql`:
  - `executed_sql`,
  - `policy_decision`,
  - `guardrail_flags`.
- Deterministic SQL failures are now surfaced as classified `tabular_sql` error payloads (instead of silent fallback).
- Added tests for required P5 scenarios:
  - `tests/unit/test_sql_guardrails.py`,
  - `tests/integration/test_tabular_sql_guardrails.py`.
- Added ADR:
  - `docs/adr/ADR-009-sql-guardrails-bounded-execution.md`.

### Backend Observability + SLO Instrumentation (2026-03-04)
- Added canonical SLO metrics namespace `llama_service_*` for offline AI HUB-first contour.
- Added route observability metrics for AI HUB primary vs Ollama fallback decisions:
  - `llama_service_llm_route_decisions_total`,
  - `llama_service_llm_fallback_total`.
- Added planner route-class metrics (`deterministic` / `narrative`):
  - `llama_service_query_planner_route_total`.
- Added ingestion reliability metrics for queue health and retries:
  - `llama_service_ingestion_enqueue_total`,
  - `llama_service_ingestion_retries_total`,
  - `llama_service_ingestion_queue_depth`,
  - `llama_service_ingestion_queue_lag_seconds`,
  - `llama_service_ingestion_dead_letter_depth`,
  - `llama_service_ingestion_queue_processing`,
  - `llama_service_ingestion_worker_heartbeat_age_seconds`.
- Added retrieval and tabular row coverage metrics:
  - `llama_service_retrieval_coverage_ratio`,
  - `llama_service_retrieval_coverage_events_total`,
  - `llama_service_tabular_row_coverage_ratio`,
  - `llama_service_tabular_row_coverage_events_total`.
- Added observability artifact:
  - `docs/14_observability_slo_offline.md` (metric contract, SLOs, alert rules, rollout checklist).

### Backend Eval Framework (P7, 2026-03-05)
- Added offline golden datasets for AI HUB-first eval pipeline:
  - `tests/evals/datasets/tabular_aggregate_golden.jsonl`,
  - `tests/evals/datasets/tabular_profile_golden.jsonl`,
  - `tests/evals/datasets/narrative_rag_golden.jsonl`,
  - `tests/evals/datasets/fallback_route_golden.jsonl`.
- Added eval runner stack:
  - `scripts/evals/runner.py`,
  - `scripts/evals/offline.py`,
  - `scripts/evals/online.py`,
  - `scripts/evals/datasets.py`.
- Added CI gate engine and CLI entrypoints:
  - `scripts/evals/ci_gates.py`,
  - `scripts/evals/run_eval_suite.py`,
  - `scripts/evals/run_ci_gates.py`.
- Added threshold baseline config:
  - `tests/evals/gates.json`.
- Added eval tests:
  - `tests/evals/test_eval_datasets_contract.py`,
  - `tests/evals/test_eval_runner_offline.py`,
  - `tests/evals/test_ci_gates.py`.
- Added P7 artifact:
  - `docs/15_eval_framework_offline.md`.

### Pre-prod Hardening (P8, 2026-03-05)
- Added end-to-end pre-prod outage/recovery drill test:
  - `tests/e2e/test_preprod_aihub_fallback_recovery.py`
  - validates `AI HUB outage -> controlled fallback -> AI HUB recovery -> fallback mode closed`.
- Added pre-prod operational runbooks:
  - `docs/runbooks/aihub_incident.md`,
  - `docs/runbooks/fallback_surge.md`,
  - `docs/runbooks/queue_backlog.md`,
  - `docs/runbooks/degraded_mode.md`.
- Added readiness artifact:
  - `docs/16_preprod_readiness_report.md` (DoD check, blockers/non-blockers, final gate recommendations).
- Added ADR for local verification strategy without direct AI HUB access:
  - `docs/adr/ADR-010-preprod-hardening-verification-strategy.md`.

### Ð˜Ð·Ð¼ÐµÐ½ÐµÐ½Ð¾
- Frontend composer: Ð¿Ð¾Ð»Ðµ Ð²Ð²Ð¾Ð´Ð° Ð¾ÑÑ‚Ð°Ð²Ð»ÐµÐ½Ð¾ Ð¾Ñ‚Ð´ÐµÐ»ÑŒÐ½Ð¾Ð¹ Ð²ÐµÑ€Ñ…Ð½ÐµÐ¹ ÑÑ‚Ñ€Ð¾ÐºÐ¾Ð¹; Ð½Ð¸Ð¶Ð½ÑÑ ÑÑ‚Ñ€Ð¾ÐºÐ° ÐºÐ¾Ð½Ñ‚Ñ€Ð¾Ð»Ð¾Ð² Ð¿Ñ€Ð¸Ð²ÐµÐ´ÐµÐ½Ð° Ðº Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ `File + provider + model + RAG mode + Send`.
- ÐŸÐ¾Ð´ ÑÐµÐ»ÐµÐºÑ‚Ð°Ð¼Ð¸ provider/model/rag mode Ð´Ð¾Ð±Ð°Ð²Ð»ÐµÐ½Ñ‹ inline-Ð¿Ð¾Ð´ÑÐºÐ°Ð·ÐºÐ¸ (`ÐŸÑ€Ð¾Ð²Ð°Ð¹Ð´ÐµÑ€ AI`, `Ð’Ñ‹Ð±Ð¾Ñ€ Ð¼Ð¾Ð´ÐµÐ»Ð¸`, `Ð ÐµÐ¶Ð¸Ð¼ Ñ€Ð°Ð±Ð¾Ñ‚Ñ‹ Ñ Ð´Ð¾ÐºÑƒÐ¼ÐµÐ½Ñ‚Ð°Ð¼Ð¸`).
- `Logout` Ð² `Settings` Ð¿ÐµÑ€ÐµÐ½ÐµÑÑ‘Ð½ Ð² Ñ„ÑƒÑ‚ÐµÑ€ Ð¸ Ð¾Ñ„Ð¾Ñ€Ð¼Ð»ÐµÐ½ ÐºÐ°Ðº danger-ÐºÐ½Ð¾Ð¿ÐºÐ° Ñ€ÑÐ´Ð¾Ð¼ Ñ `Save`.

### Ð˜ÑÐ¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾
- Ð’ sidebar Ñ‡Ð°Ñ‚Ð¾Ð² `Del` Ð·Ð°Ð¼ÐµÐ½Ñ‘Ð½ Ð½Ð° Ð¾Ñ‚Ð´ÐµÐ»ÑŒÐ½ÑƒÑŽ Ð·Ð°Ð¼ÐµÑ‚Ð½ÑƒÑŽ ÐºÐ½Ð¾Ð¿ÐºÑƒ ÑƒÐ´Ð°Ð»ÐµÐ½Ð¸Ñ Ñ Ð¸ÐºÐ¾Ð½ÐºÐ¾Ð¹ ÐºÐ¾Ñ€Ð·Ð¸Ð½Ñ‹ ÑÐ¿Ñ€Ð°Ð²Ð° Ð¾Ñ‚ ÑÑ‚Ñ€Ð¾ÐºÐ¸ Ñ‡Ð°Ñ‚Ð°.
- Ð‘Ð»Ð¾Ðº `RAG debug` Ð¿Ð¾Ð´ ÑÐ¾Ð¾Ð±Ñ‰ÐµÐ½Ð¸ÐµÐ¼ Ð±Ð¾Ð»ÑŒÑˆÐµ Ð½Ðµ Ð¾Ñ‚Ð¾Ð±Ñ€Ð°Ð¶Ð°ÐµÑ‚ÑÑ Ð¿Ñ€Ð¸ Ð²Ñ‹ÐºÐ»ÑŽÑ‡ÐµÐ½Ð½Ð¾Ð¼ Ñ„Ð»Ð°Ð³Ðµ: debug-Ð¼ÐµÑ‚Ð° Ð¿Ð¾ÐºÐ°Ð·Ñ‹Ð²Ð°ÐµÑ‚ÑÑ Ñ‚Ð¾Ð»ÑŒÐºÐ¾ ÐµÑÐ»Ð¸ Ð·Ð°Ð¿Ñ€Ð¾Ñ Ð¾Ñ‚Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½ Ñ `rag_debug=true`.

### Backend RAG (2026-03-03)
- Stage 0 diagnostics:
  - added row-level debug fields: `rows_expected_total`, `rows_retrieved_total`, `rows_used_map_total`, `rows_used_reduce_total`, `row_coverage_ratio`;
  - added full-file per-batch diagnostics: `batch_rows_start_end`, `batch_input_chars`, `batch_output_chars`;
  - added AIHub prompt trim telemetry in debug: `prompt_chars_before`, `prompt_chars_after`, `prompt_truncated`.
- Stage 1 ingestion:
  - `xlsx/xls/csv` loader switched to adaptive row-dense chunking (`XLSX_CHUNK_MAX_CHARS`, `XLSX_CHUNK_MAX_ROWS`);
  - added wide-sheet column pruning (`XLSX_MAX_COLUMNS_PER_CHUNK`) with full column set in metadata.
- Stage 2 coverage safeguards:
  - added full-file row-coverage repass (`RAG_FULL_FILE_MIN_ROW_COVERAGE`, `RAG_FULL_FILE_ESCALATION_MAX_CHUNKS`);
  - added `silent_row_loss_detected` signal when chunk coverage is high but row coverage is low.
- Stage 3 full-file answer quality:
  - map step now returns structured JSON (`facts`, `aggregates`, `row_ranges_covered`, `missing_data`);
  - reduce step merges structured payload deterministically (`strategy=structured_map_reduce`);
  - added setting `FULL_FILE_MAP_MAX_TOKENS`.
- Stage 4 deterministic tabular path:
  - ingestion creates sidecar SQLite dataset for `xlsx/xls/csv` (`custom_metadata.tabular_sidecar`);
  - aggregate intents route to `retrieval_mode=tabular_sql` (LangChain SQL tool execution), LLM used only for presentation;
  - added `tabular_profile` intent path for broad analytical prompts (`per-column stats/metrics`, `Ð¾Ð±Ñ‰Ð¸Ð¹ Ð°Ð½Ð°Ð»Ð¸Ð· Ñ„Ð°Ð¹Ð»Ð°`);
  - file delete now cleans sidecar path best effort.
- Embedding robustness:
  - replaced lossy truncation of long local/Ollama embedding inputs with segmentation + overlap + mean pooling (`OLLAMA_EMBED_MAX_INPUT_CHARS`, `OLLAMA_EMBED_SEGMENT_OVERLAP_CHARS`);
  - improved Ollama `/api/embed` HTTP 400 diagnostics in logs.
- XLSX serialization:
  - long cells are preserved via continuation lines (`Row N (cont)`) and optional cap `XLSX_CELL_MAX_CHARS` (default `0`, no cap).
- `full_file` retrieval: removed post-retrieval squeeze to hybrid limit (`top_k*4`); full retrieved set is preserved within `RAG_FULL_FILE_MAX_CHUNKS`.
- Added full-file coverage diagnostics in debug: `retrieved_chunks_total`, `coverage.expected_chunks`, `coverage.retrieved_chunks`, `coverage.ratio`, `coverage.complete`.
- Added explicit incomplete-coverage signal: when `retrieved < expected`, `truncated=true` and caveat about incomplete analysis.
- Extended tabular debug/source metadata: `row_start`, `row_end`, `total_rows`; sources now include `rows=<start>-<end>` when available.
- Updated full-file map-reduce prompts for table batches to preserve row ranges and numeric signals (outliers/trends).
- Increased default `FULL_FILE_MAP_BATCH_MAX_CHARS` to `25000` to reduce full-file map/reduce latency on large spreadsheets.
- Added response language policy: answer language follows user query language (RU->RU, EN->EN) with post-generation language rewrite fallback.
- Added coverage-based full-file source aggregation (`sheet + merged row ranges`) to avoid references to only the last chunk.
- Stabilized Excel/CSV ingestion for embeddings: reduced row-block size to compact chunks (up to 20 rows) to prevent provider `400` on oversized inputs.
- Added dynamic retrieval budget policy for `auto/hybrid`:
  - short query -> ~20% from known document chunks,
  - fact query -> ~10%,
  - broad/analysis query -> ~30%.
- Added low-coverage one-step escalation with debug trace (`rag_debug.retrieval_policy.escalation`):
  - increase `top_k` or
  - switch to `full_file` for small documents.
- Fixed context merge dedup stability for tabular chunks:
  - dedup key in retrieval context now uses `chunk_id` (fallback: `file_id + chunk_index`) and no longer uses text-prefix/header similarity,
  - prevents false collapse of Excel chunks with identical repeated headers (`=== ... EXCEL | SHEET ... ===`).
- Split metadata semantics for IDs:
  - `doc_id` now represents the whole document (`file_id`),
  - `chunk_id` represents the exact chunk (`<file_id>_<chunk_index>`).
- Added regression tests:
  - `test_xlsx_wide_sheet_chunking_adaptive`
  - `test_full_file_row_coverage_debug_fields`
  - `test_full_file_map_reduce_structured_preserves_ranges`
  - `test_aihub_prompt_truncation_debug_visible`
  - `test_tabular_intent_routes_to_sql_path`
  - `test_local_embedding_inputs_are_segmented_without_lossy_truncation`
  - `test_xlsx_long_cell_not_lossy_truncated`
  - `test_tabular_profile_intent_has_priority_over_aggregate_keywords`

### Backend Refactor (2026-03-03)
- Decomposed `app/services/chat_orchestrator.py` into dedicated modules under `app/services/chat/*`.
- Kept API/SSE contracts unchanged while moving language policy, RAG prompt builder, full-file map/reduce, debug/source formatting and post-processing logic out of the facade.
- Migrated integration tests to direct imports from `app/services/chat/*` modules and removed legacy private-alias coupling in `chat_orchestrator`.
- Chat orchestration now uses shared private pipeline steps for generation/post-processing to reduce duplication between stream and non-stream flows.

## [1.0.0] - 2025-10-16

### Ð”Ð¾Ð±Ð°Ð²Ð»ÐµÐ½Ð¾
- âœ… Ð¡Ð¸ÑÑ‚ÐµÐ¼Ð° Ð°ÑƒÑ‚ÐµÐ½Ñ‚Ð¸Ñ„Ð¸ÐºÐ°Ñ†Ð¸Ð¸ (JWT)
- âœ… Ð£Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¸Ðµ Ð±ÐµÑÐµÐ´Ð°Ð¼Ð¸
- âœ… Ð˜ÑÑ‚Ð¾Ñ€Ð¸Ñ Ñ‡Ð°Ñ‚Ð¾Ð²
- âœ… Ð˜Ð½Ñ‚ÐµÐ³Ñ€Ð°Ñ†Ð¸Ñ Ñ Ollama
- âœ… Ð˜Ð½Ñ‚ÐµÐ³Ñ€Ð°Ñ†Ð¸Ñ Ñ OpenAI
- âœ… PostgreSQL Ð±Ð°Ð·Ð° Ð´Ð°Ð½Ð½Ñ‹Ñ…
- âœ… ÐœÐ¸Ð³Ñ€Ð°Ñ†Ð¸Ð¸ Alembic
- âœ… API Ð´Ð¾ÐºÑƒÐ¼ÐµÐ½Ñ‚Ð°Ñ†Ð¸Ñ
- âœ… ÐÐ´Ð°Ð¿Ñ‚Ð¸Ð²Ð½Ñ‹Ð¹ UI
- âœ… Ð›Ð¾Ð³Ð¸Ñ€Ð¾Ð²Ð°Ð½Ð¸Ðµ Ð·Ð°Ð¿Ñ€Ð¾ÑÐ¾Ð²

### Ð˜Ð·Ð¼ÐµÐ½ÐµÐ½Ð¾
- ÐœÐ¾Ð´ÑƒÐ»ÑŒÐ½Ð°Ñ ÑÑ‚Ñ€ÑƒÐºÑ‚ÑƒÑ€Ð° frontend
- ÐžÐ¿Ñ‚Ð¸Ð¼Ð¸Ð·Ð°Ñ†Ð¸Ñ SQL Ð·Ð°Ð¿Ñ€Ð¾ÑÐ¾Ð²

### Ð˜ÑÐ¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾
- Ð˜Ð·Ð¾Ð»ÑÑ†Ð¸Ñ Ð´Ð°Ð½Ð½Ñ‹Ñ… Ð¿Ð¾Ð»ÑŒÐ·Ð¾Ð²Ð°Ñ‚ÐµÐ»ÐµÐ¹
- Ð¡Ñ‡Ñ‘Ñ‚Ñ‡Ð¸Ðº ÑÐ¾Ð¾Ð±Ñ‰ÐµÐ½Ð¸Ð¹ Ð² Ð±ÐµÑÐµÐ´Ð°Ñ…




# Changelog

## [Unreleased] - 2026-03-03

### Complex Analytics Two-Pass Pipeline Hardening (2026-03-06)
- Completed backend two-stage generation flow for `complex_analytics`:
  - stage 1: `complex_analytics_plan` (strict JSON plan),
  - stage 2: `complex_analytics_codegen` (Python-only code),
  - stage 3: sandbox execution with artifact validation,
  - stage 4: `complex_analytics_response` composition from execution output.
- Enforced safer degradation policy:
  - template fallback is now opt-in (`COMPLEX_ANALYTICS_ALLOW_TEMPLATE_FALLBACK=false` by default),
  - runtime template fallback is also opt-in (`COMPLEX_ANALYTICS_ALLOW_TEMPLATE_RUNTIME_FALLBACK=false`),
  - failed codegen/validation returns reason-specific clarification (`codegen_failed`, `missing_required_artifacts`, `validation_failed`).
- Sandbox runtime hardening:
  - expanded blocked import roots (`sys`, `importlib`),
  - added stable Agg backend initialization for matplotlib in sandbox,
  - added validation guard for required visualization artifacts.
- Extended debug/telemetry fields for complex analytics:
  - `complex_analytics_code_generation_prompt_status`,
  - `complex_analytics_code_generation_source`,
  - `complex_analytics_codegen.provider`,
  - `complex_analytics.sandbox.secure_eval`.
- Added structured logs for pipeline stages:
  - `complex_analytics.codegen_plan`,
  - `complex_analytics.codegen_execute`,
  - `complex_analytics.compose`.
- Updated tests to reflect two-pass plan/code architecture and explicit fallback policy:
  - `tests/unit/test_complex_analytics_executor.py`,
  - `tests/integration/test_complex_analytics_path.py`,
  - `tests/smoke/test_complex_analytics_smoke.py`.

Migration note:
- `ChatResponse.executor_status` now allows additional value: `fallback`.
- `complex_analytics` template fallback behavior changed from default-on to opt-in via settings above.

### Complex Analytics Dynamic Codegen (2026-03-05)
- Added LLM-assisted Python code generation stage for `complex_analytics`:
  - prompt includes user task + dataframe profile,
  - generated code is parsed and validated against sandbox contract before execution,
  - supports request-specific dependency analysis/heatmap generation scenarios.
- Added strict stability fallback:
  - invalid/unsafe generated code falls back to template executor,
  - runtime failures in generated code trigger controlled fallback (`template_runtime_fallback`) instead of user-visible hard failure.
- Added richer report layer:
  - `insights` section in formatted analytics response (RU/EN),
  - dependency-focused fallback visualizations for relevant queries.
- Added codegen observability:
  - `complex_analytics_codegen_total{status,reason}` counter,
  - debug metadata: `complex_analytics.code_source` + `complex_analytics.codegen`.
- Routing integration:
  - `build_rag_prompt` now forwards `model_source/provider_mode/model_name` into complex analytics executor,
  - codegen defaults to forced local provider (`COMPLEX_ANALYTICS_CODEGEN_FORCE_LOCAL=true`) for offline constraints.
- Added regression coverage:
  - unit tests for codegen success/fallback contract,
  - integration test for dependency-request codegen path with heatmap artifact,
  - preserved deterministic SQL and short-circuit behavior regressions.

### Complex Analytics Hardening & Hygiene (2026-03-05)
- Hardened sandbox security checks:
  - blocked dynamic bypass primitives (`getattr/setattr/delattr/globals/locals/vars`),
  - blocked dunder attribute access.
- Artifact response sanitization:
  - `artifacts.path` is now relative (`uploads/...`),
  - no absolute filesystem paths in response text/payloads.
- Added retention cleanup for `uploads/complex_analytics` runs:
  - TTL-based cleanup (`COMPLEX_ANALYTICS_ARTIFACT_TTL_HOURS`),
  - max-run-directories cap (`COMPLEX_ANALYTICS_ARTIFACT_MAX_RUN_DIRS`).
- Added observability counters for complex analytics quality:
  - `complex_analytics_executor_success_total`,
  - `complex_analytics_artifacts_generated_total`,
  - `complex_analytics_artifact_kind_total`,
  - `complex_analytics_artifacts_cleanup_total`.
- Improved RU report localization for numeric/date labels and common note messages.
- Added regression tests:
  - stream request-id coverage (`/api/v1/chat/stream`),
  - sandbox security bypass blocking,
  - no datetime-infer warning noise,
  - metrics emission checks.

### Request ID Logging Consistency Fix (2026-03-05)
- Fixed access-log correlation in `app.main` so request completion/error logs no longer emit `rid=-` during normal request flow.
- `x-request-id` is now consistently resolved for access logs from:
  - response header set by middleware,
  - current request context,
  - incoming request header fallback,
  - generated UUID fallback.
- Added smoke coverage:
  - response contains `x-request-id`,
  - incoming `x-request-id` is echoed,
  - `app.main` access log records include non-empty `request_id`.

### Complex Analytics Report Localization Fix (2026-03-05)
- Fixed RU report formatting for `complex_analytics` short-circuit responses:
  - removed mojibake/garbled section labels,
  - ensured RU query returns RU report structure and localized field semantics.
- Added localization mapping for common analytics field semantics (`purpose_hint`, process context).
- Added regression coverage:
  - `tests/unit/test_complex_analytics_executor.py::test_russian_report_is_localized_without_mojibake`.
- Updated examples:
  - `docs/examples/chat.request.complex_analytics.ru.json`,
  - `docs/examples/chat.response.complex_analytics.ru.json`.

### Complex Analytics Chart Rendering UX (2026-03-05)
- Exposed complex analytics artifacts as browser-accessible URLs under `/uploads/*` (static mount in `app/main.py`).
- Added artifact URL derivation in `app/services/chat/complex_analytics.py` and embedded chart markdown in report text:
  - `![chart](/uploads/...)`
- Extended streaming and non-streaming chat payloads to include artifact metadata list:
  - SSE `done.artifacts[]`
  - `ChatResponse.artifacts[]` (optional, non-breaking)
- Updated frontend chat renderer to show chart previews in assistant metadata gallery (`frontend/static/js/chat-manager.js` + CSS).
- Updated docs/examples for complex analytics response contract and artifact URL semantics.

Migration note:
- `ChatResponse` now may include optional `artifacts` list for complex analytics responses.
- Existing clients ignoring unknown fields remain fully compatible.

### Backend Complex Analytics Executor (2026-03-05)
- Added new planner route `complex_analytics` for requests that require sandboxed Python analytics (`python/pandas/heatmap/NLP/multi-step`).
- Added isolated executor module:
  - `app/services/chat/complex_analytics.py`
  - sandbox AST policy blocks network/system/subprocess imports and calls,
  - bounded timeout/output/artifact limits,
  - artifact persistence in controlled temp contour (`uploads/complex_analytics`),
  - tabular data loading from existing tabular runtime metadata.
- `rag_prompt_builder` now routes complex prompts to executor path (no deterministic SQL guardrail path misuse).
- Added controlled degradation on executor failures:
  - reason-specific clarification responses,
  - no implicit fallback to unsafe execution paths.
- Extended chat telemetry/debug contract with execution-plane fields:
  - `execution_route`,
  - `executor_attempted`,
  - `executor_status`,
  - `executor_error_code`,
  - `artifacts_count`.
- Added structured log enrichment in `chat_route_decision` with execution route + executor status.
- Added tests:
  - `tests/unit/test_complex_analytics_executor.py`,
  - `tests/integration/test_complex_analytics_path.py`,
  - `tests/smoke/test_complex_analytics_smoke.py`,
  - planner and response compatibility updates.
- Added docs/ADR updates:
  - `docs/05_query_planner.md`,
  - `docs/07_llm_routing.md`,
  - `docs/01_architecture_overview.md`,
  - `docs/11_llm_file_chat_best_practices_architecture.md`,
  - `docs/02_api_contracts.md`,
  - `docs/12_architecture_decisions.md`,
  - `docs/adr/ADR-014-complex-analytics-sandbox-executor.md`,
  - `docs/examples/chat.request.complex_analytics.json`,
  - `docs/examples/chat.response.complex_analytics.json`.

Migration note:
- `ChatResponse` now includes additional non-breaking fields:
  - `execution_route` (`tabular_sql|complex_analytics|narrative|clarification|unknown`),
  - `executor_attempted` (bool),
  - `executor_status` (`not_attempted|success|error|timeout|blocked`),
  - `executor_error_code` (nullable string),
  - `artifacts_count` (int).
- Existing routing fields (`model_route`, `provider_effective`, `fallback_*`, `aihub_attempted`) remain unchanged.

### Documentation Reconciliation (2026-03-05)
- Performed full architecture audit and documentation reconciliation against current codebase.
- Added canonical architecture docs:
  - `docs/00_system_overview.md`
  - `docs/01_architecture_overview.md`
  - `docs/02_service_structure.md`
  - `docs/03_rag_pipeline.md`
  - `docs/04_ingestion_pipeline.md`
  - `docs/05_query_planner.md`
  - `docs/06_tabular_runtime.md`
  - `docs/07_llm_routing.md`
  - `docs/08_sql_guardrails.md`
  - `docs/09_observability.md`
  - `docs/10_eval_framework.md`
  - `docs/12_architecture_decisions.md`
  - `docs/rag_debugging.md`
- Added runbooks:
  - `docs/runbooks/aihub_outage.md`
  - `docs/runbooks/rag_degradation.md`
- Updated runbooks:
  - `docs/runbooks/fallback_surge.md`
  - `docs/runbooks/queue_backlog.md`
- Added ADRs (Context/Decision/Consequences format):
  - `docs/adr/ADR-011-architecture-doc-reconciliation.md`
  - `docs/adr/ADR-012-rag-debug-observability-contract.md`
- Marked legacy architecture/planning docs as deprecated and linked them to canonical replacements.

### Backend LLM Routing Fix: Explicit Provider Precedence (2026-03-05)
- Fixed bug where chat requests with UI-selected `local/ollama` still attempted AI HUB route first.
- Added explicit routing mode support:
  - `provider_mode=explicit` -> selected provider only, no cross-provider fallback.
  - `provider_mode=policy` -> AI HUB primary with policy-gated Ollama fallback.
- Implemented precedence chain for provider resolution in chat path:
  - request payload -> conversation state -> server default.
- Enforced local selection behavior:
  - `model_source=local|ollama` always resolves to explicit mode.
  - no AI HUB attempt in this branch (`aihub_attempted=false`).
- Added route observability fields in chat response/SSE telemetry:
  - `route_mode`,
  - `provider_selected`,
  - `provider_effective`,
  - `fallback_attempted`,
  - `aihub_attempted`.
- Added structured chat route decision log event (`chat_route_decision`) with effective route metadata.
- Updated docs:
  - `docs/07_llm_routing.md`,
  - `docs/11_llm_file_chat_best_practices_architecture.md`,
  - `docs/12_architecture_decisions.md`,
  - `docs/adr/ADR-013-provider-selection-precedence-explicit-mode.md`,
  - `docs/02_api_contracts.md`,
  - `docs/examples/chat.request*.json`,
  - `docs/examples/chat.response.json`.
- Added/updated regression tests:
  - `tests/unit/test_model_router_explicit_modes.py`,
  - `tests/integration/test_model_router_fallback.py`,
  - `tests/e2e/test_preprod_aihub_fallback_recovery.py`.

Migration note:
- `ChatMessage.provider_mode` added as optional request field (`explicit|policy`).
- `ChatResponse` and SSE route payload now include additional optional telemetry fields listed above.
- Existing route fields (`model_route`, `fallback_reason`, `fallback_allowed`, `fallback_policy_version`) remain present for compatibility.

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




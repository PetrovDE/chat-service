# Changelog

## [Unreleased] - 2026-03-03

### Complex Analytics Modular Refactor (2026-03-06)
- Refactored `complex_analytics` implementation from monolithic module to modular package:
  - `app/services/chat/complex_analytics/{planner,codegen,sandbox,executor,composer,artifacts,errors,telemetry}.py`
- Continued modular split in the same package to reduce oversized modules and isolate responsibilities:
  - `dataset_context.py` (dataset/table resolution and loading),
  - `template_codegen.py` (safe template analytics code builder),
  - `report_quality.py` (compose quality policy and broad-query detection),
  - `localization.py` (RU localization mappings/helpers),
  - `auto_visual_patch.py` (codegen visualization contract auto-repair),
  - `executor_support.py` (error/context helpers for executor path).
- Reduced large file footprint in complex analytics package:
  - `codegen.py` -> 428 LOC,
  - `executor.py` -> 499 LOC,
  - `composer.py` -> 479 LOC.
- Preserved public import contract and route behavior:
  - `execute_complex_analytics_path(...)`
  - `is_complex_analytics_query(...)`
- Preserved execution semantics:
  - planner routing behavior and no silent downgrade to deterministic SQL,
  - telemetry/debug fields (`execution_route`, `executor_*`, `complex_analytics_*`),
  - provider precedence (`explicit local/provider > default policy`),
  - sandbox/offline security boundaries.
- Added focused module unit tests:
  - `test_complex_analytics_planner_module.py`
  - `test_complex_analytics_codegen_module.py`
  - `test_complex_analytics_sandbox_module.py`
  - `test_complex_analytics_composer_module.py`
  - `test_complex_analytics_import_compat.py`
- Added architecture note:
  - `docs/19_big_file_refactor_complex_analytics.md`
- Adjusted fallback defaults for better UX on broad analytics prompts:
  - `COMPLEX_ANALYTICS_ALLOW_TEMPLATE_FALLBACK=true` (default),
  - `COMPLEX_ANALYTICS_ALLOW_TEMPLATE_RUNTIME_FALLBACK=true` (default),
  - broad requests now degrade to safe template analytics instead of returning `codegen_failed` when LLM codegen contract is not satisfied.
- Added codegen auto-repair for visualization contract:
  - when LLM code misses `save_plot(...)`, backend injects a safe chart fallback block and re-validates contract before final fallback.
- Added debug/telemetry flagging for this repair path:
  - `complex_analytics.codegen_auto_visual_patch_applied`
  - `complex_analytics.complex_analytics_codegen.auto_visual_patch_applied`
- Added regression coverage for broad analytics prompt repair path and telemetry:
  - `tests/integration/test_complex_analytics_path.py::test_complex_analytics_auto_visual_patch_prevents_codegen_failure`
  - `tests/unit/test_complex_analytics_telemetry_module.py`
- Added compose quality gate for final report generation:
  - weak/generic compose responses now fallback to local structured formatter from executed metrics/artifacts,
  - debug telemetry includes `response_status=fallback` and `response_error_code=low_content_quality`.
- Added broad-query compose policy for full-file analysis prompts:
  - when query intent is broad full analysis, executor keeps deterministic local formatter output and skips compose LLM stage by default,
  - debug telemetry uses `response_status=fallback` and `response_error_code=broad_query_local_formatter`.
- Added regression coverage for compose quality fallback:
  - `tests/integration/test_complex_analytics_path.py::test_complex_analytics_compose_quality_gate_falls_back_to_local_formatter`
  - `tests/unit/test_complex_analytics_composer_module.py::test_compose_quality_gate_rejects_too_short_llm_output`
- Added offline eval dataset and runner metric for broad complex analytics response quality:
  - dataset: `tests/evals/datasets/complex_analytics_quality_golden.jsonl`
  - metric: `complex_analytics_report_quality`
  - gate: `complex_analytics_report_quality_min`
- Included latency gate for new eval dataset:
  - `p95_latency_ms.complex_analytics_quality_golden`
- Added online/hybrid preprod quality eval path for real `/api/v1/chat`:
  - dataset: `tests/evals/datasets/complex_analytics_quality_online.jsonl`
  - online metric: `online_report.metrics.complex_analytics_report_quality`
  - preprod gate config: `tests/evals/gates.preprod.json`
  - supports `${ENV_VAR}` placeholders in online requests (e.g. conversation id injection).
- Extended CI gates for online preprod validation:
  - `online_complex_analytics_report_quality_min`
  - `online_max_latency_violations`
  - `online_p95_latency_ms.<dataset>`
- Fixed legacy mojibake sections in changelog and normalized them to English.

Migration note:
- External HTTP/SSE API contract is unchanged.
- Refactor is internal-only; no client migration required.

### Large File Refactor Phase 2: Orchestration/RAG Split (2026-03-06)
- Reduced oversized orchestration files without changing API contracts:
  - `app/services/chat_orchestrator.py`: 884 -> 705 LOC
  - `app/services/chat/rag_prompt_builder.py`: 778 -> 586 LOC
- Added `app/services/chat/orchestrator_helpers.py`:
  - route/execution telemetry builders,
  - artifact extraction and clarification text helpers,
  - generation kwargs and postprocess helper.
- Added `app/services/chat/rag_prompt_routes.py`:
  - route-specific handlers for `clarification`, `complex_analytics`, and `deterministic_analytics` branches.
- Preserved monkeypatch/backward-compat behavior used by tests:
  - `rag_prompt_builder.execute_tabular_sql_path`
  - `rag_prompt_builder.execute_complex_analytics_path`
- Behavior lock preserved:
  - planner route semantics unchanged,
  - no silent fallback from `complex_analytics` to deterministic SQL,
  - telemetry fields shape unchanged (`execution_route`, `executor_*`, `complex_analytics_*`).

Migration note:
- External HTTP/SSE API contract unchanged.
- Internal refactor only; no client migration required.

### Large File Refactor Phase 3: Runtime/Builder Decomposition (2026-03-06)
- Continued split to remove remaining oversized methods and keep modules under 500 LOC:
  - `app/services/chat_orchestrator.py`: 705 -> 281 LOC
  - `app/services/chat/rag_prompt_builder.py`: 590 -> 475 LOC
  - `app/services/chat/orchestrator_runtime.py`: 494 LOC
- Added `app/services/chat/orchestrator_runtime.py`:
  - extracted stream/non-stream execution runtime (`stream_chat_events`, `run_nonstream_chat`)
  - centralized chat response assembly helper for non-stream path.
- Added `app/services/chat/rag_retrieval_helpers.py`:
  - grouped retrieval helper
  - context/debug collection helper
- Preserved backward compatibility and test monkeypatch contracts:
  - no route semantic changes,
  - no execution telemetry shape changes,
  - no API response contract changes.

Migration note:
- External HTTP/SSE API contract unchanged.
- Internal refactor only; no client migration required.

### Large File Refactor Phase 4: Narrative Branch Extraction (2026-03-06)
- Further decomposed RAG prompt orchestration to remove oversized function hotspots:
  - added `app/services/chat/rag_prompt_narrative.py` for narrative retrieval/escalation path,
  - moved grouped retrieval helpers into `app/services/chat/rag_retrieval_helpers.py`.
- Refined `app/services/chat/rag_prompt_builder.py`:
  - `build_rag_prompt` reduced to orchestrator-only flow (73 LOC function),
  - route-specific and narrative-specific logic delegated to dedicated modules.
- Preserved compatibility hooks expected by tests:
  - `rag_prompt_builder.settings`,
  - `rag_prompt_builder.execute_tabular_sql_path`,
  - `rag_prompt_builder.execute_complex_analytics_path`.

Migration note:
- External HTTP/SSE API contract unchanged.
- Internal refactor only; no client migration required.

### Large File Refactor Phase 5: Ingestion Pipeline Extraction (2026-03-06)
- Decomposed oversized file ingestion module while preserving service/API behavior:
  - added `app/services/file_pipeline.py` for ingestion execution pipeline internals:
    - `process_file_pipeline(...)`
    - `finalize_ingestion_pipeline(...)`
  - refactored `app/services/file.py` into runtime wiring + compatibility layer:
    - `_process_file(...)` delegates to `process_file_pipeline(...)`
    - `_finalize_ingestion(...)` delegates to `finalize_ingestion_pipeline(...)`
- Reduced oversized ingestion module:
  - `app/services/file.py`: 678 -> 408 LOC.
- Preserved compatibility and safety:
  - no external API contract changes,
  - no ingestion status/stage contract changes,
  - no metric key changes,
  - no sandbox/security policy relaxations.

Migration note:
- External HTTP/SSE API contract unchanged.
- Internal refactor only; no client migration required.

### Large File Refactor Phase 6: Deterministic SQL Pipeline Extraction (2026-03-06)
- Decomposed oversized deterministic SQL module while preserving route behavior and debug contracts:
  - added `app/services/chat/tabular_sql_pipeline.py` for aggregate/profile/error pipeline internals.
  - refactored `app/services/chat/tabular_sql.py` into coordinator + compatibility wrappers.
- Reduced oversized deterministic SQL module:
  - `app/services/chat/tabular_sql.py`: 685 -> 417 LOC.
- Preserved compatibility and safety:
  - no planner route semantic changes,
  - no deterministic SQL payload/schema changes,
  - no metric key changes,
  - existing monkeypatch test hooks (`_build_sql`, `_execute_aggregate_sync`) preserved.

Migration note:
- External HTTP/SSE API contract unchanged.
- Internal refactor only; no client migration required.

### Large File Refactor Phase 7: RAG Retriever Helper Split (2026-03-06)
- Decomposed oversized retrieval module while preserving class API and retrieval behavior:
  - added `app/rag/retriever_helpers.py` for intent/filter/scoring/rerank/prompt helper logic.
  - refactored `app/rag/retriever.py` into stable class/wiring layer with delegated internals.
- Reduced oversized retrieval module:
  - `app/rag/retriever.py`: 719 -> 458 LOC.
- Preserved compatibility and safety:
  - no route semantic changes (`hybrid`/`full_file` behavior unchanged),
  - no RAG debug payload schema changes,
  - no metric key changes.

Migration note:
- External HTTP/SSE API contract unchanged.
- Internal refactor only; no client migration required.

### Large File Refactor Phase 8: Full-File Analysis Prompt Split (2026-03-07)
- Decomposed oversized full-file prompt module while preserving RAG behavior:
  - added `app/services/chat/full_file_analysis_runtime.py` (map-reduce runtime),
  - added `app/services/chat/full_file_analysis_helpers.py` (batch/range/structured merge helpers),
  - kept `app/services/chat/full_file_analysis.py` as stable facade.
- Reduced oversized prompt module:
  - `app/services/chat/full_file_analysis.py`: 554 -> 48 LOC.
- Preserved compatibility:
  - no route behavior changes,
  - no prompt/result contract changes,
  - monkeypatch points for `settings` and `llm_manager` preserved via facade.

Migration note:
- External HTTP/SSE API contract unchanged.
- Internal refactor only; no client migration required.

### Large File Refactor Phase 9: Durable SQLite Queue Split (2026-03-07)
- Decomposed oversized ingestion queue adapter:
  - added `app/services/ingestion/sqlite_queue_runtime.py` for sync SQL operations,
  - refactored `app/services/ingestion/sqlite_queue.py` into async facade.
- Reduced oversized queue adapter module:
  - `app/services/ingestion/sqlite_queue.py`: 547 -> 183 LOC.
- Preserved compatibility:
  - `SqliteIngestionQueueAdapter` method contracts unchanged,
  - queue stats payload shape unchanged.
- Stability adjustment:
  - lease acquisition now uses exact requested `lease_seconds` lower bound `0.0` for deterministic expiry behavior in short-lease replay scenarios.

Migration note:
- External HTTP/SSE API contract unchanged.
- Internal refactor only; no client migration required.

### Large File Refactor Phase 10: Complex Executor Compose Split (2026-03-07)
- Decomposed oversized complex analytics executor:
  - added `app/services/chat/complex_analytics/executor_compose.py` for compose-stage runtime,
  - kept `_apply_compose_stage` wrapper in `executor.py` for callback monkeypatch compatibility.
- Reduced oversized executor module:
  - `app/services/chat/complex_analytics/executor.py`: 507 -> 480 LOC.
- Preserved compatibility:
  - `execute_complex_analytics_path(...)` contract unchanged,
  - debug/telemetry fields unchanged.

Migration note:
- External HTTP/SSE API contract unchanged.
- Internal refactor only; no client migration required.

### Complex Analytics Two-Pass Pipeline Hardening (2026-03-06, superseded defaults)
- Completed backend two-stage generation flow for `complex_analytics`:
  - stage 1: `complex_analytics_plan` (strict JSON plan),
  - stage 2: `complex_analytics_codegen` (Python-only code),
  - stage 3: sandbox execution with artifact validation,
  - stage 4: `complex_analytics_response` composition from execution output.
- Enforced safer degradation policy (later adjusted in same release cycle):
  - template fallback/runtime fallback remain configurable and now default-on for broad analytics UX,
  - failed codegen/validation still returns reason-specific clarification when fallback path is disabled or cannot execute (`codegen_failed`, `missing_required_artifacts`, `validation_failed`).
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
- External HTTP/SSE contract unchanged; fallback policy remains internal behavior control via settings.

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

### Changed
- Frontend composer layout: input kept on a dedicated top row, control row normalized to `File + provider + model + RAG mode + Send`.
- Added inline helper labels under provider/model/RAG selectors (`AI provider`, `Model selection`, `Document mode`).
- Moved `Logout` inside `Settings` footer and styled it as a danger action next to `Save`.

### Fixed
- Replaced compact `Del` in chat sidebar with a visible dedicated delete button (trash icon) on each chat row.
- `RAG debug` block is now hidden unless request explicitly sets `rag_debug=true`.

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
  - added `tabular_profile` intent path for broad analytical prompts (`per-column stats/metrics`, `full-file analytical summary`);
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

### Added
- Authentication system (JWT)
- Conversation management
- Chat history
- Ollama integration
- OpenAI integration
- PostgreSQL database
- Alembic migrations
- API documentation
- Responsive UI
- Request logging

### Changed
- Modular frontend structure
- SQL query performance optimizations

### Fixed
- User data isolation
- Message counters in conversations




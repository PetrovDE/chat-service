# 19. Big File Refactor: Complex Analytics

## Motivation

`app/services/chat/complex_analytics.py` had grown into a high-risk monolith (~2400 LOC) combining multiple bounded contexts:
- intent/routing
- LLM plan + codegen contract handling
- sandbox security and execution
- artifact lifecycle
- response composition/localization
- error envelope and telemetry payload construction

This increased regression risk, slowed reviews, and made safe changes difficult.

## Oversized File Audit (complex-first)

Top oversized/complex files (app):
- `app/services/chat/complex_analytics.py` (2408 LOC before refactor)
- `app/services/chat_orchestrator.py` (884 LOC)
- `app/services/chat/rag_prompt_builder.py` (778 LOC)
- `app/services/chat/tabular_sql.py` (685 LOC)
- `app/services/file.py` (678 LOC)

Bounded-context risk map:
- `complex_analytics.py`: mixed planning/codegen/sandbox/compose/telemetry in one unit.
- `chat_orchestrator.py`: provider precedence, SSE/non-SSE response assembly, route telemetry.
- `rag_prompt_builder.py`: planner branching + retrieval escalation + coverage metrics.

Key refactor risks addressed:
- accidental API contract drift in chat response/debug fields
- provider precedence drift for explicit local/provider mode
- unsafe fallback from `complex_analytics` to deterministic SQL path

## Before / After Structure

Before:
- `app/services/chat/complex_analytics.py` (single monolith)

After:
- `app/services/chat/complex_analytics/__init__.py`
- `app/services/chat/complex_analytics/planner.py`
- `app/services/chat/complex_analytics/codegen.py`
- `app/services/chat/complex_analytics/sandbox.py`
- `app/services/chat/complex_analytics/executor.py`
- `app/services/chat/complex_analytics/composer.py`
- `app/services/chat/complex_analytics/artifacts.py`
- `app/services/chat/complex_analytics/errors.py`
- `app/services/chat/complex_analytics/telemetry.py`
- `app/services/chat/complex_analytics/dataset_context.py`
- `app/services/chat/complex_analytics/template_codegen.py`
- `app/services/chat/complex_analytics/report_quality.py`
- `app/services/chat/complex_analytics/localization.py`
- `app/services/chat/complex_analytics/auto_visual_patch.py`
- `app/services/chat/complex_analytics/executor_support.py`

Public entrypoints preserved:
- `execute_complex_analytics_path(...)`
- `is_complex_analytics_query(...)`

## Compromises

- Scope intentionally kept complex-first: `chat_orchestrator` and `rag_prompt_builder` were not decomposed in this iteration.
- Internal test hooks were migrated to module-level executor imports to keep monkeypatch behavior stable during transition.
- Fallback/security/routing semantics were preserved before any behavioral cleanup.
- Added targeted internal resilience improvement: when codegen misses required visualization contract, safe auto-visual patch is attempted before template fallback.
- Auto-patch snippet was adjusted to sandbox builtin limitations (no `isinstance` usage) to avoid runtime fallback loops.

## Post-Refactor Incident Fix (2026-03-06)

Observed issue:
- Broad request ("analyze full file and build charts") could fail with `codegen_failed` when generated code did not contain `save_plot(...)`.

Implemented fix:
- `codegen.py` now tries contract auto-repair for `missing_visualization_contract` and revalidates before fallback.
- `config` defaults keep safe degradation enabled:
  - `COMPLEX_ANALYTICS_ALLOW_TEMPLATE_FALLBACK=true`
  - `COMPLEX_ANALYTICS_ALLOW_TEMPLATE_RUNTIME_FALLBACK=true`
- Added debug observability:
  - `complex_analytics.codegen_auto_visual_patch_applied`
  - `complex_analytics.complex_analytics_codegen.auto_visual_patch_applied`

Next iteration (quality):
- Added compose quality gate to prevent low-content final answers from LLM compose stage.
- If compose output is too short/generic, executor falls back to local structured report formatter using executed metrics/artifacts.
- Observability:
  - `response_status=fallback`
  - `response_error_code=low_content_quality`
- For broad full-analysis requests, compose call is now policy-skipped in favor of deterministic local formatter:
  - `COMPLEX_ANALYTICS_PREFER_LOCAL_COMPOSER_FOR_BROAD_QUERY=true` (default)
  - `response_error_code=broad_query_local_formatter`
- Added dedicated offline eval dataset + CI metric for broad prompt quality regression control:
  - `tests/evals/datasets/complex_analytics_quality_golden.jsonl`
  - metric `complex_analytics_report_quality`
  - gate `complex_analytics_report_quality_min`
- Added dedicated online/hybrid preprod quality gate for real `/api/v1/chat` validation:
  - dataset `tests/evals/datasets/complex_analytics_quality_online.jsonl`
  - metric `online_report.metrics.complex_analytics_report_quality`
  - gate config `tests/evals/gates.preprod.json`

## AI HUB Policy Timeout Fix (2026-03-10)

Observed issue:
- `complex_analytics` on `aihub + policy` could fallback with `reason=timeout` before provider response arrived, leading to weak/generic output paths.

Implemented fix:
- Added provider-aware timeout overrides for slow AI HUB policy path:
  - `COMPLEX_ANALYTICS_CODEGEN_PLAN_TIMEOUT_SECONDS_AIHUB_POLICY`
  - `COMPLEX_ANALYTICS_CODEGEN_TIMEOUT_SECONDS_AIHUB_POLICY`
  - `COMPLEX_ANALYTICS_RESPONSE_TIMEOUT_SECONDS_AIHUB_POLICY`
- Effective timeout per stage is now computed as `max(base_timeout, aihub_policy_override)`.
- Added debug trace fields for effective timeouts:
  - `complex_analytics.codegen_plan_timeout_seconds`
  - `complex_analytics.codegen_timeout_seconds`
  - `complex_analytics.response_timeout_seconds`

Verification:
- `tests/unit/test_complex_analytics_codegen_module.py`
- `tests/unit/test_complex_analytics_composer_module.py`

## Regression Verification

Executed test gates:
- `tests/unit/test_query_planner.py`
- `tests/integration/test_query_planner_contract.py`
- `tests/unit/test_chat_response_backward_compat.py`
- `tests/unit/test_complex_analytics_executor.py`
- `tests/integration/test_complex_analytics_path.py`
- `tests/smoke/test_complex_analytics_smoke.py`

Additional module-level unit coverage added:
- `tests/unit/test_complex_analytics_planner_module.py`
- `tests/unit/test_complex_analytics_codegen_module.py`
- `tests/unit/test_complex_analytics_sandbox_module.py`
- `tests/unit/test_complex_analytics_composer_module.py`
- `tests/unit/test_complex_analytics_import_compat.py`
- `tests/unit/test_complex_analytics_telemetry_module.py`

Targeted regression:
- `tests/integration/test_complex_analytics_path.py::test_complex_analytics_auto_visual_patch_prevents_codegen_failure`
- `tests/integration/test_complex_analytics_path.py::test_complex_analytics_compose_quality_gate_falls_back_to_local_formatter`

## File Size Outcome (2026-03-06)

Post-split key modules:
- `codegen.py`: 428 LOC
- `executor.py`: 499 LOC
- `composer.py`: 479 LOC

Goal achieved for this iteration:
- removed >500 LOC hot files in complex_analytics package without external contract changes.

## Phase 2 (2026-03-06): Orchestrator / RAG Builder Split

Motivation:
- after complex-analytics split, next oversized hotspots remained `chat_orchestrator.py` and `rag_prompt_builder.py`.

Changes:
- added `app/services/chat/orchestrator_helpers.py` and moved non-domain helper logic:
  - route telemetry defaults,
  - execution telemetry extraction,
  - artifact payload normalization,
  - clarification text resolution,
  - generation kwargs and postprocess helpers.
- added `app/services/chat/rag_prompt_routes.py` and moved route-specific branches:
  - `clarification`,
  - `complex_analytics`,
  - `deterministic_analytics`.

Before/after:
- `chat_orchestrator.py`: 884 -> 705 LOC
- `rag_prompt_builder.py`: 778 -> 586 LOC

Compatibility guardrails:
- preserved monkeypatch points expected by integration tests:
  - `rag_prompt_builder.execute_tabular_sql_path`
  - `rag_prompt_builder.execute_complex_analytics_path`
- no contract changes in `ChatResponse`/SSE route telemetry fields.

Verification:
- `tests/integration/test_rag_integration.py`
- `tests/integration/test_rag_dynamic_budget.py`
- `tests/integration/test_complex_analytics_path.py`
- `tests/smoke/test_complex_analytics_smoke.py`
- `tests/unit/test_query_planner.py`
- `tests/integration/test_query_planner_contract.py`
- `tests/unit/test_chat_response_backward_compat.py`

## Phase 3 (2026-03-06): Runtime / Retrieval Helper Split

Motivation:
- phase 2 still left large methods in `chat_orchestrator` and utility-heavy top section in `rag_prompt_builder`.

Changes:
- added `app/services/chat/orchestrator_runtime.py`:
  - `stream_chat_events(...)`
  - `run_nonstream_chat(...)`
- added `app/services/chat/rag_retrieval_helpers.py`:
  - grouped retrieval orchestration,
  - context/debug merge helpers.

Before/after:
- `chat_orchestrator.py`: 705 -> 281 LOC
- `rag_prompt_builder.py`: 590 -> 475 LOC
- `orchestrator_runtime.py`: 494 LOC

Compatibility:
- API and SSE payload contracts unchanged.
- Existing monkeypatch test points preserved in `rag_prompt_builder`.

Verification:
- `tests/integration/test_rag_integration.py`
- `tests/integration/test_rag_dynamic_budget.py`
- `tests/integration/test_complex_analytics_path.py`
- `tests/smoke/test_complex_analytics_smoke.py`
- `tests/smoke/test_app_smoke.py`
- `tests/unit/test_complex_analytics_executor.py`

## Phase 4 (2026-03-06): Narrative Branch Function Split

Motivation:
- `build_rag_prompt` still contained a large narrative retrieval branch with escalation/coverage logic.

Changes:
- added `app/services/chat/rag_prompt_narrative.py`:
  - full narrative retrieval branch (`query_rag` orchestration, escalation, coverage, map-reduce prompt integration).
- `app/services/chat/rag_prompt_builder.py` now coordinates:
  - dependency resolution,
  - file loading,
  - planner decision dispatch,
  - delegation to route/narrative modules.

Before/after:
- `rag_prompt_builder.py`: 475 -> 202 LOC
- `build_rag_prompt`: 432 -> 73 LOC

Compatibility:
- preserved module-level compatibility attributes used by tests:
  - `settings`, `execute_tabular_sql_path`, `execute_complex_analytics_path`.

Verification:
- `tests/integration/test_rag_integration.py`
- `tests/integration/test_rag_dynamic_budget.py`
- `tests/integration/test_complex_analytics_path.py`
- `tests/smoke/test_app_smoke.py`

## Phase 5 (2026-03-06): File Ingestion Pipeline Extraction

Motivation:
- `app/services/file.py` remained oversized and mixed worker/runtime wiring with heavy ingestion execution stages.

Changes:
- added `app/services/file_pipeline.py`:
  - `process_file_pipeline(...)` for extract/chunk/embed/upsert/finalize stages,
  - `finalize_ingestion_pipeline(...)` for status normalization and metrics updates.
- refactored `app/services/file.py` into compatibility/wiring layer:
  - `_process_file(...)` now delegates to `process_file_pipeline(...)`,
  - `_finalize_ingestion(...)` now delegates to `finalize_ingestion_pipeline(...)`,
  - public service entrypoints unchanged (`process_file_async`, `process_file_background`, worker lifecycle helpers).
- dependency injection preserved monkeypatch compatibility for existing integration tests.

Before/after:
- `file.py`: 678 -> 408 LOC
- `file_pipeline.py`: 354 LOC

Compatibility:
- no HTTP API changes.
- no ingestion status/stage enum changes.
- no ingestion metric key changes.

Verification:
- `tests/integration/test_ingestion_and_response_contract.py`
- `tests/integration/test_ingestion_chunking_strategy.py`
- `tests/smoke/test_app_smoke.py`

## Phase 6 (2026-03-06): Deterministic SQL Pipeline Extraction

Motivation:
- `app/services/chat/tabular_sql.py` remained oversized and mixed intent/sql planning wrappers with aggregate/profile/error execution internals.

Changes:
- added `app/services/chat/tabular_sql_pipeline.py`:
  - `execute_aggregate_sync_pipeline(...)`,
  - `build_profile_payload_pipeline(...)`,
  - `execute_profile_sync_pipeline(...)`,
  - `build_tabular_error_result_pipeline(...)`.
- refactored `app/services/chat/tabular_sql.py` into coordinator/compatibility layer:
  - `_execute_aggregate_sync(...)`, `_build_profile_payload(...)`, `_execute_profile_sync(...)`, `_build_tabular_error_result(...)` now delegate to pipeline module.
  - `execute_tabular_sql_path(...)`, `detect_tabular_intent(...)`, `is_tabular_aggregate_intent(...)` unchanged.
- kept test monkeypatch compatibility:
  - `_build_sql` and `_execute_aggregate_sync` remain in `tabular_sql.py` and are still used by runtime path.

Before/after:
- `tabular_sql.py`: 685 -> 417 LOC
- `tabular_sql_pipeline.py`: 385 LOC

Compatibility:
- no HTTP/SSE contract changes.
- no planner route decision changes.
- no deterministic SQL debug field schema changes.
- no metric key changes.

Verification:
- `tests/integration/test_tabular_sql_intent.py`
- `tests/integration/test_tabular_sql_guardrails.py`
- `tests/integration/test_tabular_runtime_migration.py`
- `tests/integration/test_rag_integration.py`

## Phase 7 (2026-03-06): RAG Retriever Helper Split

Motivation:
- `app/rag/retriever.py` remained oversized and combined class API, intent/filter logic, hybrid scoring, LangChain rerank glue, and context prompt formatting.

Changes:
- added `app/rag/retriever_helpers.py`:
  - intent/filter helpers: `detect_intent`, `resolve_intent`, `build_where`,
  - scoring/rerank helpers: `lexical_scores`, `merge_hybrid`, `rerank_with_langchain`, `select_with_coverage`,
  - formatting helpers: `rows_to_documents`, `build_context_prompt`,
  - LangChain adapter: `StaticDenseRetriever`.
- refactored `app/rag/retriever.py` into class-level API/wiring layer:
  - kept public class/method contract unchanged (`RAGRetriever.retrieve`, `retrieve_full_file`, `query_rag`, `build_context_prompt`),
  - internal methods now delegate to helper module while preserving behavior.

Before/after:
- `retriever.py`: 719 -> 458 LOC
- `retriever_helpers.py`: 354 LOC

Compatibility:
- no HTTP/SSE contract changes.
- no RAG debug payload shape changes.
- no planner/route semantic changes.
- no observability metric key changes.

Verification:
- `tests/integration/test_rag_integration.py`
- `tests/integration/test_rag_dynamic_budget.py`
- `tests/smoke/test_app_smoke.py`

## Phase 8 (2026-03-07): Full-File Analysis Prompt Split

Motivation:
- `app/services/chat/full_file_analysis.py` mixed helper utilities and large map-reduce orchestration in one oversized file.

Changes:
- added `app/services/chat/full_file_analysis_helpers.py` for range/batch/json-merge helpers.
- added `app/services/chat/full_file_analysis_runtime.py` for map-reduce orchestration runtime.
- kept `app/services/chat/full_file_analysis.py` as stable facade used by RAG builder.
- preserved monkeypatch compatibility used by tests:
  - `full_file_analysis.settings`,
  - `full_file_analysis.llm_manager`.

Before/after:
- `full_file_analysis.py`: 554 -> 48 LOC
- `full_file_analysis_runtime.py`: 258 LOC
- `full_file_analysis_helpers.py`: 311 LOC

Compatibility:
- no route/contract changes.
- no prompt-builder response schema changes.

Verification:
- `tests/integration/test_rag_integration.py`
- `tests/integration/test_rag_dynamic_budget.py`

## Phase 9 (2026-03-07): Durable SQLite Queue Split

Motivation:
- `app/services/ingestion/sqlite_queue.py` mixed async adapter facade and heavy sync SQL operations.

Changes:
- added `app/services/ingestion/sqlite_queue_runtime.py` with sync DB operations.
- refactored `sqlite_queue.py` into async facade delegating to runtime module.
- preserved adapter class and method contracts:
  - `enqueue`, `acquire`, `mark_*`, `requeue_expired_leases`, `heartbeat`, `get_stats`.

Before/after:
- `sqlite_queue.py`: 547 -> 183 LOC
- `sqlite_queue_runtime.py`: 467 LOC

Compatibility:
- no queue adapter API changes.
- no queue stats payload changes.

Verification:
- `tests/integration/test_ingestion_durable_queue.py`
- `tests/smoke/test_ingestion_durable_smoke.py`
- `tests/integration/test_ingestion_and_response_contract.py`
- `tests/integration/test_ingestion_chunking_strategy.py`

## Phase 10 (2026-03-07): Complex Executor Compose Split

Motivation:
- `app/services/chat/complex_analytics/executor.py` exceeded 500 LOC and contained compose-stage runtime logic.

Changes:
- added `app/services/chat/complex_analytics/executor_compose.py` with compose-stage runtime.
- kept `_apply_compose_stage` wrapper in `executor.py` to preserve test monkeypatch compatibility for compose callbacks.

Before/after:
- `executor.py`: 507 -> 480 LOC
- `executor_compose.py`: 63 LOC

Compatibility:
- `execute_complex_analytics_path(...)` unchanged.
- executor debug/telemetry fields unchanged.

Verification:
- `tests/unit/test_complex_analytics_executor.py`
- `tests/integration/test_complex_analytics_path.py`
- `tests/smoke/test_complex_analytics_smoke.py`

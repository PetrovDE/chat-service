# 09. Observability

## Logging and Trace Context
Injected correlation fields:
- `request_id`
- `user_id`
- `conversation_id`
- `file_id`

Request middleware sets `x-request-id`.

Request-id behavior contract:
- If client sends `x-request-id`, backend echoes the same value in response header.
- If header is missing, backend generates request id and returns it in response header.
- Access logs from `app.main` (`METHOD PATH -> STATUS`) are emitted with the same non-empty `request_id` as service logs.
- Same contract applies to `/api/v1/chat/stream` responses (SSE).

## Metrics Surfaces
- `/metrics` exposes in-memory Prometheus format.
- Metric primitives: counter/timer/gauge in `app/observability/metrics.py`.

## Required Operational Metrics
- Retrieval:
- `rag_retrieve_total`
- `llama_service_retrieval_coverage_ratio`
- `llama_service_retrieval_coverage_events_total`
- Embedding/provider latency:
- `llm_provider_duration_ms`
- Vector insert path:
- `ingestion_upserts_ok`, `ingestion_upserts_fail`
- Planner decisions:
- `llama_service_query_planner_route_total`
- Fallback rate:
- `llama_service_llm_fallback_total`
- Queue depth:
- `llama_service_ingestion_queue_depth`
- Complex analytics execution quality:
- `complex_analytics_executor_success_total`
- `complex_analytics_artifacts_generated_total`
- `complex_analytics_artifact_kind_total{kind=...}`
- `complex_analytics_artifacts_cleanup_total{status=deleted|failed}`
- `complex_analytics_codegen_total{status=success|fallback,reason=...}`

Structured log events for complex analytics pipeline:
- `complex_analytics.codegen_plan` (`status`, `reason`, `provider`, `mode`, contract flags, effective timeouts)
- `complex_analytics.codegen_execute` (`status`, `reason`, `provider`, `model_route`, effective timeouts)
  - includes `status=success_via_auto_visual_patch` when visualization contract is repaired before execution
- `complex_analytics.compose` (`status`, `reason`, `provider`, `model_route`, effective timeout)
  - includes `status=fallback reason=low_content_quality` when compose output is too weak and local formatter is used
  - includes `status=fallback reason=broad_query_local_formatter` when broad full-analysis query is answered directly by local formatter policy
- `chat_route_decision` (includes `execution_route`, `executor_status`, `executor_error_code`, `artifacts_count`)

## Debug/Trace Fields
RAG debug payload includes:
- filters (`where`/`filters`)
- top chunks with scores and row ranges
- sources list
- context token estimate
- provider prompt truncation metadata
- chunk and row coverage metrics

## Complex Analytics Artifact Retention
Executor applies retention cleanup on each run for artifact root:
- `COMPLEX_ANALYTICS_ARTIFACT_TTL_HOURS`
- `COMPLEX_ANALYTICS_ARTIFACT_MAX_RUN_DIRS`

Codegen controls:
- `COMPLEX_ANALYTICS_CODEGEN_ENABLED`
- `COMPLEX_ANALYTICS_CODEGEN_FORCE_LOCAL`
- `COMPLEX_ANALYTICS_CODEGEN_PLAN_TIMEOUT_SECONDS`
- `COMPLEX_ANALYTICS_CODEGEN_TIMEOUT_SECONDS`
- `COMPLEX_ANALYTICS_CODEGEN_PLAN_TIMEOUT_SECONDS_AIHUB_POLICY`
- `COMPLEX_ANALYTICS_CODEGEN_TIMEOUT_SECONDS_AIHUB_POLICY`
- `COMPLEX_ANALYTICS_CODEGEN_MAX_TOKENS`
- `COMPLEX_ANALYTICS_RESPONSE_TIMEOUT_SECONDS`
- `COMPLEX_ANALYTICS_RESPONSE_TIMEOUT_SECONDS_AIHUB_POLICY`
- `COMPLEX_ANALYTICS_ALLOW_TEMPLATE_FALLBACK` (default: true)
- `COMPLEX_ANALYTICS_ALLOW_TEMPLATE_RUNTIME_FALLBACK` (default: true)
- `COMPLEX_ANALYTICS_PREFER_LOCAL_COMPOSER_FOR_BROAD_QUERY` (default: true)

## Recommended Dashboard Panels
- AI HUB route vs fallback share
- Circuit state transitions
- Ingestion queue depth/lag/dead-letter
- Retrieval coverage buckets
- Tabular row coverage ratio
- SQL path error codes

## Update 2026-03-06
- Complex analytics adds per-stage structured logs:
  - `complex_analytics.codegen_plan`
  - `complex_analytics.codegen_execute`
  - `complex_analytics.compose`
- Debug payload includes pipeline-specific fields:
  - `complex_analytics_code_generation_prompt_status`
  - `complex_analytics_code_generation_source`
  - `complex_analytics_codegen.provider`
  - `codegen_auto_visual_patch_applied`
  - `complex_analytics_codegen.auto_visual_patch_applied`
  - `codegen_plan_timeout_seconds`
  - `codegen_timeout_seconds`
  - `response_timeout_seconds`
  - `sandbox.secure_eval`
- Internal implementation now emits these fields from modular package `app/services/chat/complex_analytics/*`; metric/log keys are unchanged.
- Additional refactor in chat-plane orchestration (`orchestrator_helpers.py`, `rag_prompt_routes.py`) does not change metric or structured-log key names.
- Runtime/refactoring split (`orchestrator_runtime.py`, `rag_retrieval_helpers.py`) also keeps metric names and structured log event keys unchanged.
- Narrative branch extraction (`rag_prompt_narrative.py`) keeps existing observability keys unchanged.
- Ingestion refactor (`file.py` -> `file_pipeline.py`) keeps ingestion metric names unchanged:
  - `ingestion_stage_ms`, `ingestion_total_ms`,
  - `ingestion_chunks_total`, `ingestion_chunks_ok`, `ingestion_chunks_bad`,
  - `ingestion_upserts_ok`, `ingestion_upserts_fail`,
  - `ingestion_finalize_total`.
- Deterministic SQL refactor (`tabular_sql.py` -> `tabular_sql_pipeline.py`) keeps existing metric names unchanged:
  - `tabular_sql_execution_ms`, `tabular_sql_path_ms`,
  - `tabular_sql_path_total`, `tabular_sql_path_timeout_total`,
  - `tabular_sql_path_error_total`, `tabular_sql_guardrail_violation_total`.
- Retrieval refactor (`retriever.py` -> `retriever_helpers.py`) keeps existing metric names unchanged:
  - `rag_retrieve_total`,
  - `rag_retrieve_duration_ms`,
  - `rag_retrieve_full_file_duration_ms`,
  - `rag_embed_duration_ms`,
  - `rag_candidates_duration_ms`,
  - `rag_rerank_duration_ms`.
- Full-file analysis refactor (`full_file_analysis.py` -> runtime/helpers split) keeps map-reduce observability payload keys unchanged.
- Durable ingestion queue refactor (`sqlite_queue.py` -> `sqlite_queue_runtime.py`) keeps worker/queue stats shape and ingestion metric keys unchanged.
- Complex analytics executor compose-stage split keeps structured event keys unchanged:
  - `complex_analytics.codegen_plan`,
  - `complex_analytics.codegen_execute`,
  - `complex_analytics.compose`,
  - `chat_route_decision` executor fields.


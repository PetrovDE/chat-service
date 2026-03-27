# ADR-014: Complex Analytics Sandbox Executor

## Context

Deterministic tabular SQL path is safe and reproducible for aggregate/profile intents, but it is intentionally limited.
Requests such as Python/pandas multi-step analysis, chart generation (heatmap), or NLP over tabular text columns require a separate execution plane.

Constraints:
- Offline contour only (no internet/external APIs).
- Existing chat API contracts must stay backward compatible.
- AI HUB-first policy remains valid for generation paths, but explicit local/provider selection must keep priority where model generation is used.
- Sandbox security is mandatory (network/subprocess/system operations blocked).

## Decision

Introduce a dedicated `complex_analytics` route and executor:
- Planner adds `complex_analytics` intent/route classification.
- `app/services/chat/complex_analytics/` package runs isolated Python analytics pipeline with:
  - two-pass LLM generation:
    - `plan` stage (strict JSON with `analysis_goal`, contract flags and `python_generation_prompt`),
    - `codegen` stage (Python-only output from plan + dataframe profile),
  - `compose` stage (same selected provider/model) to generate final user-facing report from execution output,
  - compose quality gate with fallback to local structured formatter when LLM response is weak/underspecified,
  - provider/model selection honors explicit UI/provider override; AI HUB remains default runtime policy for generic chat,
  - optional forced-local override via `COMPLEX_ANALYTICS_CODEGEN_FORCE_LOCAL`,
  - strict code contract validation before execution (`result` payload + datasets access + AST security precheck),
  - safe codegen auto-repair for visualization contract (`missing_visualization_contract`) prior to final fallback,
  - template fallback is enabled by default:
    - `COMPLEX_ANALYTICS_ALLOW_TEMPLATE_FALLBACK=true`,
    - `COMPLEX_ANALYTICS_ALLOW_TEMPLATE_RUNTIME_FALLBACK=true`,
    - failures return reason-specific clarification instead of silent template substitution,
  - AST security checks (blocked imports/calls and URL literals),
  - block dunder-attribute access and dynamic bypass primitives (`getattr/setattr/...`),
  - bounded timeout,
  - bounded stdout/result size,
  - bounded artifact count,
  - artifact write guard in controlled temp directory.
- Artifact response contract is sanitized:
  - no absolute filesystem paths in API payloads,
  - `artifacts.path` is relative and `artifacts.url` is browser-accessible.
- Artifact lifecycle has retention cleanup in executor path (TTL and max run directories).
- Data source is existing tabular runtime metadata (`tabular_dataset` + resolver), loaded via DuckDB/pandas.
- `rag_prompt_builder` uses executor path and sets `short_circuit_response` for direct answer return (no unsafe fallback).
- `ChatOrchestrator` supports short-circuit response path and extends response/SSE telemetry:
  - `execution_route`,
  - `executor_attempted`,
  - `executor_status` (`success|error|timeout|blocked|fallback|not_attempted`),
  - `executor_error_code`,
  - `artifacts_count`.
- Debug telemetry includes pipeline details:
  - `complex_analytics_code_generation_prompt_status`,
  - `complex_analytics_code_generation_source`,
  - `complex_analytics_codegen.provider`,
  - `codegen_auto_visual_patch_applied`,
  - `complex_analytics_codegen.auto_visual_patch_applied`,
  - `sandbox.secure_eval`.
- Complex analytics formatter is language-aware for report text (RU query -> RU report, EN query -> EN report).
- Report includes richer textual layer via `insights` when provided by generated script.

## Consequences

Positive:
- Complex analytics prompts no longer pass through deterministic SQL guardrail-only path.
- Security boundaries for Python analytics are explicit and testable.
- Users get route/executor observability and artifact metadata in debug.
- Users get readable localized report structure in short-circuit responses.
- Artifact storage footprint remains bounded over time via retention cleanup.

Trade-offs:
- LLM codegen quality depends on selected model/provider quality and prompt discipline.
- Plot dependencies (`matplotlib`, `seaborn`) become part of runtime requirements.
- Additional maintenance surface for sandbox policy and artifact lifecycle.

## Update 2026-03-06
- Architecture is finalized as `plan -> codegen -> sandbox -> compose` within backend.
- Template fallback policy is default-on for robust broad analytics UX (`COMPLEX_ANALYTICS_ALLOW_TEMPLATE_FALLBACK=true`).
- Missing required visualization artifacts are treated as classified validation errors with clarification response.
- Visualization contract repair path is observable via debug/telemetry auto-patch flags and `complex_analytics.codegen_execute` stage status.
- Compose quality fallback is observable via `response_status=fallback` and `response_error_code=low_content_quality`.
- Implementation is refactored from single file to modular package `app/services/chat/complex_analytics/`:
  - `planner.py` (intent/routing/plan parsing),
  - `codegen.py` (prompting + contract validation),
  - `sandbox.py` (AST policy + secure execution),
  - `executor.py` (pipeline orchestration),
  - `composer.py` (final response generation),
  - `artifacts.py` (path/url sanitization + retention),
  - `errors.py`, `telemetry.py`,
  - `dataset_context.py`, `template_codegen.py`, `report_quality.py`, `localization.py`,
  - `auto_visual_patch.py`, `executor_support.py`.
- Broad full-analysis prompts may intentionally skip compose LLM call and keep deterministic local formatter output (`response_error_code=broad_query_local_formatter`) to avoid generic low-quality summaries.
- Public import contract remains stable via package entrypoints:
  - `execute_complex_analytics_path`
  - `is_complex_analytics_query`
- Follow-up internal architecture split in surrounding chat pipeline:
  - `app/services/chat/orchestrator_helpers.py`
  - `app/services/chat/rag_prompt_routes.py`
  - `app/services/chat/orchestrator_runtime.py`
  - `app/services/chat/rag_retrieval_helpers.py`
  - `app/services/chat/rag_prompt_narrative.py`
  (no external API contract changes).
- Additional internal split for executor maintainability:
  - compose-stage runtime moved to `app/services/chat/complex_analytics/executor_compose.py`,
  - `_apply_compose_stage` in `executor.py` remains compatibility wrapper.
  (no external API contract changes).

## Update 2026-03-10
- AI HUB policy path latency handling was tightened for complex analytics:
  - provider-aware timeout overrides added for plan/codegen/compose stages,
  - effective stage timeout is `max(base_timeout, aihub_policy_timeout)`.
- New settings:
  - `COMPLEX_ANALYTICS_CODEGEN_PLAN_TIMEOUT_SECONDS_AIHUB_POLICY`
  - `COMPLEX_ANALYTICS_CODEGEN_TIMEOUT_SECONDS_AIHUB_POLICY`
  - `COMPLEX_ANALYTICS_RESPONSE_TIMEOUT_SECONDS_AIHUB_POLICY`
- Consequence:
  - fewer false fallback outcomes (`reason=timeout`) on slow AI HUB policy responses,
  - no external API contract changes (debug-only optional fields).

## Update 2026-03-10 (Quality and Artifact Budget)
- Artifact handling:
  - increased base artifact limit and introduced hard cap setting,
  - effective artifact limit is resolved dynamically by query/plan complexity and dataset width.
- Result quality:
  - executor enriches incomplete script metrics from dataframe to keep profile/statistics contract useful even on partial code outputs.
- Compose quality:
  - generic "request processed" style responses are rejected by stricter quality gate,
  - responses must reference concrete columns/metrics when context provides them.
- External API contract unchanged; only internal behavior and optional debug details were extended.

## Update 2026-03-27 (Stage 2: Remove Hardcoded Answers)
- Production runtime no longer uses template-generated analytics code fallbacks.
- `codegen.py` failure paths now return explicit metadata with `code_source=none` instead of template code payload.
- `executor.py` no longer performs runtime `template_runtime_fallback`.
- `execute_complex_analytics_path(...)` enforces `code_source=llm` for execution and returns structured `codegen_failed` when unavailable.
- Compatibility posture:
  - API envelope and debug structure remain backward compatible,
  - deterministic compose-stage fallback remains for response quality only (based on executed evidence, not template analytics generation).

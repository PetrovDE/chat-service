# 05. Query Planner

## Purpose
Route each request to either:
- `deterministic_analytics`
- `complex_analytics`
- `narrative_retrieval`

Implemented in `app/domain/chat/query_planner.py`.

## Decision Contract
`QueryPlanDecision.as_dict()` returns:
- `route`
- `intent`
- `confidence`
- `requires_clarification`
- `reason_codes`
- optional: `metric_critical`, `clarification_prompt`

## Intent Classes
- `tabular_aggregate`
- `tabular_profile`
- `narrative_retrieval`
- `complex_analytics`
- `metric_clarification`

## Complex Analytics Path
Planner routes to `complex_analytics` when request intent indicates sandboxed Python analytics, for example:
- `python` / `pandas` / `numpy` instructions,
- visualization and dependency requests (`heatmap`, `correlation`, `dependency`, chart/plot),
- NLP-like processing over tabular text columns (`comment_text`, tokens, sentiment),
- multi-step analytics instructions.

For this route:
- deterministic SQL aggregate/profile path is skipped,
- execution goes through isolated sandbox runtime (`app/services/chat/complex_analytics/` package),
- route does not silently downgrade to deterministic SQL on executor failure,
- if plan requires visualization and generated code misses chart contract, executor applies safe codegen auto-visual patch before template fallback,
- executor metadata is emitted in debug/telemetry (`execution_route`, `executor_status`, `artifacts_count`),
- response report format is language-aware (RU query -> RU report, EN query -> EN report).

## Clarification Path
For metric-critical ambiguous queries against tabular datasets:
- route remains deterministic,
- execution is blocked,
- planner returns `requires_clarification=true` and `clarification_prompt`.

`ChatOrchestrator` short-circuits and returns clarification without LLM generation.

## Telemetry
- `query_planner_decision_total`
- `query_planner_reason_total`
- SLO metric wrapper: `llama_service_query_planner_route_total`

## Update 2026-03-06
- Complex analytics intent includes dependency/correlation requests and prioritizes sandbox execution plane.
- Planner behavior is unchanged for deterministic SQL aggregate/profile intents and remains backward compatible.
- `is_complex_analytics_query` remains stable at `app.services.chat.complex_analytics` while implementation moved to package module `planner.py`.
- Broad analytics prompts are expected to execute without manual column clarification when dataset is analyzable; visualization contract auto-repair is internal-only and non-breaking.
- For broad full-analysis requests, compose stage may be skipped in favor of deterministic local formatter to avoid low-quality generic LLM summaries (`response_error_code=broad_query_local_formatter`).
- Route execution branches in RAG builder are now isolated in `app/services/chat/rag_prompt_routes.py` (internal refactor, planner semantics unchanged).
- Retrieval helper logic for narrative path is extracted to `app/services/chat/rag_retrieval_helpers.py` (no route decision changes).
- Narrative retrieval execution branch is isolated in `app/services/chat/rag_prompt_narrative.py`; planner contract remains unchanged.
- Deterministic SQL executor internals are extracted to `app/services/chat/tabular_sql_pipeline.py`; planner intent contract and `execute_tabular_sql_path` behavior are unchanged.
- Retrieval internals are extracted to `app/rag/retriever_helpers.py`; planner route contract and retrieval-mode behavior are unchanged.
- Full-file map-reduce prompt builder internals are extracted to `full_file_analysis_runtime.py`/`full_file_analysis_helpers.py`; planner route contract remains unchanged.

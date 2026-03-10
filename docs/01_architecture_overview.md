# 01. Architecture Overview

## Architectural Style
Service-oriented monolith with explicit subsystem boundaries:
- API layer (`app/api`).
- Domain/planning logic (`app/domain`).
- Application services (`app/services`).
- Infra adapters (`app/rag`, `app/services/llm/providers`, `app/services/ingestion`, `app/services/tabular`).

## Dependency Graph Summary (Repository Scan)
Scan date: `2026-03-05`.

- Python modules in `app/`: `101`.
- Internal import edges: `221`.
- Circular dependencies: `0` detected.

## Dependency Graph (Logical)
```mermaid
flowchart LR
  API[app/api] --> ORCH[app/services/chat_orchestrator]
  ORCH --> CHAT[app/services/chat/*]
  CHAT --> DOMAIN[app/domain/chat/query_planner]
  CHAT --> TAB[app/services/tabular/*]
  CHAT --> CAX[app/services/chat/complex_analytics/*]
  CHAT --> RAG[app/rag/*]
  ORCH --> LLM[app/services/llm/*]
  API --> FILES[app/services/file]
  FILES --> ING[app/services/ingestion/*]
  FILES --> RAG
  FILES --> TAB
  API --> DB[app/db + app/crud]
  ORCH --> DB
  API --> OBS[app/observability/*]
  ORCH --> OBS
  FILES --> OBS
```

### Core modules (high fan-in)
- `app/core/config.py`
- `app/observability/metrics.py`
- `app/db/session.py`
- `app/db/models/*`

### Service modules (high fan-out)
- `app/services/file.py`
- `app/services/file_pipeline.py`
- `app/services/chat_orchestrator.py`
- `app/services/chat/rag_prompt_builder.py`
- `app/services/chat/tabular_sql.py`
- `app/services/chat/tabular_sql_pipeline.py`
- `app/services/chat/full_file_analysis.py`
- `app/services/chat/full_file_analysis_runtime.py`
- `app/services/chat/full_file_analysis_helpers.py`
- `app/rag/retriever.py`
- `app/rag/retriever_helpers.py`
- `app/services/ingestion/sqlite_queue.py`
- `app/services/ingestion/sqlite_queue_runtime.py`
- `app/services/llm/manager.py`

### Cross-module coupling
- Chat orchestration depends on planner, RAG assembly, LLM routing, CRUD, and post-processing.
- Ingestion service couples queue runtime + document parsing + embeddings + vector store + tabular dataset generation.
- Tabular SQL path couples planner intent semantics with runtime guardrails and execution limits.

## Dead/Unused Candidates
No static circular deps, but probable unused modules:
- `app/core/exceptions.py`
- `app/observability/logging.py`

Validation method: no import references found via `rg` across `app/` and `tests/`.

## Risks
- `file` ingestion flow still has high integration surface (DB, embeddings, vector store, tabular metadata), even after modular split.
- Routing policy and deterministic SQL safety are spread across multiple modules and require contract-level tests to stay safe.

## Chat Lifecycle
```mermaid
sequenceDiagram
  participant C as Client
  participant API as /api/v1/chat
  participant O as ChatOrchestrator
  participant P as QueryPlanner
  participant R as RAG/Tabular Path
  participant CAX as Complex Analytics Executor
  participant M as ModelRouter
  participant H as AI HUB
  participant F as Ollama
  participant DB as DB

  C->>API: ChatMessage
  API->>O: chat()
  O->>DB: persist user message
  O->>P: plan_query()
  P-->>O: route + intent
  O->>R: build_rag_prompt()/execute_tabular_sql_path()
  alt route=complex_analytics
    R->>CAX: plan -> codegen -> sandbox -> compose
    CAX-->>O: direct response + artifacts + executor debug
  else route=deterministic/narrative
    R-->>O: final_prompt + rag_debug
    O->>M: generate_response()
    M->>H: primary attempt
    H-->>M: response or outage
    M->>F: fallback if policy allows
    M-->>O: response + route telemetry
  end
  O->>DB: persist assistant message
  O-->>API: ChatResponse
  API-->>C: response
```

## Architectural Fit vs Baseline (`docs/11`)
Implemented and aligned:
- AI HUB-first routing with policy-gated Ollama fallback.
- Durable ingestion queue.
- Deterministic tabular path with guardrails.
- Planner split deterministic vs narrative.
- Complex analytics sandbox path with direct short-circuit response, artifacts, and language-aware report formatting.
- Coverage/SLO instrumentation.
- Eval framework + CI gates.

Not fully aligned with target clean boundaries:
- Domain/use-case/adapters separation is partial, not strict.
- Several orchestration modules still combine policy and integration details.

## Update 2026-03-06
- Complex analytics execution plane is now explicit and deterministic in sequence:
  - planning prompt generation,
  - Python code generation,
  - secure sandbox execution,
  - final response composition.
- For broad analytics prompts with required charts, codegen now applies safe auto-repair when generated code misses visualization contract (`save_plot(...)`) before template fallback.
- Compose stage now has a quality gate: weak/non-informative LLM report text falls back to local structured formatter built from executed metrics/artifacts.
- AI HUB policy latency in complex analytics now uses provider-aware timeout overrides on plan/codegen/compose stages (`max(base, aihub_policy_override)` per stage) to reduce false timeout fallbacks.
- Internal architecture for this route is modularized into:
  - `planner.py`, `codegen.py`, `sandbox.py`, `executor.py`, `composer.py`, `artifacts.py`, `errors.py`, `telemetry.py`,
  - `dataset_context.py`, `template_codegen.py`, `report_quality.py`, `localization.py`, `auto_visual_patch.py`, `executor_support.py`.
- Public import contract remains stable via `app.services.chat.complex_analytics`:
  - `execute_complex_analytics_path`
  - `is_complex_analytics_query`
- Debug contract includes non-breaking execution details for this repair path:
  - `complex_analytics.codegen_auto_visual_patch_applied`
  - `complex_analytics.complex_analytics_codegen.auto_visual_patch_applied`
  - `complex_analytics.response_status=fallback`
  - `complex_analytics.response_error_code=broad_query_local_formatter` for broad full-analysis prompts handled by deterministic local formatter.
- Next oversized-file split in chat plane:
  - `chat_orchestrator.py` extracted reusable helpers to `app/services/chat/orchestrator_helpers.py`,
  - `rag_prompt_builder.py` extracted route handlers to `app/services/chat/rag_prompt_routes.py`.
- Follow-up extraction:
  - stream/non-stream chat runtime moved to `app/services/chat/orchestrator_runtime.py`,
  - grouped retrieval/context helpers moved to `app/services/chat/rag_retrieval_helpers.py`.
- Narrative retrieval orchestration branch moved to `app/services/chat/rag_prompt_narrative.py`.
- File ingestion orchestration extracted from `app/services/file.py` to `app/services/file_pipeline.py` via dependency injection.
  - `file.py` now acts as compatibility and runtime wiring layer for ingestion worker and service API.
  - `_process_file(...)` and `_finalize_ingestion(...)` signatures are unchanged.
- Deterministic SQL execution internals extracted from `app/services/chat/tabular_sql.py` to `app/services/chat/tabular_sql_pipeline.py`.
  - `execute_tabular_sql_path(...)` and intent helpers are unchanged.
  - Existing monkeypatch hooks used in tests (`_build_sql`, `_execute_aggregate_sync`) are preserved in `tabular_sql.py`.
- RAG retriever helper logic extracted from `app/rag/retriever.py` to `app/rag/retriever_helpers.py`.
  - `RAGRetriever` public methods (`retrieve`, `retrieve_full_file`, `query_rag`, `build_context_prompt`) remain unchanged.
  - Hybrid/full-file retrieval behavior and debug payload schema are unchanged.
- Full-file map-reduce prompt builder extracted into:
  - `app/services/chat/full_file_analysis_runtime.py` (orchestration),
  - `app/services/chat/full_file_analysis_helpers.py` (batch/range/structured-merge helpers),
  - `app/services/chat/full_file_analysis.py` kept as compatibility facade.
- Durable ingestion SQLite queue internals extracted from `app/services/ingestion/sqlite_queue.py` to `app/services/ingestion/sqlite_queue_runtime.py`.
  - `SqliteIngestionQueueAdapter` async contract is unchanged.
- Complex analytics compose-stage runtime extracted from `executor.py` to `executor_compose.py`.
  - `execute_complex_analytics_path(...)` contract and debug shape are unchanged.
- Behavior and API contracts unchanged; extraction is internal-only.

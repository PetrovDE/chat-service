# ADR-008: Query Planner Split (Deterministic vs Narrative)

Date: 2026-03-04

## What

Introduced a dedicated query planning domain module:

- `app/domain/chat/query_planner.py`

Planner now returns a structured decision contract:

- `route` (`deterministic_analytics | narrative_retrieval`)
- `intent`
- `confidence`
- `requires_clarification`
- `reason_codes`

Additional decision metadata:

- `metric_critical` flag
- `clarification_prompt` for explicit clarification-only flow

Integration changes:

- `app/services/chat/rag_prompt_builder.py` now asks planner first, then executes deterministic SQL or narrative retrieval based on planner route.
- `app/services/chat_orchestrator.py` short-circuits model generation when planner marks `requires_clarification=true`.
- `app/services/chat/tabular_sql.py` delegates intent detection to planner logic to keep a single planning source of truth.

## Why

- baseline architecture requires explicit split between deterministic analytics and narrative retrieval;
- route choice and ambiguity handling must be explainable, testable, and observable;
- metric-critical ambiguous requests must not be guessed by generation path.

## Trade-offs

- more domain contracts and integration points (planner + orchestrator short-circuit);
- slightly stricter routing may ask clarification earlier for ambiguous metric-critical prompts;
- better correctness and debuggability, at the cost of additional planning logic maintenance.


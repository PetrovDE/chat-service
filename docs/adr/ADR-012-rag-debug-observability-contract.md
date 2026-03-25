# ADR-012: Stabilize RAG Debug and Observability Contract

## Context
RAG troubleshooting depends on runtime diagnostics (retrieved chunks, filter context, similarity, coverage, provider prompt truncation). Previously these details were spread across modules and not documented as a single operational contract.

## Decision
Treat `rag_debug` payload and SLO metrics as stable operational contracts. Document required fields, diagnostics workflow, and runbooks (`docs/09_observability.md`, `docs/rag_debugging.md`, `docs/runbooks/rag_degradation.md`).

## Consequences
- Faster triage for retrieval/coverage regressions.
- Better reproducibility for incidents and eval failures.
- Tighter coupling between API response debug fields and monitoring dashboards.
- Any breaking change in debug fields now requires changelog + docs update.

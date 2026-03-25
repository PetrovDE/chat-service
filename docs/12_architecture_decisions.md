# 12. Architecture Decisions

This index consolidates architecture decisions implemented in code.

## Active ADRs
- `ADR-005` AI HUB-first router with policy-gated fallback.
- `ADR-006` Durable ingestion with SQLite queue and lease/retry/dead-letter semantics.
- `ADR-007` Shared DuckDB/Parquet tabular runtime with provenance metadata.
- `ADR-008` Query planner split deterministic vs narrative.
- `ADR-009` SQL guardrails and bounded deterministic execution.
- `ADR-010` Pre-prod hardening verification strategy.
- `ADR-011` Documentation reconciliation as source-of-truth architecture map.
- `ADR-012` RAG debug + observability contract stabilization.
- `ADR-013` Provider selection precedence and explicit routing mode.
- `ADR-014` Complex analytics sandbox executor.

## How to Add New ADR
Use format:
- `Context`
- `Decision`
- `Consequences`

Store file under `docs/adr/ADR-XXX-<slug>.md` and reference here.

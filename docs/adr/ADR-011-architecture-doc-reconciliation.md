# ADR-011: Architecture Documentation Reconciliation as Source of Truth

## Context
Documentation in `docs/` was produced incrementally across P0-P8 and drifted from current implementation details (module boundaries, runtime pipelines, routing guardrails, and debug contracts).

## Decision
Adopt a canonical architecture documentation set (`docs/00..12` + runbooks + `rag_debugging.md`) that is directly reconciled to current code paths and settings. Keep historical documents, but mark outdated planning/architecture docs as deprecated and link to canonical documents.

## Consequences
- New developers can onboard from docs without full code read.
- Architecture audits become diff-driven against stable canonical files.
- Historical context is preserved without mixing with current contracts.
- Requires discipline to update canonical docs in each architecture-impacting change.

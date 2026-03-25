# ADR-009: SQL Guardrails and Bounded Execution for Deterministic Tabular Path

Date: 2026-03-04
Status: Accepted
Related: `docs/11_llm_file_chat_best_practices_architecture.md`, `docs/13_offline_refactor_gap_analysis.md`

## What
- Added explicit SQL guardrail policy for deterministic tabular path:
  - allowlist root operations: `SELECT`, `WITH`;
  - block dangerous keywords/patterns and multi-statement/comment payloads.
- Added bounded execution limits:
  - query timeout,
  - max scanned rows,
  - max result rows,
  - max result payload size (bytes).
- Added typed deterministic SQL error classification for API/debug:
  - `sql_guardrail_blocked`,
  - `sql_scan_limit_exceeded`,
  - `sql_result_limit_exceeded`,
  - `sql_result_size_exceeded`,
  - `sql_timeout`,
  - `sql_execution_failed`.
- Added trace fields in `rag_debug.tabular_sql`:
  - `executed_sql`,
  - `policy_decision`,
  - `guardrail_flags`.

## Why
- Target architecture requires deterministic analytics path to be safe, bounded, and observable.
- Silent fallback from deterministic SQL to narrative path can hide numeric reliability regressions.
- Explicit classification and trace metadata are required for incident triage and API-level transparency.

## Trade-offs
- More strict SQL policy can reject borderline queries that previously executed.
- Additional checks/metadata introduce small runtime overhead.
- Deterministic path now returns classified errors instead of silent narrative fallback, which can increase visible clarification responses but improves correctness guarantees.

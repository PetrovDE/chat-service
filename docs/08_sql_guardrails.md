# 08. SQL Guardrails

## Enforcement Scope
`app/services/tabular/sql_guardrails.py` enforces safety for deterministic SQL path.

## Allowlist
Allowed statement families only:
- `SELECT`
- `WITH ... SELECT`

## Blocked
- Non-read operations (`insert`, `update`, `delete`, `drop`, `alter`, `create`, etc.).
- Dangerous external read patterns (`read_csv`, `read_parquet`, `httpfs`, etc.).
- SQL comments.
- Multiple statements.

## Guardrails Policy Output
`enforce()` returns:
- final SQL (LIMIT appended when required)
- debug payload with `policy_decision` and `guardrail_flags`

## Bounded Execution Limits
Configured via settings:
- `TABULAR_SQL_TIMEOUT_SECONDS`
- `TABULAR_SQL_MAX_RESULT_ROWS`
- `TABULAR_SQL_MAX_RESULT_BYTES`
- `TABULAR_SQL_MAX_SCANNED_ROWS`
- `TABULAR_SQL_MAX_CHARS`

## Error Codes
- `sql_guardrail_blocked`
- `sql_scan_limit_exceeded`
- `sql_result_limit_exceeded`
- `sql_result_size_exceeded`
- `sql_timeout`
- `sql_execution_failed`

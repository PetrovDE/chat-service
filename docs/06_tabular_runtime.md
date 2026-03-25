# 06. Tabular Runtime

## Runtime Engines
Resolved from `file.custom_metadata`:
- `duckdb_parquet` (shared catalog + per-file Parquet).

## Dataset Metadata Contract
`custom_metadata.tabular_dataset` includes:
- `dataset_id`, `dataset_version`, `dataset_provenance_id`
- `catalog_path`, `dataset_root`
- tables: `table_name`, `sheet_name`, `row_count`, `columns`, `column_aliases`, `table_version`, `provenance_id`, `parquet_path`

## Execution Session
`TabularExecutionSession`:
- DuckDB in-memory view over parquet table.
- Result bounds validated by `SQLExecutionLimits`.

## Query Types
Implemented in `app/services/chat/tabular_sql.py`:
- Aggregate path (`count/sum/avg/min/max`, optional group by).
- Profile path (column-level stats, sample rows).

## Bounded Execution
- Timeout via `TABULAR_SQL_TIMEOUT_SECONDS`.
- Max result rows/bytes and scan rows limits.
- Structured error payloads (`sql_timeout`, `sql_guardrail_blocked`, etc.).

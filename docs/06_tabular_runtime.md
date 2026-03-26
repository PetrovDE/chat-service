# 06. Tabular Runtime

## Runtime Engines
Resolved from `file.custom_metadata`:
- `duckdb_parquet` (shared catalog + per-file Parquet).

## Dataset Metadata Contract
`custom_metadata.tabular_dataset` includes:
- `dataset_id`, `dataset_version`, `dataset_provenance_id`
- `column_metadata_contract_version` (`tabular_column_metadata_v1`)
- `column_metadata_stats` (aggregated alias/sample/budget counts)
- `catalog_path`, `dataset_root`
- tables: `table_name`, `sheet_name`, `row_count`, `columns`, `column_aliases`, `table_version`, `provenance_id`, `parquet_path`
- table metadata contract fields:
  - `tables[].column_metadata_contract_version`
  - `tables[].column_metadata_stats`
- optional per-column metadata for schema-first resolution:
  - `column_metadata.<column>.raw_name`
  - `column_metadata.<column>.normalized_name`
  - `column_metadata.<column>.display_name`
  - `column_metadata.<column>.aliases[]`
  - `column_metadata.<column>.dtype`
  - `column_metadata.<column>.sample_values[]`
  - `column_metadata.<column>.cardinality_hint` (`empty|single|low|medium|high`)

## Execution Session
`TabularExecutionSession`:
- DuckDB in-memory view over parquet table.
- Result bounds validated by `SQLExecutionLimits`.

## Query Types
Implemented in `app/services/chat/tabular_sql.py`:
- Aggregate path (`count/sum/avg/min/max`, optional group by).
- Profile path (column-level stats, sample rows).
- Lookup path (schema-first filter/value lookup).
- Chart path (schema-first dimension resolution + chart artifact materialization).

## Field Resolution Contract

Runtime field selection is schema-first and metadata-driven:
- candidates are built from actual dataset schema only (`columns`, `column_aliases`, optional `column_metadata`)
- scoring signals include:
  - exact normalized name match
  - display-name match
  - metadata alias match
  - token overlap and fuzzy similarity
  - dtype compatibility (when expected dtype is known)
  - sample-value evidence (penalty when requested text looks like a value instead of a field)
- explicit confidence thresholds are required:
  - weak score -> `no_match`
  - close competing candidates -> `ambiguous`
- weak/ambiguous outcomes must return controlled mismatch/clarification flows
- no silent guessed substitution to first column or "best effort" defaults

Metadata parsing contract:

- runtime sanitizes incoming column metadata through one shared parser/sanitizer path,
- runtime preserves explicit budget bounds from ingestion (no ad-hoc unbounded reconstruction),
- if metadata is missing/partial, runtime keeps explicit schema fields and returns controlled no-match/ambiguous outcomes.

## Bounded Execution
- Timeout via `TABULAR_SQL_TIMEOUT_SECONDS`.
- Max result rows/bytes and scan rows limits.
- Structured error payloads (`sql_timeout`, `sql_guardrail_blocked`, etc.).

## Update 2026-03-26 (Targeted Runtime Cleanup)

Deterministic tabular internals were split to reduce mixed responsibilities in hot modules:

- `app/services/chat/tabular_sql_query_planner.py`
  - owns aggregate/lookup SQL plan construction from parsed query + schema-first field matching.
- `app/services/chat/tabular_sql_route_payloads.py`
  - owns route-level tabular debug payload application and controlled schema/missing-column response payload builders.
- `app/services/chat/tabular_deterministic_result.py`
  - owns deterministic success-route debug/fallback/result shaping used by `rag_prompt_routes`.

Compatibility contract:
- `execute_tabular_sql_path(...)` remains the stable deterministic entrypoint.
- Existing tabular debug/fallback fields are preserved.
- No silent fallback guesses were introduced.

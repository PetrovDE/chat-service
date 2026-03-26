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

## Update 2026-03-26 (Tabular Scope Selection UX Pass)

Deterministic tabular scope selection (file + sheet/table) now applies explicit ranking and ambiguity handling:

- ranking signals:
  - filename mention overlap,
  - table/sheet surface-name overlap,
  - column mention overlap,
  - bounded row-count bonus only as tie support.
- scope selection outcomes:
  - `selected`
  - `ambiguous_file`
  - `ambiguous_table`
  - `no_tabular_dataset`
- ambiguous file/sheet scope now returns controlled clarification (concise options), instead of implicit first-file/first-sheet selection.
- deterministic route keeps explicit no-guess behavior for columns/tables/sheets/files.
- schema/file-summary queries (`schema_question`) are allowed to return a multi-sheet summary context for the selected file when sheet ranking is ambiguous, so users can see available sheets/tables and pick a next step without silent sheet selection.

Scope observability fields (debug, additive and backward compatible):

- `scope_selection_status`
- `scope_selected_file_id`
- `scope_selected_file_name`
- `scope_selected_table_name`
- `scope_selected_sheet_name`
- `scope_file_candidates`
- `table_scope_candidates`

## Update 2026-03-26 (Derived Temporal Dimensions and Follow-Up Continuity)

Deterministic tabular analytics now supports derived temporal grouping without requiring a literal `month` or `year` column:

- supported requested time grains: `day|week|month|quarter|year`
- source datetime resolution is schema-first and metadata-driven:
  - uses real schema columns, aliases, and sanitized column metadata only
  - uses dtype compatibility and confidence/ambiguity thresholds
  - does not use domain-specific hardcoded hints
- if no confident datetime source is available, runtime returns explicit controlled clarification/mismatch behavior (`no_datetime_source` or `ambiguous_datetime_source`)
- runtime does not silently invent or guess derived columns

Temporal planning contract (additive):

- `requested_time_grain`
- `source_datetime_field`
- `derived_grouping_dimension`
- `temporal_plan_status`
- `temporal_aggregation_plan`

Chart execution contract for temporal requests:

- if temporal plan is `resolved` and required measure/dimension can be matched, deterministic chart SQL executes
- supported executable requests do not degrade to code-generation response mode
- fallback remains explicit and controlled only when execution preconditions are not met

Short follow-up continuity contract for tabular requests:

- short refinement turns (for example `use created_at`, `group by month from dates`) can reuse prior tabular user intent
- follow-up reuse is constrained to prior tabular intent and short refinement messages; it is not applied globally

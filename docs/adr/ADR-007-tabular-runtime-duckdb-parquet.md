# ADR-007: Shared Tabular Runtime on DuckDB/Parquet

Date: 2026-03-04

## What

Migrated tabular deterministic runtime from per-file SQLite sidecars to a shared DuckDB/Parquet adapter:

- ingestion normalization moved to `app/services/tabular/normalization.py`;
- shared tabular storage adapter introduced in `app/services/tabular/storage_adapter.py`;
- SQL execution isolated in `app/services/tabular/sql_execution.py`;
- SQL guardrails isolated in `app/services/tabular/sql_guardrails.py`;
- deterministic chat path in `app/services/chat/tabular_sql.py` now executes against versioned `tabular_dataset` metadata.

New ingestion metadata (stored in `files.custom_metadata.tabular_dataset`) includes:

- `dataset_id`, `dataset_version`, `dataset_provenance_id`;
- per-table `table_version`, `provenance_id`, `parquet_path`;
- normalized schema (`columns`, `column_aliases`).

## Why

- target architecture baseline requires shared tabular runtime (`DuckDB/Parquet`) instead of isolated SQLite sidecars;
- deterministic SQL/profile path must remain reproducible and explainable;
- versioning/provenance is required for auditability and rollback-safe migration;
- separating normalization/storage/execution/guardrails reduces coupling and makes P4/P5 planner hardening simpler.

## Trade-offs

- additional runtime dependency (`duckdb`);
- more moving parts (catalog + parquet dataset lifecycle);
- migration period keeps legacy SQLite read-compatibility to avoid breaking already processed files.

Accepted because operational traceability and deterministic reproducibility outweigh added implementation complexity.


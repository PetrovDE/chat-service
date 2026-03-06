from __future__ import annotations

import sqlite3
from typing import Any, Dict, Optional, Tuple

from app.core.config import settings
from app.services.tabular.sql_execution import (
    ResolvedTabularDataset,
    ResolvedTabularTable,
    resolve_tabular_dataset,
)


def quote_ident(name: str) -> str:
    return '"' + str(name or "").replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def resolve_table_for_query(*, query: str, dataset: ResolvedTabularDataset) -> Optional[ResolvedTabularTable]:
    if not dataset.tables:
        return None
    q = (query or "").lower()
    for table in dataset.tables:
        if table.table_name.lower() in q or table.sheet_name.lower() in q:
            return table
    return max(dataset.tables, key=lambda t: int(t.row_count or 0))


def load_table_dataframe(
    *,
    dataset: ResolvedTabularDataset,
    table: ResolvedTabularTable,
    max_rows: int,
) -> Any:
    try:
        import duckdb  # noqa: PLC0415
        import pandas as pd  # noqa: PLC0415
    except Exception as exc:  # pragma: no cover - dependency check path
        raise RuntimeError("pandas and duckdb are required for complex analytics executor") from exc

    table_q = quote_ident(table.table_name)
    limit_clause = f" LIMIT {int(max_rows)}" if int(max_rows) > 0 else ""

    if dataset.engine == "duckdb_parquet":
        if table.parquet_path is None:
            raise RuntimeError(f"Missing parquet path for table {table.table_name}")
        conn = duckdb.connect(database=":memory:")
        try:
            sql = f"SELECT * FROM read_parquet({sql_literal(str(table.parquet_path))}){limit_clause}"
            return conn.execute(sql).df()
        finally:
            conn.close()

    if dataset.engine == "sqlite_legacy":
        if dataset.sqlite_path is None:
            raise RuntimeError("Missing SQLite path for legacy tabular dataset")
        sqlite_conn = sqlite3.connect(str(dataset.sqlite_path))
        try:
            sql = f"SELECT * FROM {table_q}{limit_clause}"
            return pd.read_sql_query(sql, sqlite_conn)
        finally:
            sqlite_conn.close()

    raise RuntimeError(f"Unsupported tabular dataset engine: {dataset.engine}")


def collect_datasets_for_file(file_obj: Any) -> Optional[Tuple[ResolvedTabularDataset, Dict[str, Any]]]:
    dataset = resolve_tabular_dataset(file_obj)
    if dataset is None or not dataset.tables:
        return None

    frames: Dict[str, Any] = {}
    max_rows = int(settings.COMPLEX_ANALYTICS_MAX_ROWS)
    for table in dataset.tables:
        frames[table.table_name] = load_table_dataframe(dataset=dataset, table=table, max_rows=max_rows)
    return dataset, frames


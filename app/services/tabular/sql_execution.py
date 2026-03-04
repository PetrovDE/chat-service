from __future__ import annotations

from dataclasses import dataclass
import json
import sqlite3
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.services.tabular.sql_errors import (
    SQL_ERROR_EXECUTION_FAILED,
    SQL_ERROR_RESULT_LIMIT_EXCEEDED,
    SQL_ERROR_RESULT_SIZE_EXCEEDED,
    SQL_ERROR_TIMEOUT,
    TabularSQLException,
)

try:
    import duckdb
except Exception:  # pragma: no cover - import error path is runtime-dependent
    duckdb = None


def _quote_ident(name: str) -> str:
    return '"' + str(name or "").replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


@dataclass
class ResolvedTabularTable:
    table_name: str
    sheet_name: str
    row_count: int
    columns: List[str]
    column_aliases: Dict[str, str]
    table_version: int
    provenance_id: Optional[str]
    parquet_path: Optional[Path]


@dataclass
class ResolvedTabularDataset:
    engine: str
    dataset_id: Optional[str]
    dataset_version: Optional[int]
    dataset_provenance_id: Optional[str]
    tables: List[ResolvedTabularTable]
    catalog_path: Optional[Path]
    sqlite_path: Optional[Path]


@dataclass(frozen=True)
class SQLExecutionLimits:
    max_result_rows: int
    max_result_bytes: int


def _estimate_rows_size_bytes(rows: Sequence[Tuple[Any, ...]]) -> int:
    total = 0
    for row in rows:
        total += 2  # [] delimiters
        for value in row:
            if value is None:
                text = "null"
            elif isinstance(value, bool):
                text = "true" if value else "false"
            else:
                text = str(value)
            total += len(text.encode("utf-8")) + 1
    return total


class TabularExecutionSession:
    def __init__(
        self,
        *,
        dataset: ResolvedTabularDataset,
        table: ResolvedTabularTable,
        limits: Optional[SQLExecutionLimits] = None,
    ) -> None:
        self.dataset = dataset
        self.table = table
        self.limits = limits
        self._duckdb_conn = None
        self._sqlite_conn = None

    def __enter__(self) -> "TabularExecutionSession":
        if self.dataset.engine == "duckdb_parquet":
            if duckdb is None:
                raise RuntimeError("duckdb package is required for tabular SQL execution")
            if self.table.parquet_path is None:
                raise ValueError("Missing parquet path for duckdb tabular execution")
            self._duckdb_conn = duckdb.connect(database=":memory:")
            self._duckdb_conn.execute("PRAGMA threads=1")
            self._duckdb_conn.execute(
                f"CREATE VIEW {_quote_ident(self.table.table_name)} AS "
                f"SELECT * FROM read_parquet({_sql_literal(str(self.table.parquet_path))})"
            )
            return self

        if self.dataset.engine == "sqlite_legacy":
            if self.dataset.sqlite_path is None:
                raise ValueError("Missing sqlite path for legacy SQL execution")
            self._sqlite_conn = sqlite3.connect(str(self.dataset.sqlite_path))
            return self

        raise ValueError(f"Unsupported tabular engine: {self.dataset.engine}")

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        if self._duckdb_conn is not None:
            self._duckdb_conn.close()
            self._duckdb_conn = None
        if self._sqlite_conn is not None:
            self._sqlite_conn.close()
            self._sqlite_conn = None

    def _validate_result_bounds(
        self,
        *,
        rows: Sequence[Tuple[Any, ...]],
        sql: str,
        max_result_rows: Optional[int],
        max_result_bytes: Optional[int],
    ) -> None:
        if max_result_rows is not None and int(max_result_rows) > 0 and len(rows) > int(max_result_rows):
            raise TabularSQLException(
                code=SQL_ERROR_RESULT_LIMIT_EXCEEDED,
                message="Tabular SQL result row count exceeds configured limit",
                details={
                    "max_result_rows": int(max_result_rows),
                    "actual_result_rows": len(rows),
                },
                executed_sql=sql,
            )

        if max_result_bytes is not None and int(max_result_bytes) > 0:
            estimated_bytes = _estimate_rows_size_bytes(rows)
            if estimated_bytes > int(max_result_bytes):
                raise TabularSQLException(
                    code=SQL_ERROR_RESULT_SIZE_EXCEEDED,
                    message="Tabular SQL result size exceeds configured limit",
                    details={
                        "max_result_bytes": int(max_result_bytes),
                        "actual_result_bytes": int(estimated_bytes),
                    },
                    executed_sql=sql,
                )

    def execute(
        self,
        sql: str,
        *,
        timeout_seconds: Optional[float] = None,
        max_result_rows: Optional[int] = None,
        max_result_bytes: Optional[int] = None,
    ) -> List[Tuple[Any, ...]]:
        effective_max_rows = max_result_rows
        effective_max_bytes = max_result_bytes
        if self.limits is not None:
            if effective_max_rows is None:
                effective_max_rows = int(self.limits.max_result_rows)
            if effective_max_bytes is None:
                effective_max_bytes = int(self.limits.max_result_bytes)

        if self._duckdb_conn is not None:
            try:
                rows = self._duckdb_conn.execute(sql).fetchall()
                parsed = [tuple(row) for row in rows]
                self._validate_result_bounds(
                    rows=parsed,
                    sql=sql,
                    max_result_rows=effective_max_rows,
                    max_result_bytes=effective_max_bytes,
                )
                return parsed
            except TabularSQLException:
                raise
            except Exception as exc:
                raise TabularSQLException(
                    code=SQL_ERROR_EXECUTION_FAILED,
                    message="Tabular SQL execution failed",
                    details={"exception_type": type(exc).__name__},
                    executed_sql=sql,
                ) from exc
        if self._sqlite_conn is not None:
            cur = self._sqlite_conn.cursor()
            start = perf_counter()

            timeout = float(timeout_seconds or 0.0)
            if timeout > 0:
                def _progress_handler() -> int:
                    return 1 if (perf_counter() - start) > timeout else 0

                self._sqlite_conn.set_progress_handler(_progress_handler, 1000)

            try:
                cur.execute(sql)
                rows = cur.fetchall()
                parsed = [tuple(row) for row in rows]
                self._validate_result_bounds(
                    rows=parsed,
                    sql=sql,
                    max_result_rows=effective_max_rows,
                    max_result_bytes=effective_max_bytes,
                )
                return parsed
            except TabularSQLException:
                raise
            except sqlite3.OperationalError as exc:
                lowered = str(exc).lower()
                if timeout > 0 and "interrupted" in lowered:
                    raise TabularSQLException(
                        code=SQL_ERROR_TIMEOUT,
                        message="Tabular SQL execution timeout",
                        details={"timeout_seconds": timeout},
                        executed_sql=sql,
                    ) from exc
                raise TabularSQLException(
                    code=SQL_ERROR_EXECUTION_FAILED,
                    message="Tabular SQL execution failed",
                    details={"exception_type": type(exc).__name__, "message": str(exc)},
                    executed_sql=sql,
                ) from exc
            except Exception as exc:
                raise TabularSQLException(
                    code=SQL_ERROR_EXECUTION_FAILED,
                    message="Tabular SQL execution failed",
                    details={"exception_type": type(exc).__name__},
                    executed_sql=sql,
                ) from exc
            finally:
                if timeout > 0:
                    self._sqlite_conn.set_progress_handler(None, 0)
                cur.close()
        raise RuntimeError("TabularExecutionSession is not initialized")


def _parse_table_entry(raw: Dict[str, Any]) -> Optional[ResolvedTabularTable]:
    table_name = str(raw.get("table_name") or "").strip()
    if not table_name:
        return None

    raw_columns = raw.get("columns")
    if isinstance(raw_columns, list):
        columns = [str(col) for col in raw_columns]
    else:
        columns = []

    raw_aliases = raw.get("column_aliases")
    if isinstance(raw_aliases, dict):
        column_aliases = {str(k): str(v) for k, v in raw_aliases.items()}
    else:
        column_aliases = {}

    parquet_path = None
    raw_path = raw.get("parquet_path")
    if raw_path:
        path = Path(str(raw_path)).expanduser()
        if path.exists():
            parquet_path = path.resolve()

    return ResolvedTabularTable(
        table_name=table_name,
        sheet_name=str(raw.get("sheet_name") or ""),
        row_count=int(raw.get("row_count", 0) or 0),
        columns=columns,
        column_aliases=column_aliases,
        table_version=int(raw.get("table_version", 1) or 1),
        provenance_id=str(raw.get("provenance_id")) if raw.get("provenance_id") else None,
        parquet_path=parquet_path,
    )


def resolve_tabular_dataset(file_obj: Any) -> Optional[ResolvedTabularDataset]:
    metadata = getattr(file_obj, "custom_metadata", None)
    if not isinstance(metadata, dict):
        return None

    dataset = metadata.get("tabular_dataset")
    if isinstance(dataset, dict):
        tables_raw = dataset.get("tables")
        if not isinstance(tables_raw, list):
            tables_raw = []
        tables: List[ResolvedTabularTable] = []
        for raw in tables_raw:
            if not isinstance(raw, dict):
                continue
            parsed = _parse_table_entry(raw)
            if parsed is not None:
                tables.append(parsed)
        if tables:
            catalog_path = None
            if dataset.get("catalog_path"):
                candidate = Path(str(dataset.get("catalog_path"))).expanduser()
                if candidate.exists():
                    catalog_path = candidate.resolve()
            return ResolvedTabularDataset(
                engine="duckdb_parquet",
                dataset_id=str(dataset.get("dataset_id") or ""),
                dataset_version=int(dataset.get("dataset_version", 0) or 0),
                dataset_provenance_id=str(dataset.get("dataset_provenance_id") or ""),
                tables=tables,
                catalog_path=catalog_path,
                sqlite_path=None,
            )

    sidecar = metadata.get("tabular_sidecar")
    if isinstance(sidecar, dict):
        raw_path = str(sidecar.get("path") or "")
        path = Path(raw_path).expanduser()
        raw_tables = sidecar.get("tables") if isinstance(sidecar.get("tables"), list) else []
        if path.exists() and raw_tables:
            tables: List[ResolvedTabularTable] = []
            for raw in raw_tables:
                if not isinstance(raw, dict):
                    continue
                parsed = _parse_table_entry(raw)
                if parsed is None:
                    continue
                if not parsed.columns and isinstance(raw.get("columns"), list):
                    parsed.columns = [str(col) for col in raw.get("columns")]
                tables.append(parsed)
            if tables:
                return ResolvedTabularDataset(
                    engine="sqlite_legacy",
                    dataset_id=str(sidecar.get("dataset_id") or ""),
                    dataset_version=int(sidecar.get("dataset_version", 1) or 1),
                    dataset_provenance_id=str(sidecar.get("dataset_provenance_id") or ""),
                    tables=tables,
                    catalog_path=None,
                    sqlite_path=path.resolve(),
                )

    return None


def rows_to_result_text(rows: Sequence[Tuple[Any, ...]]) -> str:
    out: List[List[Any]] = []
    for row in rows:
        parsed_row: List[Any] = []
        for value in row:
            if value is None:
                parsed_row.append(None)
            elif isinstance(value, (int, float, str, bool)):
                parsed_row.append(value)
            else:
                parsed_row.append(str(value))
        out.append(parsed_row)
    return json.dumps(out, ensure_ascii=False)

from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.services.tabular.column_metadata_contract import (
    TABULAR_COLUMN_METADATA_CONTRACT_VERSION,
    aggregate_tabular_column_metadata_stats,
    sanitize_tabular_column_metadata,
)
from app.services.tabular.sql_errors import (
    SQL_ERROR_EXECUTION_FAILED,
    SQL_ERROR_RESULT_LIMIT_EXCEEDED,
    SQL_ERROR_RESULT_SIZE_EXCEEDED,
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
    column_metadata: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    column_metadata_contract_version: Optional[str] = None
    column_metadata_stats: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResolvedTabularDataset:
    engine: str
    dataset_id: Optional[str]
    dataset_version: Optional[int]
    dataset_provenance_id: Optional[str]
    tables: List[ResolvedTabularTable]
    catalog_path: Optional[Path]
    column_metadata_contract_version: Optional[str] = None
    column_metadata_stats: Dict[str, Any] = field(default_factory=dict)


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

        raise ValueError(f"Unsupported tabular engine: {self.dataset.engine}")

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        if self._duckdb_conn is not None:
            self._duckdb_conn.close()
            self._duckdb_conn = None

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
        raise RuntimeError("TabularExecutionSession is not initialized")


def _parse_table_entry(
    raw: Dict[str, Any],
    *,
    dataset_metadata_contract_version: Optional[str],
) -> Optional[ResolvedTabularTable]:
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

    raw_metadata = raw.get("column_metadata")
    column_metadata, column_metadata_stats = sanitize_tabular_column_metadata(
        raw_metadata=raw_metadata,
        columns=columns,
        aliases=column_aliases,
    )
    metadata_contract_version = str(
        raw.get("column_metadata_contract_version")
        or dataset_metadata_contract_version
        or TABULAR_COLUMN_METADATA_CONTRACT_VERSION
    )

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
        column_metadata=column_metadata,
        table_version=int(raw.get("table_version", 1) or 1),
        provenance_id=str(raw.get("provenance_id")) if raw.get("provenance_id") else None,
        parquet_path=parquet_path,
        column_metadata_contract_version=metadata_contract_version,
        column_metadata_stats=column_metadata_stats,
    )


def resolve_tabular_dataset(file_obj: Any) -> Optional[ResolvedTabularDataset]:
    metadata = getattr(file_obj, "custom_metadata", None)
    if not isinstance(metadata, dict):
        return None

    dataset = metadata.get("tabular_dataset")
    if isinstance(dataset, dict):
        dataset_metadata_contract_version = str(
            dataset.get("column_metadata_contract_version") or TABULAR_COLUMN_METADATA_CONTRACT_VERSION
        )
        tables_raw = dataset.get("tables")
        if not isinstance(tables_raw, list):
            tables_raw = []
        tables: List[ResolvedTabularTable] = []
        for raw in tables_raw:
            if not isinstance(raw, dict):
                continue
            parsed = _parse_table_entry(
                raw,
                dataset_metadata_contract_version=dataset_metadata_contract_version,
            )
            if parsed is not None and parsed.parquet_path is not None:
                tables.append(parsed)
        if tables:
            dataset_metadata_stats = aggregate_tabular_column_metadata_stats(
                [table.column_metadata_stats for table in tables if isinstance(table.column_metadata_stats, dict)]
            )
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
                column_metadata_contract_version=dataset_metadata_contract_version,
                column_metadata_stats=dataset_metadata_stats,
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

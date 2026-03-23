from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import shutil
from threading import Lock
from typing import Any, Dict, List, Optional
from uuid import UUID

from app.core.config import settings
from app.services.tabular.normalization import NormalizedTabularTable, load_normalized_tables

logger = logging.getLogger(__name__)

try:
    import duckdb
except Exception:  # pragma: no cover - import error path is runtime-dependent
    duckdb = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _quote_ident(name: str) -> str:
    return '"' + str(name or "").replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _file_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


@dataclass
class TabularCleanupResult:
    datasets_deleted: int = 0
    tables_deleted: int = 0
    parquet_files_deleted: int = 0


class SharedDuckDBParquetStorageAdapter:
    def __init__(self, *, dataset_root: Path, catalog_path: Path) -> None:
        if duckdb is None:
            raise RuntimeError("duckdb package is required for tabular runtime")

        self.dataset_root = Path(dataset_root).resolve()
        self.catalog_path = Path(catalog_path).resolve()
        self.dataset_root.mkdir(parents=True, exist_ok=True)
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._ensure_catalog_schema()

    def _connect(self):
        return duckdb.connect(str(self.catalog_path))

    def _ensure_catalog_schema(self) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tabular_datasets (
                    dataset_id VARCHAR NOT NULL,
                    dataset_version BIGINT NOT NULL,
                    file_id VARCHAR NOT NULL,
                    source_file_path VARCHAR,
                    source_fingerprint VARCHAR,
                    dataset_provenance_id VARCHAR NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    runtime_engine VARCHAR NOT NULL,
                    PRIMARY KEY (dataset_id, dataset_version)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tabular_tables (
                    dataset_id VARCHAR NOT NULL,
                    dataset_version BIGINT NOT NULL,
                    table_name VARCHAR NOT NULL,
                    sheet_name VARCHAR,
                    row_count BIGINT NOT NULL,
                    columns_json VARCHAR NOT NULL,
                    column_aliases_json VARCHAR NOT NULL,
                    table_version BIGINT NOT NULL,
                    table_provenance_id VARCHAR NOT NULL,
                    parquet_path VARCHAR NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    PRIMARY KEY (dataset_id, dataset_version, table_name)
                )
                """
            )
        finally:
            conn.close()

    def _next_dataset_version(self, conn, dataset_id: str) -> int:
        row = conn.execute(
            "SELECT COALESCE(MAX(dataset_version), 0) + 1 FROM tabular_datasets WHERE dataset_id = ?",
            [dataset_id],
        ).fetchone()
        return int((row or [1])[0] or 1)

    def ingest(
        self,
        *,
        file_id: str,
        file_path: Path,
        file_type: str,
        source_filename: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        tables = load_normalized_tables(file_path=file_path, file_type=file_type)
        if not tables:
            return None

        dataset_id = str(file_id)
        fingerprint = _file_fingerprint(file_path)
        created_at = _utc_now_iso()

        with self._lock:
            conn = self._connect()
            try:
                dataset_version = self._next_dataset_version(conn, dataset_id=dataset_id)
                dataset_provenance_id = _sha256_hex(
                    f"{dataset_id}|{dataset_version}|{fingerprint}|{file_path}|{created_at}"
                )

                dataset_dir = self.dataset_root / dataset_id / f"v{dataset_version}"
                dataset_dir.mkdir(parents=True, exist_ok=True)

                conn.execute(
                    """
                    INSERT INTO tabular_datasets (
                        dataset_id, dataset_version, file_id, source_file_path,
                        source_fingerprint, dataset_provenance_id, created_at, runtime_engine
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        dataset_id,
                        dataset_version,
                        str(file_id),
                        str(file_path),
                        fingerprint,
                        dataset_provenance_id,
                        datetime.fromisoformat(created_at),
                        "duckdb_parquet",
                    ],
                )

                table_payloads: List[Dict[str, Any]] = []
                for index, table in enumerate(tables):
                    payload = self._store_table(
                        conn=conn,
                        dataset_id=dataset_id,
                        dataset_version=dataset_version,
                        dataset_dir=dataset_dir,
                        created_at=created_at,
                        source_fingerprint=fingerprint,
                        table=table,
                        index=index,
                    )
                    table_payloads.append(payload)

                return {
                    "engine": "duckdb_parquet",
                    "runtime": "shared_duckdb_parquet",
                    "runtime_schema_version": 1,
                    "catalog_path": str(self.catalog_path),
                    "dataset_root": str(self.dataset_root),
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "dataset_provenance_id": dataset_provenance_id,
                    "source_file_path": str(file_path),
                    "source_filename": source_filename,
                    "source_fingerprint": fingerprint,
                    "generated_at": created_at,
                    "tables": table_payloads,
                }
            finally:
                conn.close()

    def _store_table(
        self,
        *,
        conn,
        dataset_id: str,
        dataset_version: int,
        dataset_dir: Path,
        created_at: str,
        source_fingerprint: str,
        table: NormalizedTabularTable,
        index: int,
    ) -> Dict[str, Any]:
        parquet_path = (dataset_dir / f"{table.table_name}.parquet").resolve()
        view_name = f"_ingest_df_{index + 1}"
        table_version = int(dataset_version)
        table_provenance_id = _sha256_hex(
            f"{dataset_id}|{dataset_version}|{table.table_name}|{source_fingerprint}|{table.row_count}|{','.join(table.columns)}"
        )

        conn.register(view_name, table.dataframe)
        try:
            conn.execute(
                f"COPY (SELECT * FROM {_quote_ident(view_name)}) TO {_sql_literal(str(parquet_path))} (FORMAT PARQUET)"
            )
        finally:
            conn.unregister(view_name)

        conn.execute(
            """
            INSERT INTO tabular_tables (
                dataset_id, dataset_version, table_name, sheet_name, row_count,
                columns_json, column_aliases_json, table_version,
                table_provenance_id, parquet_path, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                dataset_id,
                dataset_version,
                table.table_name,
                table.sheet_name,
                int(table.row_count),
                json.dumps(table.columns, ensure_ascii=False),
                json.dumps(table.column_aliases, ensure_ascii=False),
                table_version,
                table_provenance_id,
                str(parquet_path),
                datetime.fromisoformat(created_at),
            ],
        )

        return {
            "table_name": table.table_name,
            "sheet_name": table.sheet_name,
            "row_count": int(table.row_count),
            "columns": list(table.columns),
            "column_aliases": dict(table.column_aliases),
            "table_version": table_version,
            "provenance_id": table_provenance_id,
            "parquet_path": str(parquet_path),
        }

    def cleanup_for_file(self, *, file_id: str, custom_metadata: Optional[Dict[str, Any]]) -> TabularCleanupResult:
        result = TabularCleanupResult()

        dataset_info = None
        if isinstance(custom_metadata, dict):
            data = custom_metadata.get("tabular_dataset")
            if isinstance(data, dict):
                dataset_info = data
        dataset_id = str((dataset_info or {}).get("dataset_id") or file_id)

        with self._lock:
            conn = self._connect()
            try:
                table_rows = conn.execute(
                    "SELECT parquet_path FROM tabular_tables WHERE dataset_id = ?",
                    [dataset_id],
                ).fetchall()
                for row in table_rows:
                    if not row:
                        continue
                    path = Path(str(row[0]))
                    try:
                        if path.exists():
                            path.unlink()
                            result.parquet_files_deleted += 1
                    except Exception:
                        logger.warning("Failed to delete parquet table file %s", path, exc_info=True)

                versions_row = conn.execute(
                    "SELECT COUNT(DISTINCT dataset_version) FROM tabular_datasets WHERE dataset_id = ?",
                    [dataset_id],
                ).fetchone()
                tables_row = conn.execute(
                    "SELECT COUNT(*) FROM tabular_tables WHERE dataset_id = ?",
                    [dataset_id],
                ).fetchone()
                result.datasets_deleted = int((versions_row or [0])[0] or 0)
                result.tables_deleted = int((tables_row or [0])[0] or 0)

                conn.execute("DELETE FROM tabular_tables WHERE dataset_id = ?", [dataset_id])
                conn.execute("DELETE FROM tabular_datasets WHERE dataset_id = ?", [dataset_id])
            finally:
                conn.close()

        dataset_dir = self.dataset_root / dataset_id
        if dataset_dir.exists():
            try:
                shutil.rmtree(dataset_dir, ignore_errors=True)
            except Exception:
                logger.warning("Failed to remove tabular dataset directory %s", dataset_dir, exc_info=True)

        return result


_shared_adapter: Optional[SharedDuckDBParquetStorageAdapter] = None
_shared_adapter_lock = Lock()


def _resolve_runtime_paths() -> tuple[Path, Path]:
    root = settings.get_tabular_runtime_root()
    catalog = settings.get_tabular_runtime_catalog_path()
    return root, catalog


def get_shared_tabular_storage_adapter() -> SharedDuckDBParquetStorageAdapter:
    global _shared_adapter
    if _shared_adapter is not None:
        return _shared_adapter

    with _shared_adapter_lock:
        if _shared_adapter is None:
            dataset_root, catalog_path = _resolve_runtime_paths()
            _shared_adapter = SharedDuckDBParquetStorageAdapter(
                dataset_root=dataset_root,
                catalog_path=catalog_path,
            )
    return _shared_adapter


def build_tabular_dataset_metadata(
    *,
    file_id: UUID | str,
    file_path: Path,
    file_type: str,
    source_filename: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    adapter = get_shared_tabular_storage_adapter()
    return adapter.ingest(
        file_id=str(file_id),
        file_path=Path(file_path).resolve(),
        file_type=file_type,
        source_filename=source_filename,
    )


def cleanup_tabular_artifacts_for_file(
    *,
    file_id: UUID | str,
    custom_metadata: Optional[Dict[str, Any]],
) -> TabularCleanupResult:
    adapter = get_shared_tabular_storage_adapter()
    return adapter.cleanup_for_file(file_id=str(file_id), custom_metadata=custom_metadata)

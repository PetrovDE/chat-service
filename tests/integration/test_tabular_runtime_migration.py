import asyncio
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.chat.tabular_sql import execute_tabular_sql_path
from app.services.tabular.storage_adapter import SharedDuckDBParquetStorageAdapter


def _write_csv(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows), encoding="utf-8")


def test_tabular_dataset_versioning_increments(tmp_path: Path):
    pytest.importorskip("duckdb")

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "sales.csv"

    _write_csv(
        csv_path,
        [
            "city,amount",
            "ekb,10",
            "msk,20",
        ],
    )
    v1 = adapter.ingest(file_id="file-1", file_path=csv_path, file_type="csv", source_filename="sales.csv")
    assert v1 is not None
    assert v1["dataset_version"] == 1
    assert v1["tables"][0]["table_version"] == 1
    assert Path(v1["tables"][0]["parquet_path"]).exists()

    _write_csv(
        csv_path,
        [
            "city,amount",
            "ekb,10",
            "msk,20",
            "spb,30",
        ],
    )
    v2 = adapter.ingest(file_id="file-1", file_path=csv_path, file_type="csv", source_filename="sales.csv")
    assert v2 is not None
    assert v2["dataset_version"] == 2
    assert v2["tables"][0]["table_version"] == 2
    assert v1["dataset_provenance_id"] != v2["dataset_provenance_id"]
    assert v1["tables"][0]["parquet_path"] != v2["tables"][0]["parquet_path"]


def test_tabular_sql_reproducible_for_same_dataset_version(tmp_path: Path):
    pytest.importorskip("duckdb")

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "report.csv"
    _write_csv(
        csv_path,
        [
            "city,amount",
            "ekb,10",
            "msk,20",
            "spb,30",
        ],
    )
    dataset = adapter.ingest(file_id="file-r", file_path=csv_path, file_type="csv", source_filename="report.csv")
    assert dataset is not None

    file_obj = SimpleNamespace(
        id="file-r",
        file_type="csv",
        original_filename="report.csv",
        custom_metadata={"tabular_dataset": dataset},
    )

    query = "\u0421\u043a\u043e\u043b\u044c\u043a\u043e \u0432\u0441\u0435\u0433\u043e \u0441\u0442\u0440\u043e\u043a \u0432 \u0444\u0430\u0439\u043b\u0435?"
    first = asyncio.run(execute_tabular_sql_path(query=query, files=[file_obj]))
    second = asyncio.run(execute_tabular_sql_path(query=query, files=[file_obj]))

    assert first is not None
    assert second is not None
    assert first["debug"]["tabular_sql"]["dataset_version"] == dataset["dataset_version"]
    assert second["debug"]["tabular_sql"]["dataset_version"] == dataset["dataset_version"]
    assert first["debug"]["tabular_sql"]["sql"] == second["debug"]["tabular_sql"]["sql"]
    assert first["debug"]["tabular_sql"]["result"] == second["debug"]["tabular_sql"]["result"]

    rows = json.loads(first["debug"]["tabular_sql"]["result"])
    assert rows[0][0] == 3


def test_tabular_sql_supports_legacy_sqlite_sidecar_metadata(tmp_path: Path):
    sidecar_path = tmp_path / "legacy.sqlite"
    conn = sqlite3.connect(str(sidecar_path))
    try:
        conn.execute("CREATE TABLE sheet_1 (city TEXT, amount TEXT)")
        conn.execute("INSERT INTO sheet_1(city, amount) VALUES ('ekb', '10')")
        conn.execute("INSERT INTO sheet_1(city, amount) VALUES ('msk', '20')")
        conn.execute("INSERT INTO sheet_1(city, amount) VALUES ('spb', '30')")
        conn.commit()
    finally:
        conn.close()

    file_obj = SimpleNamespace(
        id="legacy-file",
        file_type="xlsx",
        original_filename="legacy.xlsx",
        custom_metadata={
            "tabular_sidecar": {
                "path": str(sidecar_path),
                "tables": [
                    {
                        "table_name": "sheet_1",
                        "sheet_name": "Sheet1",
                        "row_count": 3,
                        "columns": ["city", "amount"],
                    }
                ],
            }
        },
    )

    result = asyncio.run(
        execute_tabular_sql_path(
            query="\u0421\u043a\u043e\u043b\u044c\u043a\u043e \u0432 \u0442\u0430\u0431\u043b\u0438\u0446\u0435 \u0432\u0441\u0435\u0433\u043e \u0441\u0442\u0440\u043e\u043a?",
            files=[file_obj],
        )
    )
    assert result is not None
    assert result["debug"]["tabular_sql"]["storage_engine"] == "sqlite_legacy"
    assert result["rows_expected_total"] == 3
    assert result["row_coverage_ratio"] == 1.0


import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from app.services.chat.tabular_sql import execute_tabular_sql_path
from app.services.chat.tabular_schema_resolver import resolve_requested_field
from app.services.tabular.column_metadata_contract import TABULAR_COLUMN_METADATA_CONTRACT_VERSION
from app.services.tabular.sql_execution import resolve_tabular_dataset
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


def test_tabular_sql_ignores_legacy_sqlite_sidecar_metadata(tmp_path: Path):
    sidecar_path = tmp_path / "legacy.sqlite"
    sidecar_path.write_text("", encoding="utf-8")
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
    assert result is None


def test_tabular_metadata_contract_propagates_to_runtime_and_bounds_payload(tmp_path: Path):
    pytest.importorskip("duckdb")
    pytest.importorskip("openpyxl")

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )

    csv_path = tmp_path / "orders.csv"
    _write_csv(
        csv_path,
        [
            "Order ID,Total Amount,State",
            "REQ-1,10.5,open",
            "REQ-2,20.0,closed",
            "REQ-3,11.2,open",
        ],
    )
    csv_dataset = adapter.ingest(file_id="file-csv", file_path=csv_path, file_type="csv", source_filename="orders.csv")
    assert csv_dataset is not None
    assert csv_dataset["column_metadata_contract_version"] == TABULAR_COLUMN_METADATA_CONTRACT_VERSION
    assert csv_dataset["column_metadata_stats"]["columns_with_metadata"] > 0

    table_payload = csv_dataset["tables"][0]
    assert table_payload["column_metadata_contract_version"] == TABULAR_COLUMN_METADATA_CONTRACT_VERSION
    column_metadata = table_payload["column_metadata"]
    assert "order_id" in column_metadata
    assert column_metadata["order_id"]["display_name"] == "Order ID"
    assert isinstance(column_metadata["order_id"]["aliases"], list)
    assert column_metadata["total_amount"]["dtype"] in {"numeric", "integer", "text"}
    assert len(column_metadata["state"].get("sample_values", [])) <= 5
    metadata_bytes = len(json.dumps(column_metadata, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    assert metadata_bytes <= int(table_payload["column_metadata_stats"]["metadata_budget_bytes"])

    xlsx_path = tmp_path / "orders.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        pd.DataFrame(
            {
                "Order ID": ["REQ-1", "REQ-2", "REQ-3"],
                "Total Amount": ["10.5", "20.0", "11.2"],
                "State": ["open", "closed", "open"],
            }
        ).to_excel(writer, index=False, sheet_name="Orders")

    xlsx_dataset = adapter.ingest(
        file_id="file-xlsx",
        file_path=xlsx_path,
        file_type="xlsx",
        source_filename="orders.xlsx",
    )
    assert xlsx_dataset is not None
    xlsx_table = xlsx_dataset["tables"][0]
    assert xlsx_table["column_metadata_contract_version"] == TABULAR_COLUMN_METADATA_CONTRACT_VERSION
    assert set(xlsx_table["column_metadata"].keys()) == set(column_metadata.keys())
    assert xlsx_table["column_metadata"]["order_id"]["display_name"] == "Order ID"
    assert xlsx_table["column_metadata"]["state"]["dtype"] == column_metadata["state"]["dtype"]

    file_obj = SimpleNamespace(
        id="file-csv",
        file_type="csv",
        original_filename="orders.csv",
        custom_metadata={"tabular_dataset": csv_dataset},
    )
    resolved = resolve_tabular_dataset(file_obj)
    assert resolved is not None
    assert resolved.column_metadata_contract_version == TABULAR_COLUMN_METADATA_CONTRACT_VERSION
    assert resolved.column_metadata_stats["columns_with_metadata"] > 0

    table = resolved.tables[0]
    resolution = resolve_requested_field(requested_field_text="total amount", table=table)
    assert resolution.status == "matched"
    assert resolution.matched_column == "total_amount"

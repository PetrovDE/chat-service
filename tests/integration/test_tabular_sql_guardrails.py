import asyncio
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.chat import tabular_sql
from app.services.chat.tabular_sql import execute_tabular_sql_path
from app.services.tabular.storage_adapter import SharedDuckDBParquetStorageAdapter
from app.services.tabular.sql_errors import (
    SQL_ERROR_GUARDRAIL_BLOCKED,
    SQL_ERROR_SCAN_LIMIT_EXCEEDED,
    SQL_ERROR_TIMEOUT,
)


def _make_tabular_dataset_file(tmp_path: Path) -> SimpleNamespace:
    pytest.importorskip("duckdb")
    pytest.importorskip("pandas")
    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "guardrails.csv"
    csv_path.write_text(
        "\n".join(
            [
                "city,amount",
                "ekb,10",
                "msk,20",
                "spb,30",
            ]
        ),
        encoding="utf-8",
    )
    dataset = adapter.ingest(
        file_id="guardrails-file",
        file_path=csv_path,
        file_type="csv",
        source_filename="guardrails.csv",
    )
    assert dataset is not None

    return SimpleNamespace(
        id="guardrails-file",
        extension="csv",
        file_type="csv",
        original_filename="guardrails.csv",
        custom_metadata={
            "tabular_dataset": dataset,
        }
    )


def test_tabular_sql_guardrails_blocked_sql_returns_classified_error(tmp_path: Path, monkeypatch):
    file_obj = _make_tabular_dataset_file(tmp_path)

    def fake_build_sql(*, query, table):  # noqa: ARG001
        return "DELETE FROM sheet_1", {"operation": "count", "group_by_column": None, "metric_column": None}

    monkeypatch.setattr(tabular_sql, "_build_sql", fake_build_sql)

    result = asyncio.run(execute_tabular_sql_path(query="count rows", files=[file_obj]))
    assert result is not None
    assert result["status"] == "error"
    assert result["debug"]["deterministic_error"]["code"] == SQL_ERROR_GUARDRAIL_BLOCKED
    assert result["debug"]["tabular_sql"]["policy_decision"]["allowed"] is False


def test_tabular_sql_guardrails_timeout_returns_classified_error(tmp_path: Path, monkeypatch):
    file_obj = _make_tabular_dataset_file(tmp_path)

    def slow_execute(**kwargs):  # noqa: ANN003
        time.sleep(0.2)
        return {"status": "ok"}

    monkeypatch.setattr(tabular_sql.settings, "TABULAR_SQL_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(tabular_sql, "_execute_aggregate_sync", slow_execute)

    result = asyncio.run(execute_tabular_sql_path(query="count rows", files=[file_obj]))
    assert result is not None
    assert result["status"] == "error"
    assert result["debug"]["deterministic_error"]["code"] == SQL_ERROR_TIMEOUT


def test_tabular_sql_guardrails_scan_limit_returns_classified_error(tmp_path: Path, monkeypatch):
    file_obj = _make_tabular_dataset_file(tmp_path)

    monkeypatch.setattr(tabular_sql.settings, "TABULAR_SQL_MAX_SCANNED_ROWS", 2)

    result = asyncio.run(execute_tabular_sql_path(query="count rows", files=[file_obj]))
    assert result is not None
    assert result["status"] == "error"
    assert result["debug"]["deterministic_error"]["code"] == SQL_ERROR_SCAN_LIMIT_EXCEEDED


def test_tabular_sql_guardrails_happy_path_includes_trace_fields(tmp_path: Path, monkeypatch):
    file_obj = _make_tabular_dataset_file(tmp_path)
    monkeypatch.setattr(tabular_sql.settings, "TABULAR_SQL_MAX_SCANNED_ROWS", 1000)
    monkeypatch.setattr(tabular_sql.settings, "TABULAR_SQL_TIMEOUT_SECONDS", 2.0)

    result = asyncio.run(execute_tabular_sql_path(query="count rows", files=[file_obj]))
    assert result is not None
    assert result["status"] == "ok"
    debug_payload = result["debug"]["tabular_sql"]

    assert isinstance(debug_payload.get("executed_sql"), str) and debug_payload["executed_sql"]
    assert isinstance(debug_payload.get("policy_decision"), dict)
    assert debug_payload["policy_decision"]["allowed"] is True
    assert isinstance(debug_payload.get("guardrail_flags"), list)


def test_tabular_sql_lookup_happy_path_returns_lookup_intent(tmp_path: Path, monkeypatch):
    file_obj = _make_tabular_dataset_file(tmp_path)
    monkeypatch.setattr(tabular_sql.settings, "TABULAR_SQL_MAX_SCANNED_ROWS", 1000)
    monkeypatch.setattr(tabular_sql.settings, "TABULAR_SQL_TIMEOUT_SECONDS", 2.0)

    result = asyncio.run(execute_tabular_sql_path(query="find rows where city = msk", files=[file_obj]))
    assert result is not None
    assert result["status"] == "ok"
    debug_payload = result["debug"]
    assert debug_payload["intent"] == "tabular_lookup"
    assert "sql_lookup" in result["sources"][0]

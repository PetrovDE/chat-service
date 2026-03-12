import asyncio
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

from app.services.chat import tabular_sql
from app.services.chat.tabular_sql import execute_tabular_sql_path
from app.services.tabular.sql_errors import (
    SQL_ERROR_GUARDRAIL_BLOCKED,
    SQL_ERROR_SCAN_LIMIT_EXCEEDED,
    SQL_ERROR_TIMEOUT,
)


def _make_legacy_sidecar_file(tmp_path: Path) -> SimpleNamespace:
    sidecar_path = tmp_path / "guardrails_legacy.sqlite"
    conn = sqlite3.connect(str(sidecar_path))
    try:
        conn.execute("CREATE TABLE sheet_1 (city TEXT, amount TEXT)")
        conn.execute("INSERT INTO sheet_1(city, amount) VALUES ('ekb', '10')")
        conn.execute("INSERT INTO sheet_1(city, amount) VALUES ('msk', '20')")
        conn.execute("INSERT INTO sheet_1(city, amount) VALUES ('spb', '30')")
        conn.commit()
    finally:
        conn.close()

    return SimpleNamespace(
        id="legacy-guardrails-file",
        file_type="xlsx",
        original_filename="guardrails.xlsx",
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


def test_tabular_sql_guardrails_blocked_sql_returns_classified_error(tmp_path: Path, monkeypatch):
    file_obj = _make_legacy_sidecar_file(tmp_path)

    def fake_build_sql(*, query, table):  # noqa: ARG001
        return "DELETE FROM sheet_1", {"operation": "count", "group_by_column": None, "metric_column": None}

    monkeypatch.setattr(tabular_sql, "_build_sql", fake_build_sql)

    result = asyncio.run(execute_tabular_sql_path(query="count rows", files=[file_obj]))
    assert result is not None
    assert result["status"] == "error"
    assert result["debug"]["deterministic_error"]["code"] == SQL_ERROR_GUARDRAIL_BLOCKED
    assert result["debug"]["tabular_sql"]["policy_decision"]["allowed"] is False


def test_tabular_sql_guardrails_timeout_returns_classified_error(tmp_path: Path, monkeypatch):
    file_obj = _make_legacy_sidecar_file(tmp_path)

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
    file_obj = _make_legacy_sidecar_file(tmp_path)

    monkeypatch.setattr(tabular_sql.settings, "TABULAR_SQL_MAX_SCANNED_ROWS", 2)

    result = asyncio.run(execute_tabular_sql_path(query="count rows", files=[file_obj]))
    assert result is not None
    assert result["status"] == "error"
    assert result["debug"]["deterministic_error"]["code"] == SQL_ERROR_SCAN_LIMIT_EXCEEDED


def test_tabular_sql_guardrails_happy_path_includes_trace_fields(tmp_path: Path, monkeypatch):
    file_obj = _make_legacy_sidecar_file(tmp_path)
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
    file_obj = _make_legacy_sidecar_file(tmp_path)
    monkeypatch.setattr(tabular_sql.settings, "TABULAR_SQL_MAX_SCANNED_ROWS", 1000)
    monkeypatch.setattr(tabular_sql.settings, "TABULAR_SQL_TIMEOUT_SECONDS", 2.0)

    result = asyncio.run(execute_tabular_sql_path(query="find rows where city = msk", files=[file_obj]))
    assert result is not None
    assert result["status"] == "ok"
    debug_payload = result["debug"]
    assert debug_payload["intent"] == "tabular_lookup"
    assert "sql_lookup" in result["sources"][0]

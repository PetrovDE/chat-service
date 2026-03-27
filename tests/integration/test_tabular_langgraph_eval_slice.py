from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.chat.tabular_sql import execute_tabular_sql_path
from app.services.tabular.storage_adapter import SharedDuckDBParquetStorageAdapter


def _write_csv(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows), encoding="utf-8")


def _build_file(tmp_path: Path, *, file_id: str = "file-1") -> SimpleNamespace:
    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "spending.csv"
    _write_csv(
        csv_path,
        [
            "request_id,created_at,amount_rub,city,status",
            "1,2026-01-05 10:00:00,100,ekb,new",
            "2,2026-01-15 12:00:00,150,msk,new",
            "3,2026-02-03 09:00:00,240,ekb,approved",
            "4,2026-02-20 18:30:00,180,spb,approved",
            "5,2026-03-08 11:15:00,300,msk,new",
            "6,2026-03-18 14:45:00,120,spb,approved",
        ],
    )
    dataset = adapter.ingest(
        file_id=file_id,
        file_path=csv_path,
        file_type="csv",
        source_filename="spending.csv",
    )
    assert dataset is not None
    return SimpleNamespace(
        id=file_id,
        extension="csv",
        file_type="csv",
        chunks_count=24,
        original_filename="spending.csv",
        custom_metadata={"tabular_dataset": dataset},
    )


def _fake_chart_delivery(**kwargs):  # noqa: ANN003
    _ = kwargs
    return {
        "chart_rendered": True,
        "chart_artifact_available": True,
        "chart_artifact_exists": True,
        "chart_fallback_reason": "none",
        "chart_artifact_path": "tabular_sql/run/chart.png",
        "chart_artifact_id": "chart-1",
        "artifact": {
            "kind": "tabular_chart",
            "name": "chart.png",
            "path": "tabular_sql/run/chart.png",
            "url": "/uploads/tabular_sql/run/chart.png",
            "content_type": "image/png",
        },
    }


@pytest.mark.parametrize(
    "query_text,expected_route",
    [
        ("what columns are in this file", "schema_question"),
        ("count rows", "aggregation"),
        ("show sum of amount_rub by month from created_at", "trend"),
        ("show chart of amount_rub by city", "chart"),
        ("show chart of amount_rub\nFollow-up refinement: same but by status", "chart"),
        ("show chart of amount_rub by city\nFollow-up refinement: same chart but monthly", "chart"),
        ("show chart of amount_rub by city\nFollow-up refinement: filter where status = approved", "chart"),
    ],
)
def test_langgraph_eval_slice_queries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    query_text: str,
    expected_route: str,
) -> None:
    pytest.importorskip("duckdb")

    file_obj = _build_file(tmp_path)
    monkeypatch.setattr("app.services.chat.tabular_sql.render_chart_artifact", _fake_chart_delivery)
    monkeypatch.setattr("app.services.chat.tabular_sql.settings.ANALYTICS_ENGINE_MODE", "langgraph")
    monkeypatch.setattr("app.services.chat.tabular_sql.settings.ANALYTICS_ENGINE_SHADOW", False)
    monkeypatch.setattr("app.services.chat.tabular_sql.settings.TABULAR_LLM_GUARDED_PLANNER_ENABLED", False)

    result = asyncio.run(execute_tabular_sql_path(query=query_text, files=[file_obj]))

    assert isinstance(result, dict)
    assert str(result.get("status") or "") == "ok"
    debug = result.get("debug") or {}
    assert debug.get("analytics_engine_mode_served") == "langgraph"
    assert str(debug.get("selected_route") or "") == expected_route

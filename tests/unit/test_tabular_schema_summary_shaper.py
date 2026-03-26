from __future__ import annotations

from types import SimpleNamespace

from app.services.chat.tabular_schema_summary_shaper import (
    build_schema_summary_context,
    rank_relevant_fields,
)
from app.services.tabular.sql_execution import ResolvedTabularDataset, ResolvedTabularTable


def _table(
    *,
    table_name: str,
    sheet_name: str,
    row_count: int,
    columns: list[str],
    column_metadata: dict | None = None,
) -> ResolvedTabularTable:
    return ResolvedTabularTable(
        table_name=table_name,
        sheet_name=sheet_name,
        row_count=row_count,
        columns=list(columns),
        column_aliases={},
        table_version=1,
        provenance_id=f"{table_name}-prov",
        parquet_path=None,
        column_metadata=dict(column_metadata or {}),
    )


def test_rank_relevant_fields_prefers_metadata_supported_analytic_columns() -> None:
    table = _table(
        table_name="orders",
        sheet_name="Orders",
        row_count=120,
        columns=["id", "created_at", "amount_total", "status", "comment"],
        column_metadata={
            "created_at": {"dtype": "datetime", "cardinality_hint": "medium", "sample_values": ["2026-01-01"]},
            "amount_total": {"dtype": "float64", "cardinality_hint": "high", "sample_values": [120.5]},
            "status": {"dtype": "string", "cardinality_hint": "low", "sample_values": ["paid", "pending"]},
            "id": {"dtype": "int64", "cardinality_hint": "high", "sample_values": [1]},
        },
    )

    ranked = rank_relevant_fields(table=table, limit=4)

    assert [item["name"] for item in ranked[:3]] == ["created_at", "amount_total", "status"]
    assert all(str(item.get("name")) != "id" for item in ranked[:3])


def test_schema_summary_context_for_single_table_is_actionable() -> None:
    table = _table(
        table_name="orders",
        sheet_name="Orders",
        row_count=308,
        columns=["order_id", "created_at", "amount_total", "status"],
        column_metadata={
            "created_at": {"dtype": "datetime", "cardinality_hint": "medium"},
            "amount_total": {"dtype": "float64", "cardinality_hint": "high"},
            "status": {"dtype": "string", "cardinality_hint": "low"},
        },
    )
    dataset = ResolvedTabularDataset(
        engine="duckdb_parquet",
        dataset_id="ds-1",
        dataset_version=3,
        dataset_provenance_id="prov-1",
        tables=[table],
        catalog_path=None,
    )

    payload = build_schema_summary_context(
        query="tell me about this file",
        dataset=dataset,
        target_file=SimpleNamespace(original_filename="orders.xlsx", stored_filename="orders.xlsx"),
        selected_table=table,
    )

    assert payload["file_name"] == "orders.xlsx"
    assert payload["tables_total"] == 1
    assert payload["rows_total"] == 308
    assert isinstance(payload["next_question_suggestions"], list) and payload["next_question_suggestions"]
    assert len(payload["tables"][0]["relevant_fields"]) <= 6


def test_schema_summary_context_for_multi_sheet_exposes_scope_choices() -> None:
    north = _table(
        table_name="north_sheet",
        sheet_name="North",
        row_count=120,
        columns=["created_at", "amount_total", "status"],
        column_metadata={"amount_total": {"dtype": "float64"}},
    )
    south = _table(
        table_name="south_sheet",
        sheet_name="South",
        row_count=120,
        columns=["created_at", "amount_total", "status"],
        column_metadata={"amount_total": {"dtype": "float64"}},
    )
    dataset = ResolvedTabularDataset(
        engine="duckdb_parquet",
        dataset_id="ds-2",
        dataset_version=1,
        dataset_provenance_id="prov-2",
        tables=[north, south],
        catalog_path=None,
    )

    payload = build_schema_summary_context(
        query="which sheets are available in this file?",
        dataset=dataset,
        target_file=SimpleNamespace(original_filename="regions.xlsx", stored_filename="regions.xlsx"),
        selected_table=None,
    )

    assert payload["tables_total"] == 2
    assert "Workbook with 2 table(s)/sheet(s)" in str(payload.get("summary_statement") or "")
    assert any("sheet North" in str(item.get("scope_label") or "") for item in payload["tables"])
    assert any("sheet South" in str(item.get("scope_label") or "") for item in payload["tables"])
    assert any("Compare row counts" in str(item) for item in payload.get("next_question_suggestions") or [])

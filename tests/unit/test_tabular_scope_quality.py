import asyncio
from types import SimpleNamespace

from app.services.chat import tabular_sql as tsql
from app.services.chat.tabular_intent_router import TabularIntentDecision
from app.services.chat.tabular_sql_route_payloads import build_missing_column_response
from app.services.tabular.sql_execution import ResolvedTabularDataset, ResolvedTabularTable


def _table(name: str, sheet: str, *, columns=None, rows: int = 100) -> ResolvedTabularTable:
    return ResolvedTabularTable(
        table_name=name,
        sheet_name=sheet,
        row_count=rows,
        columns=list(columns or ["status", "amount"]),
        column_aliases={},
        table_version=1,
        provenance_id=f"{name}-prov",
        parquet_path=None,
    )


def _dataset(dataset_id: str, tables) -> ResolvedTabularDataset:
    return ResolvedTabularDataset(
        engine="duckdb_parquet",
        dataset_id=dataset_id,
        dataset_version=1,
        dataset_provenance_id=f"{dataset_id}-prov",
        tables=list(tables),
        catalog_path=None,
    )


def _file(file_id: str, filename: str):
    return SimpleNamespace(
        id=file_id,
        extension="xlsx",
        file_type="xlsx",
        original_filename=filename,
        stored_filename=filename,
        custom_metadata={},
    )


def test_multi_file_ambiguity_returns_concise_clarification(monkeypatch):
    north_file = _file("f-north", "north.xlsx")
    south_file = _file("f-south", "south.xlsx")
    datasets = {
        "f-north": _dataset("ds-north", [_table("north_sheet", "North")]),
        "f-south": _dataset("ds-south", [_table("south_sheet", "South")]),
    }

    monkeypatch.setattr(tsql, "resolve_tabular_dataset", lambda file_obj: datasets.get(str(file_obj.id)))

    result = asyncio.run(
        tsql.execute_tabular_sql_path(
            query="show count by status",
            files=[north_file, south_file],
        )
    )

    assert result is not None
    assert result["status"] == "error"
    clarification = str(result.get("clarification_prompt") or "").lower()
    assert "multiple possible file matches" in clarification
    assert "north.xlsx" in clarification
    assert "south.xlsx" in clarification
    assert result["debug"]["selected_route"] == "ambiguous_data_scope"
    assert result["debug"]["scope_selection_status"] == "ambiguous_file"


def test_multi_sheet_ambiguity_returns_concise_clarification(monkeypatch):
    file_obj = _file("f-1", "regions.xlsx")
    dataset = _dataset(
        "ds-1",
        [
            _table("north_sheet", "North", rows=120),
            _table("south_sheet", "South", rows=120),
        ],
    )
    monkeypatch.setattr(tsql, "resolve_tabular_dataset", lambda _: dataset)

    result = asyncio.run(
        tsql.execute_tabular_sql_path(
            query="show count",
            files=[file_obj],
        )
    )

    assert result is not None
    assert result["status"] == "error"
    clarification = str(result.get("clarification_prompt") or "").lower()
    assert "multiple possible sheet/table matches" in clarification
    assert "north" in clarification
    assert "south" in clarification
    assert result["debug"]["scope_selection_status"] == "ambiguous_table"


def test_selected_scope_is_reflected_in_tabular_debug(monkeypatch):
    north_file = _file("f-north", "north.xlsx")
    south_file = _file("f-south", "south.xlsx")
    datasets = {
        "f-north": _dataset("ds-north", [_table("north_sheet", "North")]),
        "f-south": _dataset("ds-south", [_table("south_sheet", "South")]),
    }

    monkeypatch.setattr(tsql, "resolve_tabular_dataset", lambda file_obj: datasets.get(str(file_obj.id)))

    def _fake_execute_aggregate_sync(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {
            "status": "ok",
            "prompt_context": "Deterministic tabular SQL result",
            "debug": {
                "retrieval_mode": "tabular_sql",
                "intent": "tabular_aggregate",
                "tabular_sql": {},
            },
            "sources": ["north.xlsx | table=north_sheet | sql"],
            "rows_expected_total": 120,
            "rows_retrieved_total": 120,
            "rows_used_map_total": 120,
            "rows_used_reduce_total": 120,
            "row_coverage_ratio": 1.0,
        }

    monkeypatch.setattr(tsql, "_execute_aggregate_sync", _fake_execute_aggregate_sync)

    result = asyncio.run(
        tsql.execute_tabular_sql_path(
            query="count rows in north",
            files=[north_file, south_file],
        )
    )

    assert result is not None
    assert result["status"] == "ok"
    assert result["debug"]["scope_selection_status"] == "selected"
    assert result["debug"]["scope_selected_file_name"] == "north.xlsx"
    assert result["debug"]["scope_selected_sheet_name"] == "North"
    assert result["debug"]["scope_selected_table_name"] == "north_sheet"


def test_missing_column_response_prefers_query_relevant_alternatives():
    dataset = _dataset("ds-1", [_table("requests", "Sheet1", columns=["amount_rub", "amount_total", "city", "status"])])
    table = dataset.tables[0]
    decision = TabularIntentDecision(
        detected_intent="aggregation",
        selected_route="unsupported_missing_column",
        legacy_intent="aggregate",
        requested_fields=["revenue"],
        matched_columns=[],
        unmatched_requested_fields=["revenue"],
        fallback_reason="missing_required_columns",
        requested_field_text="revenue",
        candidate_columns=["amount_total", "amount_rub", "status"],
        scored_candidates=[{"column": "amount_total", "score": 0.88}],
    )
    payload = build_missing_column_response(
        query="show sum of revenue",
        decision=decision,
        dataset=dataset,
        table=table,
        target_file=SimpleNamespace(original_filename="requests.xlsx"),
    )

    clarification = str(payload.get("clarification_prompt") or "").lower()
    assert "amount_total" in clarification
    assert "amount_rub" in clarification
    assert "best next question" in clarification

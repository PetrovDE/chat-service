from types import SimpleNamespace

from app.domain.chat.query_planner import (
    INTENT_COMPLEX_ANALYTICS,
    INTENT_NARRATIVE_RETRIEVAL,
    INTENT_TABULAR_AGGREGATE,
    INTENT_TABULAR_COMBINED,
    INTENT_TABULAR_LOOKUP,
    INTENT_TABULAR_PROFILE,
    ROUTE_COMPLEX_ANALYTICS,
    ROUTE_DETERMINISTIC_ANALYTICS,
    ROUTE_NARRATIVE_RETRIEVAL,
    plan_query,
)


def _tabular_file(*, file_type: str = "xlsx", extension: str = "xlsx"):
    return SimpleNamespace(
        id="file-1",
        file_type=file_type,
        extension=extension,
        custom_metadata={
            "tabular_dataset": {
                "dataset_id": "ds-1",
                "dataset_version": 1,
                "dataset_provenance_id": "prov-1",
                "tables": [
                    {
                        "table_name": "sheet_1",
                        "sheet_name": "Sheet1",
                        "row_count": 100,
                        "columns": ["region", "revenue"],
                        "column_aliases": {"revenue": "Выручка"},
                    }
                ],
            }
        },
    )


def test_planner_routes_profile_query_to_deterministic():
    decision = plan_query(
        query="Покажи общий анализ по каждой колонке",
        files=[_tabular_file()],
    )
    assert decision.route == ROUTE_DETERMINISTIC_ANALYTICS
    assert decision.intent == INTENT_TABULAR_PROFILE
    assert decision.requires_clarification is False
    assert decision.confidence >= 0.9


def test_planner_routes_aggregate_query_to_deterministic():
    decision = plan_query(
        query="Посчитай сумму revenue по region",
        files=[_tabular_file()],
    )
    assert decision.route == ROUTE_DETERMINISTIC_ANALYTICS
    assert decision.intent == INTENT_TABULAR_AGGREGATE


def test_planner_routes_russian_schema_filename_query_to_profile(monkeypatch):
    from app.domain.chat import query_planner as planner_module

    dataset = SimpleNamespace(
        tables=[
            SimpleNamespace(
                table_name="sheet_1",
                sheet_name="Sheet1",
                row_count=460,
                columns=["office", "status"],
                column_aliases={},
            )
        ]
    )
    monkeypatch.setattr(planner_module, "resolve_tabular_dataset", lambda _file: dataset)

    decision = plan_query(
        query=(
            "\u043a\u0430\u043a\u0438\u0435 \u0434\u0430\u043d\u043d\u044b\u0435 \u0432 \u0444\u0430\u0439\u043b\u0435 "
            "test_requests_460_rows.xlsx \u0438 \u043a\u0430\u043a\u0438\u0435 \u0441\u0442\u043e\u043b\u0431\u0446\u044b"
        ),
        files=[_tabular_file()],
    )
    assert decision.route == ROUTE_DETERMINISTIC_ANALYTICS
    assert decision.intent == INTENT_TABULAR_PROFILE
    assert decision.requires_clarification is False


def test_planner_routes_lookup_query_to_deterministic():
    decision = plan_query(
        query="Find rows where region is north",
        files=[_tabular_file()],
    )
    assert decision.route == ROUTE_DETERMINISTIC_ANALYTICS
    assert decision.intent == INTENT_TABULAR_LOOKUP
    assert decision.requires_clarification is False


def test_planner_routes_non_tabular_query_to_narrative():
    decision = plan_query(
        query="Сделай краткое объяснение документа",
        files=[SimpleNamespace(id="text-1", file_type="txt", custom_metadata={})],
    )
    assert decision.route == ROUTE_NARRATIVE_RETRIEVAL
    assert decision.intent == INTENT_NARRATIVE_RETRIEVAL
    assert decision.requires_clarification is False


def test_planner_routes_python_pandas_request_to_complex_analytics():
    decision = plan_query(
        query="Write Python pandas code, run NLP on comment_text and build a heatmap by office",
        files=[_tabular_file()],
    )
    assert decision.route == ROUTE_COMPLEX_ANALYTICS
    assert decision.intent == INTENT_COMPLEX_ANALYTICS
    assert decision.requires_clarification is False


def test_planner_routes_combined_when_semantic_table_scope_and_aggregate_needed():
    decision = plan_query(
        query="На каком листе есть регион North и сколько там записей?",
        files=[_tabular_file()],
    )
    assert decision.route == ROUTE_DETERMINISTIC_ANALYTICS
    assert decision.intent == INTENT_TABULAR_COMBINED
    assert decision.strategy_mode == "combined"
    assert decision.requires_clarification is False


def test_planner_detects_tabular_file_by_extension_without_file_type():
    file_obj = _tabular_file(file_type="", extension="csv")
    decision = plan_query(
        query="Сколько всего строк в таблице?",
        files=[file_obj],
    )
    assert decision.route == ROUTE_DETERMINISTIC_ANALYTICS
    assert decision.intent == INTENT_TABULAR_AGGREGATE

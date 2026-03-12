from types import SimpleNamespace

from app.domain.chat.query_planner import (
    INTENT_COMPLEX_ANALYTICS,
    INTENT_NARRATIVE_RETRIEVAL,
    INTENT_TABULAR_AGGREGATE,
    INTENT_TABULAR_LOOKUP,
    INTENT_TABULAR_PROFILE,
    ROUTE_COMPLEX_ANALYTICS,
    ROUTE_DETERMINISTIC_ANALYTICS,
    ROUTE_NARRATIVE_RETRIEVAL,
    plan_query,
)


def _tabular_file(*, file_type: str = "xlsx"):
    return SimpleNamespace(
        id="file-1",
        file_type=file_type,
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

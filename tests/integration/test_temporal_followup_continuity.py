import asyncio
import uuid
from types import SimpleNamespace

from app.domain.chat.query_planner import (
    INTENT_TABULAR_AGGREGATE,
    ROUTE_DETERMINISTIC_ANALYTICS,
    QueryPlanDecision,
)
from app.services.chat import rag_prompt_builder as rag_builder


def test_followup_temporal_clarification_reuses_prior_tabular_intent_and_short_circuits_chart(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    planner_queries = []
    executor_queries = []

    tabular_file = SimpleNamespace(
        id=uuid.uuid4(),
        extension="xlsx",
        file_type="xlsx",
        chunks_count=12,
        original_filename="spending.xlsx",
        custom_metadata={
            "tabular_dataset": {
                "dataset_id": "ds-1",
                "dataset_version": 1,
                "dataset_provenance_id": "prov-1",
                "tables": [
                    {
                        "table_name": "requests",
                        "sheet_name": "Sheet1",
                        "row_count": 120,
                        "columns": ["request_id", "created_at", "amount_rub", "status"],
                        "column_aliases": {},
                    }
                ],
            }
        },
    )

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [tabular_file]

    def planner_capture(*, query, files):  # noqa: ARG001
        planner_queries.append(query)
        return QueryPlanDecision(
            route=ROUTE_DETERMINISTIC_ANALYTICS,
            intent=INTENT_TABULAR_AGGREGATE,
            strategy_mode="analytical",
            confidence=0.95,
            requires_clarification=False,
            reason_codes=["test_force_deterministic"],
        )

    async def fake_tabular_executor(*, query, files):  # noqa: ARG001
        executor_queries.append(query)
        return {
            "status": "ok",
            "prompt_context": "temporal chart context",
            "chart_response_text": "Temporal chart rendered from created_at by month.",
            "artifacts": [
                {
                    "kind": "tabular_chart",
                    "path": "uploads/tabular_sql/run/chart.png",
                    "url": "/uploads/tabular_sql/run/chart.png",
                    "content_type": "image/png",
                }
            ],
            "sources": ["spending.xlsx | table=requests | sql_chart"],
            "rows_expected_total": 120,
            "rows_retrieved_total": 12,
            "rows_used_map_total": 12,
            "rows_used_reduce_total": 12,
            "row_coverage_ratio": 0.1,
            "debug": {
                "retrieval_mode": "tabular_sql",
                "intent": "tabular_chart",
                "selected_route": "trend",
                "chart_spec_generated": True,
                "chart_rendered": True,
                "chart_artifact_available": True,
                "chart_artifact_exists": True,
                "chart_fallback_reason": "none",
                "requested_time_grain": "month",
                "source_datetime_field": "created_at",
                "derived_temporal_dimension": "month(created_at)",
                "temporal_plan_status": "resolved",
                "temporal_aggregation_plan": {
                    "requested_time_grain": "month",
                    "source_datetime_field": "created_at",
                    "derived_grouping_dimension": "month(created_at)",
                    "operation": "sum",
                    "measure_column": "amount_rub",
                    "status": "ready",
                    "fallback_reason": "none",
                },
                "tabular_sql": {"chart_spec": {"matched_chart_field": "created_at"}},
            },
        }

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder, "execute_tabular_sql_path", fake_tabular_executor)

    _prompt, _rag_used, rag_debug, _docs, _caveats, _sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="take month from dates",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=planner_capture,
            conversation_history=[
                {"role": "user", "content": "show spending by month"},
                {"role": "assistant", "content": "I need your datetime preference."},
            ],
        )
    )

    assert planner_queries
    assert "show spending by month" in planner_queries[0]
    assert "take month from dates" in planner_queries[0]
    assert executor_queries
    assert "show spending by month" in executor_queries[0]
    assert "take month from dates" in executor_queries[0]
    assert rag_debug["followup_context_used"] is True
    assert rag_debug["prior_tabular_intent_reused"] is True
    assert rag_debug["short_circuit_response"] is True
    assert "import pandas" not in rag_debug["short_circuit_response_text"].lower()
    assert rag_debug["selected_route"] == "trend"
    assert rag_debug["requested_time_grain"] == "month"
    assert rag_debug["source_datetime_field"] == "created_at"
    assert rag_debug["temporal_plan_status"] == "resolved"

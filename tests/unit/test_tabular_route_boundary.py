import asyncio
from types import SimpleNamespace
import uuid

from app.domain.chat.query_planner import INTENT_TABULAR_AGGREGATE, QueryPlanDecision
from app.services.chat import rag_prompt_routes


def test_deterministic_route_delegates_success_payload_building(monkeypatch):
    planner_decision = QueryPlanDecision(
        route="deterministic_analytics",
        intent=INTENT_TABULAR_AGGREGATE,
        strategy_mode="analytical",
        confidence=0.9,
        requires_clarification=False,
        reason_codes=["tabular_dataset_available"],
    )
    planner_payload = planner_decision.as_dict()
    called = {"value": False}

    async def _fake_tabular_executor(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {
            "status": "ok",
            "prompt_context": "deterministic context",
            "debug": {"selected_route": "aggregation"},
            "artifacts": [],
            "sources": [],
            "rows_expected_total": 10,
            "rows_retrieved_total": 10,
            "rows_used_map_total": 10,
            "rows_used_reduce_total": 10,
            "row_coverage_ratio": 1.0,
        }

    expected = ("delegated prompt", True, {"delegated": True}, [], [], [])

    def _fake_builder(**kwargs):  # noqa: ANN003
        called["value"] = True
        assert kwargs["tabular_sql_result"]["status"] == "ok"
        return expected

    monkeypatch.setattr(rag_prompt_routes, "build_tabular_success_route_result", _fake_builder)

    result = asyncio.run(
        rag_prompt_routes.maybe_run_deterministic_route(
            query="count rows",
            user_id=uuid.uuid4(),
            conversation_id=uuid.uuid4(),
            files=[SimpleNamespace(id="f-1")],
            planner_decision=planner_decision,
            planner_decision_payload=planner_payload,
            expected_chunks_total=0,
            rag_mode="auto",
            top_k=3,
            preferred_lang="en",
            tabular_sql_executor=_fake_tabular_executor,
            rag_retriever_client=None,
            is_combined_intent=False,
        )
    )

    assert called["value"] is True
    assert result == expected

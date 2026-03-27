from types import SimpleNamespace
import uuid
import asyncio

import pytest

from app.domain.chat.query_planner import (
    INTENT_TABULAR_AGGREGATE,
    QueryPlanDecision,
)
from app.services.chat.rag_prompt_routes import maybe_run_deterministic_route
from app.services.chat import tabular_sql as tsql
from app.services.chat.tabular_intent_router import TabularIntentDecision
from app.services.tabular.sql_errors import SQL_ERROR_EXECUTION_FAILED, TabularSQLException
from app.services.tabular.sql_execution import ResolvedTabularDataset, ResolvedTabularTable


def _build_dataset(columns, *, aliases=None):
    table = ResolvedTabularTable(
        table_name="requests",
        sheet_name="Sheet1",
        row_count=100,
        columns=list(columns),
        column_aliases=dict(aliases or {}),
        table_version=1,
        provenance_id="tbl-1",
        parquet_path=None,
    )
    return ResolvedTabularDataset(
        engine="duckdb_parquet",
        dataset_id="ds-1",
        dataset_version=1,
        dataset_provenance_id="prov-1",
        tables=[table],
        catalog_path=None,
    ), table


def _chart_decision(*, matched_columns):
    return TabularIntentDecision(
        detected_intent="chart",
        selected_route="chart",
        legacy_intent="aggregate",
        requested_fields=[],
        matched_columns=list(matched_columns),
        unmatched_requested_fields=[],
        fallback_reason="none",
    )


def test_build_chart_sql_rejects_unmatched_explicit_field_instead_of_request_id():
    _dataset, table = _build_dataset(["request_id", "created_at"])
    decision = _chart_decision(matched_columns=[])

    with pytest.raises(TabularSQLException) as exc_info:
        tsql._build_chart_sql(
            query="дай график распределения по Status Code",
            table=table,
            decision=decision,
        )

    assert exc_info.value.code == SQL_ERROR_EXECUTION_FAILED
    assert exc_info.value.details.get("requested_chart_field") == "status code"


def test_execute_chart_sync_success_includes_artifact_and_chart_debug(monkeypatch):
    dataset, table = _build_dataset(
        ["request_id", "status_code"],
        aliases={"status_code": "Status Code"},
    )
    decision = _chart_decision(matched_columns=["status_code"])

    class _FakeSession:
        def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
            _ = args, kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    monkeypatch.setattr(tsql, "TabularExecutionSession", _FakeSession)
    monkeypatch.setattr(
        tsql,
        "_run_guarded_query",
        lambda **kwargs: ([("200", 12), ("500", 3)], kwargs["sql"], {"policy_decision": {"allowed": True}, "guardrail_flags": []}),
    )
    monkeypatch.setattr(
        tsql,
        "_render_chart_artifact",
        lambda **kwargs: {
            "chart_rendered": True,
            "chart_artifact_available": True,
            "chart_artifact_exists": True,
            "chart_fallback_reason": "none",
            "chart_artifact_path": "uploads/tabular_sql/run/chart.png",
            "chart_artifact_id": "chart-1",
            "artifact": {
                "kind": "tabular_chart",
                "name": "chart.png",
                "path": "uploads/tabular_sql/run/chart.png",
                "url": "/uploads/tabular_sql/run/chart.png",
                "content_type": "image/png",
                "column": "status_code",
            },
        },
    )

    payload = tsql._execute_chart_sync(
        query="дай график распределения по Status Code",
        dataset=dataset,
        table=table,
        target_file=SimpleNamespace(original_filename="requests.csv"),
        timeout_seconds=3.0,
        decision=decision,
    )

    assert payload["status"] == "ok"
    assert payload["artifacts"][0]["url"].startswith("/uploads/")
    assert payload["debug"]["requested_chart_field"] == "status code"
    assert payload["debug"]["matched_chart_field"] == "status_code"
    assert payload["debug"]["chart_spec_generated"] is True
    assert payload["debug"]["chart_rendered"] is True
    assert payload["debug"]["chart_artifact_available"] is True
    assert payload["debug"]["chart_artifact_exists"] is True
    assert payload["debug"]["chart_fallback_reason"] == "none"
    assert "status_code" in payload["debug"]["tabular_sql"]["chart_spec"]["matched_chart_field"]
    assert payload["artifacts"][0]["url"].endswith(payload["artifacts"][0]["path"].replace("uploads/", ""))


def test_execute_chart_sync_render_failure_returns_controlled_fallback(monkeypatch):
    dataset, table = _build_dataset(["request_id", "status_code"])
    decision = _chart_decision(matched_columns=["status_code"])

    class _FakeSession:
        def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
            _ = args, kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

    monkeypatch.setattr(tsql, "TabularExecutionSession", _FakeSession)
    monkeypatch.setattr(
        tsql,
        "_run_guarded_query",
        lambda **kwargs: ([("200", 7), ("500", 2)], kwargs["sql"], {"policy_decision": {"allowed": True}, "guardrail_flags": []}),
    )
    monkeypatch.setattr(
        tsql,
        "_render_chart_artifact",
        lambda **kwargs: {
            "chart_rendered": False,
            "chart_artifact_available": False,
            "chart_artifact_exists": False,
            "chart_fallback_reason": "renderer_unavailable",
            "chart_artifact_path": None,
            "chart_artifact_id": None,
            "artifact": None,
        },
    )

    payload = tsql._execute_chart_sync(
        query="дай график распределения по Status Code",
        dataset=dataset,
        table=table,
        target_file=SimpleNamespace(original_filename="requests.csv"),
        timeout_seconds=3.0,
        decision=decision,
    )

    assert payload["status"] == "ok"
    assert payload["artifacts"] == []
    assert payload["debug"]["chart_rendered"] is False
    assert payload["debug"]["chart_artifact_available"] is False
    assert payload["debug"]["chart_artifact_exists"] is False
    assert payload["debug"]["chart_fallback_reason"] == "renderer_unavailable"
    assert "\u0438\u0437\u043e\u0431\u0440\u0430\u0436\u0435\u043d\u0438\u0435" in payload["chart_response_text"].lower()
    assert "Distribution" not in payload["chart_response_text"]


def test_deterministic_route_sets_short_circuit_and_artifacts_for_chart():
    planner_decision = QueryPlanDecision(
        route="deterministic_analytics",
        intent=INTENT_TABULAR_AGGREGATE,
        strategy_mode="analytical",
        confidence=0.9,
        requires_clarification=False,
        reason_codes=["tabular_dataset_available"],
    )
    planner_payload = planner_decision.as_dict()
    fake_result = {
        "status": "ok",
        "prompt_context": "chart context",
        "chart_response_text": "График распределения по «Status Code» успешно построен.",
        "artifacts": [{"url": "/uploads/tabular_sql/run/chart.png", "path": "uploads/tabular_sql/run/chart.png", "kind": "tabular_chart"}],
        "sources": ["requests.csv | sql_chart"],
        "rows_expected_total": 100,
        "rows_retrieved_total": 2,
        "rows_used_map_total": 2,
        "rows_used_reduce_total": 2,
        "row_coverage_ratio": 0.02,
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_chart",
            "selected_route": "chart",
            "chart_spec_generated": True,
            "chart_rendered": True,
            "chart_artifact_available": True,
            "chart_artifact_exists": True,
            "chart_fallback_reason": "none",
            "tabular_sql": {"chart_spec": {"matched_chart_field": "status_code"}},
        },
    }
    async def _fake_tabular_executor(**kwargs):  # noqa: ANN003
        _ = kwargs
        return fake_result

    _prompt, _rag_used, rag_debug, _docs, _caveats, _sources = asyncio.run(
        maybe_run_deterministic_route(
            query="дай график распределения по Status Code",
            user_id=uuid.uuid4(),
            conversation_id=uuid.uuid4(),
            files=[SimpleNamespace(id="f-1")],
            planner_decision=planner_decision,
            planner_decision_payload=planner_payload,
            expected_chunks_total=0,
            rag_mode="auto",
            top_k=3,
            preferred_lang="ru",
            tabular_sql_executor=_fake_tabular_executor,
            rag_retriever_client=None,
            is_combined_intent=False,
        )
    )

    assert rag_debug["short_circuit_response"] is True
    assert "\u0433\u0440\u0430\u0444\u0438\u043a" in rag_debug["short_circuit_response_text"].lower()
    assert rag_debug["artifacts_count"] == 1
    assert rag_debug["artifacts"][0]["url"].startswith("/uploads/")


def test_deterministic_route_missing_artifact_forces_honest_fallback_text():
    planner_decision = QueryPlanDecision(
        route="deterministic_analytics",
        intent=INTENT_TABULAR_AGGREGATE,
        strategy_mode="analytical",
        confidence=0.9,
        requires_clarification=False,
        reason_codes=["tabular_dataset_available"],
    )
    planner_payload = planner_decision.as_dict()
    fake_result = {
        "status": "ok",
        "prompt_context": "chart context",
        "chart_response_text": "The distribution chart for 'Status Code' was generated and is available in Charts.",
        "artifacts": [{"url": "/uploads/tabular_sql/run/chart.png", "path": "uploads/tabular_sql/run/chart.png", "kind": "tabular_chart"}],
        "sources": ["requests.csv | sql_chart"],
        "rows_expected_total": 100,
        "rows_retrieved_total": 2,
        "rows_used_map_total": 2,
        "rows_used_reduce_total": 2,
        "row_coverage_ratio": 0.02,
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_chart",
            "selected_route": "chart",
            "chart_spec_generated": True,
            "chart_rendered": True,
            "chart_artifact_available": False,
            "chart_artifact_exists": False,
            "chart_fallback_reason": "artifact_not_accessible",
            "response_language": "en",
            "tabular_sql": {"chart_spec": {"matched_chart_field": "status_code"}},
        },
    }

    async def _fake_tabular_executor(**kwargs):  # noqa: ANN003
        _ = kwargs
        return fake_result

    _prompt, _rag_used, rag_debug, _docs, _caveats, _sources = asyncio.run(
        maybe_run_deterministic_route(
            query="show the distribution chart by Status Code",
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

    assert rag_debug["short_circuit_response"] is True
    assert "could not be delivered" in rag_debug["short_circuit_response_text"].lower()
    assert "generated and is available" not in rag_debug["short_circuit_response_text"].lower()
    assert rag_debug["fallback_type"] == "tabular_chart_render_failed"
    assert rag_debug["chart_artifact_available"] is False
    assert rag_debug["artifacts_count"] == 0
    assert rag_debug["artifacts"] == []


def test_deterministic_route_missing_artifact_keeps_russian_user_text():
    planner_decision = QueryPlanDecision(
        route="deterministic_analytics",
        intent=INTENT_TABULAR_AGGREGATE,
        strategy_mode="analytical",
        confidence=0.9,
        requires_clarification=False,
        reason_codes=["tabular_dataset_available"],
    )
    planner_payload = planner_decision.as_dict()
    fake_result = {
        "status": "ok",
        "prompt_context": "chart context",
        "chart_response_text": "",
        "artifacts": [{"url": "/uploads/tabular_sql/run/chart.png", "path": "uploads/tabular_sql/run/chart.png", "kind": "tabular_chart"}],
        "sources": ["requests.csv | sql_chart"],
        "rows_expected_total": 100,
        "rows_retrieved_total": 2,
        "rows_used_map_total": 2,
        "rows_used_reduce_total": 2,
        "row_coverage_ratio": 0.02,
        "debug": {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_chart",
            "selected_route": "chart",
            "chart_spec_generated": True,
            "chart_rendered": True,
            "chart_artifact_available": False,
            "chart_artifact_exists": False,
            "chart_fallback_reason": "artifact_not_accessible",
            "response_language": "ru",
            "tabular_sql": {"chart_spec": {"matched_chart_field": "status_code"}},
        },
    }

    async def _fake_tabular_executor(**kwargs):  # noqa: ANN003
        _ = kwargs
        return fake_result

    _prompt, _rag_used, rag_debug, _docs, _caveats, _sources = asyncio.run(
        maybe_run_deterministic_route(
            query="\u043f\u043e\u043a\u0430\u0436\u0438 chart \u043f\u043e Status Code",
            user_id=uuid.uuid4(),
            conversation_id=uuid.uuid4(),
            files=[SimpleNamespace(id="f-1")],
            planner_decision=planner_decision,
            planner_decision_payload=planner_payload,
            expected_chunks_total=0,
            rag_mode="auto",
            top_k=3,
            preferred_lang="ru",
            tabular_sql_executor=_fake_tabular_executor,
            rag_retriever_client=None,
            is_combined_intent=False,
        )
    )

    assert "could not be delivered" not in rag_debug["short_circuit_response_text"].lower()
    assert "\u0433\u0440\u0430\u0444\u0438\u043a" in rag_debug["short_circuit_response_text"].lower()
    assert rag_debug["chart_artifact_available"] is False


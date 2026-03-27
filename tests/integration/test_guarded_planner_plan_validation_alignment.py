from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.schemas.chat import ChatMessage
from app.services.chat import tabular_llm_guarded_planner as guarded_planner
from app.services.chat.tabular_sql import execute_tabular_sql_path
from app.services.chat_orchestrator import ChatOrchestrator
from app.services.tabular.storage_adapter import SharedDuckDBParquetStorageAdapter


RU_CITY_CHART_QUERY = "\u043f\u043e\u0441\u0442\u0440\u043e\u0439 \u0433\u0440\u0430\u0444\u0438\u043a \u043e\u0431\u044a\u0435\u043c\u0430 \u0437\u0430\u0442\u0440\u0430\u0442 \u043f\u043e \u0433\u043e\u0440\u043e\u0434\u0430\u043c"
RU_MONTH_CHART_QUERY = "\u041f\u043e\u043a\u0430\u0436\u0438 \u0433\u0440\u0430\u0444\u0438\u043a \u043e\u0431\u044a\u0435\u043c\u0430 \u0437\u0430\u0442\u0440\u0430\u0442 \u043f\u043e \u043c\u0435\u0441\u044f\u0446\u0430\u043c!"
RU_MONTH_CREATED_AT_CHART_QUERY = "\u041f\u043a\u0430\u0436\u0438 \u0433\u0440\u0430\u0444\u0438\u043a amount_rub \u043f\u043e created_at(\u0432 \u043c\u0435\u0441\u044f\u0446)"
RU_SCHEMA_QUERY = "\u041a\u0430\u043a\u0438\u0435 \u0441\u0442\u043e\u043b\u0431\u0446\u044b \u0432 \u0444\u0430\u0439\u043b\u0435 \u0438 \u0434\u0430\u043d\u043d\u044b\u0435?"
RU_ONE_COLUMN_BROKEN = (
    "\u0432 \u0444\u0430\u0439\u043b\u0435 \u043f\u0440\u0435\u0434\u0441\u0442\u0430\u0432\u043b\u0435\u043d "
    "\u0442\u043e\u043b\u044c\u043a\u043e \u043e\u0434\u0438\u043d \u0441\u0442\u043e\u043b\u0431\u0435\u0446"
)


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
            "request_id,created_at,amount_rub,city,status,department,country,payment_type",
            "1,2026-01-05 10:00:00,100,ekb,new,ops,ru,card",
            "2,2026-01-15 12:00:00,150,msk,new,ops,ru,transfer",
            "3,2026-02-03 09:00:00,240,ekb,approved,sales,ru,card",
            "4,2026-02-20 18:30:00,180,spb,approved,sales,ru,transfer",
            "5,2026-03-08 11:15:00,300,msk,new,finance,ru,card",
            "6,2026-03-18 14:45:00,120,spb,approved,finance,ru,transfer",
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


def _enable_guarded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(guarded_planner.settings, "TABULAR_LLM_GUARDED_PLANNER_ENABLED", True)
    monkeypatch.setattr(guarded_planner.settings, "TABULAR_LLM_GUARDED_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(guarded_planner.settings, "TABULAR_LLM_GUARDED_PLAN_TIMEOUT_SECONDS", 3.0)
    monkeypatch.setattr(guarded_planner.settings, "TABULAR_LLM_GUARDED_EXECUTION_TIMEOUT_SECONDS", 3.0)
    monkeypatch.setattr(guarded_planner.settings, "TABULAR_LLM_GUARDED_PLAN_MAX_TOKENS", 512)
    monkeypatch.setattr(guarded_planner.settings, "TABULAR_LLM_GUARDED_EXECUTION_MAX_TOKENS", 512)


def _llm_payload(response_payload: dict) -> dict:
    return {
        "response": json.dumps(response_payload, ensure_ascii=False),
        "model": "llama-test",
        "model_route": "ollama",
        "route_mode": "explicit",
        "provider_selected": "local",
        "provider_effective": "ollama",
        "fallback_reason": "none",
        "fallback_allowed": False,
        "fallback_attempted": False,
        "fallback_policy_version": "p1-aihub-first-v1",
        "aihub_attempted": False,
        "tokens_used": 64,
    }


def _mock_chart_delivery(monkeypatch: pytest.MonkeyPatch) -> None:
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

    monkeypatch.setattr(guarded_planner, "render_chart_artifact", _fake_chart_delivery)


def test_ru_chart_by_city_guarded_path_plan_validation_alignment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    pytest.importorskip("duckdb")
    _enable_guarded(monkeypatch)
    _mock_chart_delivery(monkeypatch)
    file_obj = _build_file(tmp_path, file_id="city-file")

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        policy_class = str(kwargs.get("policy_class") or "")
        if policy_class == "tabular_llm_guarded_plan":
            return _llm_payload(
                {
                    "task_type": "chart",
                    "requested_output_type": "graph",
                    "measures": [
                        {
                            "requested": "\u043e\u0431\u044a\u0435\u043c \u0437\u0430\u0442\u0440\u0430\u0442",
                            "field": "\u043e\u0431\u044a\u0435\u043c \u0437\u0430\u0442\u0440\u0430\u0442",
                            "aggregation": "sum",
                        }
                    ],
                    "dimensions": [{"requested": "\u043f\u043e \u0433\u043e\u0440\u043e\u0434\u0430\u043c", "field": "city"}],
                    "derived_time_grain": "none",
                    "source_datetime_field": None,
                    "filters": [],
                    "chart_type": "column",
                    "confidence": 0.05,
                    "ambiguity_flags": ["none"],
                }
            )
        if policy_class == "tabular_llm_guarded_execution":
            return _llm_payload(
                {
                    "selected_route": "chart",
                    "requested_output_type": "graph",
                    "measure": {"field": "amount_rub", "aggregation": "sum"},
                    "dimension": {"field": "city"},
                    "derived_time_grain": "none",
                    "source_datetime_field": None,
                    "filters": [],
                    "chart_type": "column",
                    "output_columns": ["group_key", "value"],
                }
            )
        raise AssertionError(f"Unexpected policy class: {policy_class}")

    monkeypatch.setattr(guarded_planner.llm_manager, "generate_response", fake_generate_response)

    result = asyncio.run(execute_tabular_sql_path(query=RU_CITY_CHART_QUERY, files=[file_obj]))

    assert isinstance(result, dict)
    assert result["status"] == "ok"
    debug = result.get("debug") or {}
    assert debug.get("planner_mode") == "llm_guarded"
    assert debug.get("plan_validation_status") == "success"
    assert debug.get("sql_generation_mode") == "llm_guarded_execution_spec"
    assert debug.get("sql_validation_status") == "success"
    assert debug.get("repair_failure_reason") == "none"
    assert debug.get("final_execution_mode") == "llm_guarded"
    assert debug.get("final_selected_route") == "chart"
    assert debug.get("requested_output_type") == "chart"
    assert debug.get("fallback_reason") == "none"

    plan_json = debug.get("analytic_plan_json") if isinstance(debug.get("analytic_plan_json"), dict) else {}
    assert plan_json.get("requested_output_type") == "chart"
    assert plan_json.get("task_type") == "chart"
    assert isinstance(plan_json.get("measures"), list) and plan_json["measures"][0]["field"] == "amount_rub"
    assert isinstance(plan_json.get("dimensions"), list) and plan_json["dimensions"][0]["field"] == "city"
    assert float(plan_json.get("confidence", 0.0) or 0.0) >= 0.2

    trace = ((debug.get("tabular_sql") or {}).get("repair_iteration_trace") or [])
    assert isinstance(trace, list) and trace
    assert str(trace[-1].get("status") or "") == "success"
    assert all(str(item.get("reason") or "") != "low_plan_confidence" for item in trace)
    assert all(str(item.get("reason") or "") != "invalid_selected_route" for item in trace)


@pytest.mark.parametrize("query_text", [RU_MONTH_CHART_QUERY, RU_MONTH_CREATED_AT_CHART_QUERY])
def test_ru_chart_by_month_guarded_temporal_plan_executes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    query_text: str,
) -> None:
    pytest.importorskip("duckdb")
    _enable_guarded(monkeypatch)
    _mock_chart_delivery(monkeypatch)
    file_obj = _build_file(tmp_path, file_id="month-file")
    execution_payloads: list[dict] = []

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        policy_class = str(kwargs.get("policy_class") or "")
        if policy_class == "tabular_llm_guarded_plan":
            return _llm_payload(
                {
                    "task_type": "chart",
                    "requested_output_type": "visualization",
                    "measures": [
                        {
                            "requested": "\u0441\u0443\u043c\u043c\u0430 \u0437\u0430\u0442\u0440\u0430\u0442",
                            "field": "\u0441\u0443\u043c\u043c\u0430 \u0437\u0430\u0442\u0440\u0430\u0442",
                            "aggregation": "sum",
                        }
                    ],
                    "dimensions": [],
                    "derived_time_grain": "month",
                    "source_datetime_field": "created_at",
                    "filters": [],
                    "chart_type": "time_series",
                    "confidence": 0.01,
                    "ambiguity_flags": ["none"],
                }
            )
        if policy_class == "tabular_llm_guarded_execution":
            payload = {
                "task_type": "trend",
                "requested_output_type": "visualization",
                "measure": {"field": "amount_rub", "aggregation": "sum"},
                "dimension": {"field": None},
                "derived_time_grain": "month",
                "source_datetime_field": "created_at",
                "filters": [],
                "chart_type": "time_series",
                "output_columns": ["bucket", "value"],
            }
            execution_payloads.append(dict(payload))
            return _llm_payload(payload)
        raise AssertionError(f"Unexpected policy class: {policy_class}")

    monkeypatch.setattr(guarded_planner.llm_manager, "generate_response", fake_generate_response)

    result = asyncio.run(execute_tabular_sql_path(query=query_text, files=[file_obj]))

    assert isinstance(result, dict)
    assert result["status"] == "ok"
    debug = result.get("debug") or {}
    assert debug.get("planner_mode") == "llm_guarded"
    assert debug.get("plan_validation_status") == "success"
    assert debug.get("sql_validation_status") == "success"
    assert debug.get("repair_failure_reason") == "none"
    assert debug.get("final_execution_mode") == "llm_guarded"
    assert debug.get("final_selected_route") == "chart"
    assert debug.get("selected_route") == "chart"
    assert debug.get("requested_output_type") == "chart"
    assert debug.get("requested_time_grain") == "month"
    assert debug.get("source_datetime_field") == "created_at"
    assert debug.get("fallback_reason") == "none"

    plan_json = debug.get("analytic_plan_json") if isinstance(debug.get("analytic_plan_json"), dict) else {}
    assert plan_json.get("requested_output_type") == "chart"
    assert plan_json.get("derived_time_grain") == "month"
    assert plan_json.get("source_datetime_field") == "created_at"
    assert isinstance(plan_json.get("measures"), list) and plan_json["measures"][0]["field"] == "amount_rub"
    assert float(plan_json.get("confidence", 0.0) or 0.0) >= 0.2

    trace = ((debug.get("tabular_sql") or {}).get("repair_iteration_trace") or [])
    assert isinstance(trace, list) and trace
    assert str(trace[-1].get("status") or "") == "success"
    assert all(str(item.get("reason") or "") != "low_plan_confidence" for item in trace)
    assert all(str(item.get("reason") or "") != "invalid_selected_route" for item in trace)
    assert execution_payloads and "selected_route" not in execution_payloads[0]


def test_schema_summary_real_response_reports_one_table_and_eight_columns(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pytest.importorskip("duckdb")
    file_obj = _build_file(tmp_path, file_id="schema-file")
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    current_user = SimpleNamespace(id=user_id, username="tester")
    message_store: list[SimpleNamespace] = []
    llm_calls = {"count": 0}

    async def fake_get_conversation(db, id):  # noqa: ARG001,A002
        return SimpleNamespace(
            id=conversation_id,
            user_id=user_id,
            model_source="local",
            model_name="llama-test",
        )

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [file_obj]

    async def fake_create_message(db, conversation_id, role, content, **kwargs):  # noqa: ARG001
        message = SimpleNamespace(
            id=uuid.uuid4(),
            conversation_id=conversation_id,
            role=role,
            content=content,
        )
        message_store.append(message)
        return message

    async def fake_get_last_messages(db, conversation_id, count):  # noqa: ARG001
        filtered = [item for item in message_store if item.conversation_id == conversation_id]
        if count <= 0:
            return []
        return filtered[-count:]

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        llm_calls["count"] += 1
        return _llm_payload({"text": RU_ONE_COLUMN_BROKEN, "echo_prompt_len": len(str(kwargs.get("prompt") or ""))})

    monkeypatch.setattr("app.services.chat_orchestrator.crud_conversation.get", fake_get_conversation)
    monkeypatch.setattr("app.services.chat.rag_prompt_builder.crud_file.get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr("app.services.chat_orchestrator.crud_message.create_message", fake_create_message)
    monkeypatch.setattr("app.services.chat_orchestrator.crud_message.get_last_messages", fake_get_last_messages)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.crud_message.create_message", fake_create_message)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.llm_manager.generate_response", fake_generate_response)
    monkeypatch.setattr("app.services.chat.postprocess.llm_manager.generate_response", fake_generate_response)

    orchestrator = ChatOrchestrator()
    response = asyncio.run(
        orchestrator.chat(
            chat_data=ChatMessage(
                message=RU_SCHEMA_QUERY,
                conversation_id=conversation_id,
                model_source="local",
                provider_mode="explicit",
                model_name="llama-test",
                rag_debug=True,
            ),
            db=object(),
            current_user=current_user,
        )
    )

    assert llm_calls["count"] == 0
    assert isinstance(response.response, str) and response.response
    assert "8" in response.response
    assert RU_ONE_COLUMN_BROKEN not in response.response.lower()
    assert response.execution_route == "tabular_sql"
    assert response.response_contract.contract_version == "chat_response_v1"
    assert response.response_contract.execution_route == "tabular_sql"

    rag_debug = response.rag_debug or {}
    assert rag_debug.get("selected_route") == "schema_question"
    assert rag_debug.get("planner_mode") == "deterministic"
    planner_section = ((rag_debug.get("debug_sections") or {}).get("planner") or {})
    assert planner_section.get("planner_mode") == "deterministic"

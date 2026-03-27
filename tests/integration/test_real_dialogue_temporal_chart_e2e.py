import asyncio
import re
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.schemas.chat import ChatMessage
from app.services.chat_orchestrator import ChatOrchestrator
from app.services.tabular.storage_adapter import SharedDuckDBParquetStorageAdapter


RU_GREETING = "\u043f\u0440\u0438\u0432\u0435\u0442\u0438\u043a"
RU_FILE_SUMMARY = "\u041a\u0430\u043a\u0438\u0435 \u0441\u0442\u043e\u043b\u0431\u0446\u044b \u0432 \u0444\u0430\u0439\u043b\u0435 \u0438 \u0434\u0430\u043d\u043d\u044b\u0435?"
RU_MONTHLY_CHART = "\u041f\u043e\u043a\u0430\u0436\u0438 \u0433\u0440\u0430\u0444\u0438\u043a \u043e\u0431\u044a\u0435\u043c\u0430 \u0437\u0430\u0442\u0440\u0430\u0442 \u043f\u043e \u043c\u0435\u0441\u044f\u0446\u0430\u043c!"
RU_MONTH_HINT = "\u043c\u0435\u0441\u044f\u0446\u0430 \u043c\u043e\u0436\u043d\u043e \u0432\u0437\u044f\u0442\u044c \u0438\u0437 \u0434\u0430\u0442 - \u0432\u044b\u0434\u0435\u043b\u0438\u0432 \u0442\u043e\u043b\u044c\u043a\u043e \u043c\u0435\u0441\u044f\u0446\u044b"
RU_DRAW_IT = "\u043e\u0442\u043b\u0438\u0447\u043d\u043e, \u043d\u043e \u043c\u043d\u0435 \u043d\u0443\u0436\u043d\u043e \u0433\u0440\u0430\u0444\u0438\u043a - \u043d\u0430\u0440\u0438\u0441\u0443\u0439"


def _write_csv(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows), encoding="utf-8")


def _has_cyrillic(text: str) -> bool:
    return bool(re.search(r"[\u0400-\u04FF]", str(text or "")))


def _llm_payload(response: str) -> dict:
    return {
        "response": response,
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


def test_real_dialogue_temporal_chart_path_executes_end_to_end(monkeypatch, tmp_path: Path):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")
    monkeypatch.setattr("app.services.chat.tabular_llm_guarded_planner.settings.TABULAR_LLM_GUARDED_PLANNER_ENABLED", False)

    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    current_user = SimpleNamespace(id=user_id, username="tester")

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "spending.csv"
    _write_csv(
        csv_path,
        [
            "request_id,created_at,amount_rub,status",
            "1,2026-01-05 10:00:00,100,new",
            "2,2026-01-18 12:30:00,150,new",
            "3,2026-02-03 09:40:00,200,approved",
            "4,2026-02-15 15:20:00,300,approved",
            "5,2026-03-01 11:00:00,250,new",
            "6,2026-03-20 18:10:00,180,approved",
        ],
    )
    dataset = adapter.ingest(
        file_id=str(file_id),
        file_path=csv_path,
        file_type="csv",
        source_filename="spending.csv",
    )
    assert dataset is not None

    tabular_file = SimpleNamespace(
        id=file_id,
        extension="csv",
        file_type="csv",
        chunks_count=12,
        original_filename="spending.csv",
        custom_metadata={"tabular_dataset": dataset},
    )

    message_store: list[SimpleNamespace] = []

    async def fake_get_conversation(db, id):  # noqa: ARG001,A002
        return SimpleNamespace(
            id=conversation_id,
            user_id=user_id,
            model_source="local",
            model_name="llama-test",
        )

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [tabular_file]

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
        prompt = str(kwargs.get("prompt") or "")
        if "Rewrite the answer strictly in Russian." in prompt:
            return _llm_payload("\u0417\u0434\u0440\u0430\u0432\u0441\u0442\u0432\u0443\u0439\u0442\u0435! \u0427\u0435\u043c \u043c\u043e\u0433\u0443 \u043f\u043e\u043c\u043e\u0447\u044c?")
        if RU_GREETING in prompt:
            return _llm_payload("\u041f\u0440\u0438\u0432\u0435\u0442! \u041a\u0430\u043a \u044f \u043c\u043e\u0433\u0443 \u0432\u0430\u043c helfen?")
        if RU_FILE_SUMMARY in prompt:
            return _llm_payload(
                "\u0412 \u0444\u0430\u0439\u043b\u0435 \u0435\u0441\u0442\u044c \u0441\u0442\u043e\u043b\u0431\u0446\u044b request_id, created_at, amount_rub, status \u0438 \u043f\u0440\u0438\u043c\u0435\u0440\u044b \u0437\u0430\u043f\u0438\u0441\u0435\u0439."
            )
        return _llm_payload("import pandas as pd\nimport matplotlib.pyplot as plt")

    monkeypatch.setattr("app.services.chat_orchestrator.crud_conversation.get", fake_get_conversation)
    monkeypatch.setattr("app.services.chat.rag_prompt_builder.crud_file.get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr("app.services.chat_orchestrator.crud_message.create_message", fake_create_message)
    monkeypatch.setattr("app.services.chat_orchestrator.crud_message.get_last_messages", fake_get_last_messages)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.crud_message.create_message", fake_create_message)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.llm_manager.generate_response", fake_generate_response)
    monkeypatch.setattr("app.services.chat.postprocess.llm_manager.generate_response", fake_generate_response)

    orchestrator = ChatOrchestrator()

    async def run_turn(message: str):
        return await orchestrator.chat(
            chat_data=ChatMessage(
                message=message,
                conversation_id=conversation_id,
                model_source="local",
                provider_mode="explicit",
                model_name="llama-test",
                rag_debug=True,
            ),
            db=object(),
            current_user=current_user,
        )

    async def _run_dialogue():
        turn_1 = await run_turn(RU_GREETING)
        turn_2 = await run_turn(RU_FILE_SUMMARY)
        turn_3 = await run_turn(RU_MONTHLY_CHART)
        turn_4 = await run_turn(RU_MONTH_HINT)
        turn_5 = await run_turn(RU_DRAW_IT)
        return turn_1, turn_2, turn_3, turn_4, turn_5

    turn_1, turn_2, turn_3, turn_4, turn_5 = asyncio.run(_run_dialogue())

    assert _has_cyrillic(turn_1.response)
    assert "helfen" not in turn_1.response.lower()
    assert turn_1.rag_debug["selected_route"] == "general_chat"

    assert _has_cyrillic(turn_2.response)
    assert "import pandas" not in turn_2.response.lower()

    for idx, turn in enumerate((turn_3, turn_4, turn_5), start=3):
        debug = dict(turn.rag_debug or {})
        plan = debug.get("temporal_aggregation_plan") if isinstance(debug.get("temporal_aggregation_plan"), dict) else {}
        assert turn.execution_route == "tabular_sql", f"turn={idx} must stay on deterministic tabular route"
        assert turn.response_contract.execution_route == "tabular_sql"
        assert debug.get("selected_route") in {"chart", "trend", "comparison"}
        assert debug.get("requested_time_grain") == "month"
        assert debug.get("source_datetime_field") == "created_at"
        assert debug.get("temporal_plan_status") == "resolved"
        assert plan.get("operation") == "sum"
        assert plan.get("measure_column") == "amount_rub"
        assert debug.get("fallback_reason") not in {
            "requested_field_not_matched",
            "missing_or_ambiguous_datetime_source",
            "missing_chart_dimension_column",
        }
        assert "import pandas" not in str(turn.response or "").lower()
        assert "matplotlib" not in str(turn.response or "").lower()

    assert turn_4.rag_debug.get("followup_context_used") is True
    assert turn_4.rag_debug.get("prior_tabular_intent_reused") is True
    assert turn_5.rag_debug.get("followup_context_used") is True
    assert turn_5.rag_debug.get("prior_tabular_intent_reused") is True

    chart_or_controlled = (
        bool(turn_5.response_contract.chart_artifact_available)
        or str(turn_5.response_contract.controlled_response_state) == "chart_render_failed"
    )
    assert chart_or_controlled

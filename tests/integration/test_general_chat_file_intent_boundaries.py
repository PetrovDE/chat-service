import asyncio
import uuid
from types import SimpleNamespace

from app.domain.chat.query_planner import (
    INTENT_NARRATIVE_RETRIEVAL,
    ROUTE_NARRATIVE_RETRIEVAL,
    QueryPlanDecision,
)
from app.services.chat import rag_prompt_builder as rag_builder


def _ready_active_processing() -> SimpleNamespace:
    return SimpleNamespace(id=uuid.uuid4(), status="ready")


def _attached_ready_file(*, file_id: uuid.UUID, extension: str = "xlsx") -> SimpleNamespace:
    return SimpleNamespace(
        id=file_id,
        file_type=extension,
        extension=extension,
        chunks_count=5,
        original_filename=f"dataset.{extension}",
        custom_metadata={},
        active_processing=_ready_active_processing(),
    )


def _narrative_planner_decision() -> QueryPlanDecision:
    return QueryPlanDecision(
        route=ROUTE_NARRATIVE_RETRIEVAL,
        intent=INTENT_NARRATIVE_RETRIEVAL,
        strategy_mode="semantic",
        confidence=0.8,
        requires_clarification=False,
        reason_codes=["test_narrative_route"],
    )


def test_no_files_russian_greeting_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u043f\u0440\u0438\u0432\u0435\u0442",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"


def test_no_files_python_chart_code_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)

    final_prompt, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u043d\u0430\u043f\u0438\u0448\u0438 \u043a\u043e\u0434 \u0434\u043b\u044f \u0432\u044b\u0432\u043e\u0434\u0430 \u0433\u0440\u0430\u0444\u0438\u043a\u043e\u0432 \u043d\u0430 python",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert isinstance(final_prompt, str) and final_prompt
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"


def test_no_files_what_is_pandas_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u0447\u0442\u043e \u0442\u0430\u043a\u043e\u0435 pandas",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["fallback_type"] == "none"


def test_attached_file_python_chart_code_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id, extension="xlsx")]

    def fail_query_planner(**kwargs):  # noqa: ANN003
        raise AssertionError("query_planner should not run for general_chat route")

    async def fail_query_rag(**kwargs):  # noqa: ANN003
        raise AssertionError("query_rag should not run for general_chat route")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u043d\u0430\u043f\u0438\u0448\u0438 \u043a\u043e\u0434 \u0434\u043b\u044f \u0432\u044b\u0432\u043e\u0434\u0430 \u0433\u0440\u0430\u0444\u0438\u043a\u043e\u0432 \u043d\u0430 python",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fail_query_planner,
        )
    )

    assert rag_used is False
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"


def test_attached_file_what_is_pandas_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id, extension="xlsx")]

    def fail_query_planner(**kwargs):  # noqa: ANN003
        raise AssertionError("query_planner should not run for general_chat route")

    async def fail_query_rag(**kwargs):  # noqa: ANN003
        raise AssertionError("query_rag should not run for general_chat route")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u0447\u0442\u043e \u0442\u0430\u043a\u043e\u0435 pandas",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fail_query_planner,
        )
    )

    assert rag_used is False
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"


def test_attached_file_schema_question_routes_file_aware(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    planner_called = {"value": False}

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id, extension="xlsx")]

    async def fake_query_rag(**kwargs):  # noqa: ANN003
        ids = kwargs.get("file_ids") or []
        return {
            "docs": [
                {
                    "content": "columns: amount_rub, city, month",
                    "metadata": {"file_id": ids[0], "filename": "dataset.xlsx", "chunk_index": 0},
                    "similarity_score": 0.98,
                }
            ],
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"},
        }

    def fake_query_planner(**kwargs):  # noqa: ANN003
        planner_called["value"] = True
        return _narrative_planner_decision()

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u043a\u0430\u043a\u0438\u0435 \u0441\u0442\u043e\u043b\u0431\u0446\u044b \u0432 \u0444\u0430\u0439\u043b\u0435",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fake_query_planner,
        )
    )

    assert planner_called["value"] is True
    assert rag_used is True
    assert rag_debug["selected_route"] != "general_chat"
    assert rag_debug["retrieval_mode"] != "assistant_direct"
    assert rag_debug["fallback_type"] != "no_context"


def test_attached_file_temporal_chart_routes_file_aware(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    planner_called = {"value": False}

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id, extension="xlsx")]

    async def fake_query_rag(**kwargs):  # noqa: ANN003
        ids = kwargs.get("file_ids") or []
        return {
            "docs": [
                {
                    "content": "monthly aggregation context",
                    "metadata": {"file_id": ids[0], "filename": "dataset.xlsx", "chunk_index": 0},
                    "similarity_score": 0.97,
                }
            ],
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"},
        }

    def fake_query_planner(**kwargs):  # noqa: ANN003
        planner_called["value"] = True
        return _narrative_planner_decision()

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u043f\u043e\u0441\u0442\u0440\u043e\u0439 \u0433\u0440\u0430\u0444\u0438\u043a \u043f\u043e \u043c\u0435\u0441\u044f\u0446\u0430\u043c",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fake_query_planner,
        )
    )

    assert planner_called["value"] is True
    assert rag_used is True
    assert rag_debug["selected_route"] != "general_chat"
    assert rag_debug["retrieval_mode"] != "assistant_direct"
    assert rag_debug["fallback_type"] != "no_context"


def test_no_file_refusal_for_generic_python_chart_how_to(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)

    final_prompt, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="how to build a bar chart in python",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"
    assert "There are no ready files in this chat" not in final_prompt
    assert "Attach a file to this chat" not in final_prompt

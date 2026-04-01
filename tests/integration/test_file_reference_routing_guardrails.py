import asyncio
import uuid
from types import SimpleNamespace

from app.domain.chat.query_planner import (
    INTENT_NARRATIVE_RETRIEVAL,
    ROUTE_NARRATIVE_RETRIEVAL,
    QueryPlanDecision,
)
from app.services.chat import rag_prompt_builder as rag_builder

RU_NUMPY_ERROR_QUERY = (
    "\u0447\u0442\u043e \u043e\u0437\u043d\u0430\u0447\u0430\u0435\u0442 \u043e\u0448\u0438\u0431\u043a\u0430 \u0432 Python: "
    "\u041e\u0448\u0438\u0431\u043a\u0430 \u043f\u0440\u0438 \u0437\u0430\u0433\u0440\u0443\u0437\u043a\u0435 \u0434\u0430\u043d\u043d\u044b\u0445: "
    "'numpy.ndarray' object has no attribute 'itertuples'?"
)
RU_COLUMNS_IN_FILE_QUERY = "\u043a\u0430\u043a\u0438\u0435 \u0441\u0442\u043e\u043b\u0431\u0446\u044b \u0432 \u0444\u0430\u0439\u043b\u0435?"
RU_COLUMN_DESCRIPTION_QUERY = (
    "\u043f\u043e\u043a\u0430\u0436\u0438 \u043e\u043f\u0438\u0441\u0430\u043d\u0438\u0435 "
    "\u043f\u043e \u043a\u0430\u0436\u0434\u043e\u043c\u0443 \u0441\u0442\u043e\u043b\u0431\u0446\u0443"
)
RU_XLSX_DATE_BROKEN_QUERY = (
    "\u0432 \u044d\u0442\u043e\u043c xlsx \u043f\u043e\u0447\u0435\u043c\u0443 "
    "\u0434\u0430\u0442\u0430 \u0441\u043b\u043e\u043c\u0430\u043b\u0430\u0441\u044c?"
)
MATPLOTLIB_REQUEST = "give me a matplotlib chart example"


def _ready_active_processing() -> SimpleNamespace:
    return SimpleNamespace(id=uuid.uuid4(), status="ready")


def _attached_ready_file(*, file_id: uuid.UUID, filename: str = "dataset.xlsx") -> SimpleNamespace:
    extension = filename.rsplit(".", 1)[-1] if "." in filename else "xlsx"
    return SimpleNamespace(
        id=file_id,
        file_type=extension,
        extension=extension,
        chunks_count=5,
        original_filename=filename,
        stored_filename=f"{file_id}_{filename}",
        custom_metadata={},
        active_processing=_ready_active_processing(),
    )


def _narrative_planner_decision() -> QueryPlanDecision:
    return QueryPlanDecision(
        route=ROUTE_NARRATIVE_RETRIEVAL,
        intent=INTENT_NARRATIVE_RETRIEVAL,
        strategy_mode="semantic",
        confidence=0.9,
        requires_clarification=False,
        reason_codes=["test_narrative_route"],
    )


def _assert_general_chat_payload(*, rag_used: bool, rag_debug: dict) -> None:
    assert rag_used is False
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"
    assert rag_debug["requested_file_names"] == []
    assert rag_debug["file_resolution_status"] == "not_requested"


def _stub_narrative_retrieval(monkeypatch):
    async def fake_query_rag(**kwargs):  # noqa: ANN003
        ids = kwargs.get("file_ids") or []
        return {
            "docs": [
                {
                    "content": "table context evidence",
                    "metadata": {"file_id": ids[0] if ids else "none", "filename": "dataset.xlsx", "chunk_index": 0},
                    "similarity_score": 0.98,
                }
            ],
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"},
        }

    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)


def test_no_files_python_error_with_numpy_ndarray_routes_to_general_chat(monkeypatch):
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
            query=RU_NUMPY_ERROR_QUERY,
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    _assert_general_chat_payload(rag_used=rag_used, rag_debug=rag_debug)


def test_attached_files_python_error_with_numpy_ndarray_still_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id)]

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
            query=RU_NUMPY_ERROR_QUERY,
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fail_query_planner,
        )
    )

    _assert_general_chat_payload(rag_used=rag_used, rag_debug=rag_debug)


def test_no_files_matplotlib_request_routes_to_general_chat(monkeypatch):
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
            query=MATPLOTLIB_REQUEST,
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    _assert_general_chat_payload(rag_used=rag_used, rag_debug=rag_debug)


def test_attached_files_matplotlib_request_still_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id)]

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
            query=MATPLOTLIB_REQUEST,
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fail_query_planner,
        )
    )

    _assert_general_chat_payload(rag_used=rag_used, rag_debug=rag_debug)


def test_attached_files_columns_question_remains_file_aware(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    planner_called = {"value": False}

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id)]

    def fake_query_planner(**kwargs):  # noqa: ANN003
        planner_called["value"] = True
        return _narrative_planner_decision()

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    _stub_narrative_retrieval(monkeypatch)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query=RU_COLUMNS_IN_FILE_QUERY,
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


def test_attached_files_schema_followup_description_stays_file_aware(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    planner_called = {"value": False}

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id)]

    def fake_query_planner(**kwargs):  # noqa: ANN003
        planner_called["value"] = True
        return _narrative_planner_decision()

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    _stub_narrative_retrieval(monkeypatch)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query=RU_COLUMN_DESCRIPTION_QUERY,
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fake_query_planner,
            conversation_history=[
                {"role": "user", "content": RU_COLUMNS_IN_FILE_QUERY},
                {"role": "assistant", "content": "I can list schema details."},
            ],
        )
    )

    assert planner_called["value"] is True
    assert rag_used is True
    assert rag_debug["followup_context_used"] is True
    assert rag_debug["prior_tabular_intent_reused"] is True
    assert rag_debug["selected_route"] != "general_chat"
    assert rag_debug["retrieval_mode"] != "assistant_direct"


def test_attached_files_arbitrary_technical_dotted_identifier_does_not_trigger_file_lookup(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    query = "explain numpy.ndarray vs pandas.DataFrame vs matplotlib.pyplot in Python"

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id)]

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
            query=query,
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fail_query_planner,
        )
    )

    _assert_general_chat_payload(rag_used=rag_used, rag_debug=rag_debug)


def test_explicit_uploaded_filename_with_extension_resolves_when_referenced(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    attached = []

    ready_file = SimpleNamespace(
        id=file_id,
        embedding_model="local:nomic-embed-text:latest",
        chunks_count=3,
        original_filename="monthly.xlsx",
        stored_filename=f"{file_id}_monthly.xlsx",
        custom_metadata={"display_name": "monthly.xlsx"},
    )

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    async def fake_get_user_ready_files_for_resolution(db, user_id, limit=300):  # noqa: ARG001
        return [ready_file]

    async def fake_add_file_to_conversation(db, file_id, conversation_id, attached_by_user_id=None):  # noqa: ARG001
        attached.append((str(file_id), str(conversation_id), str(attached_by_user_id)))
        return SimpleNamespace(file_id=file_id, chat_id=conversation_id)

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(
        rag_builder.crud_file,
        "get_user_ready_files_for_resolution",
        fake_get_user_ready_files_for_resolution,
    )
    monkeypatch.setattr(rag_builder.crud_file, "add_file_to_conversation", fake_add_file_to_conversation)
    _stub_narrative_retrieval(monkeypatch)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="tell me about monthly.xlsx",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=lambda **kwargs: _narrative_planner_decision(),  # noqa: ARG005
        )
    )

    assert rag_used is True
    assert attached and attached[0][0] == str(file_id)
    assert rag_debug["file_resolution_status"] == "resolved_unique"
    assert str(file_id) in rag_debug["resolved_file_ids"]
    assert "monthly.xlsx" in rag_debug["resolved_file_names"]


def test_mixed_explicit_file_debug_question_with_xlsx_stays_file_aware(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    planner_called = {"value": False}

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id, filename="events.xlsx")]

    def fake_query_planner(**kwargs):  # noqa: ANN003
        planner_called["value"] = True
        return _narrative_planner_decision()

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    _stub_narrative_retrieval(monkeypatch)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query=RU_XLSX_DATE_BROKEN_QUERY,
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


import asyncio
import uuid
from types import SimpleNamespace

from app.services.chat import rag_prompt_builder as rag_builder


def _ready_active_processing() -> SimpleNamespace:
    return SimpleNamespace(id=uuid.uuid4(), status="ready")


def test_no_files_greeting_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="hello",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert context_docs == []
    assert rag_caveats == []
    assert rag_sources == []
    assert isinstance(final_prompt, str) and final_prompt
    assert isinstance(rag_debug, dict)
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["requires_clarification"] is False
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"
    assert rag_debug["file_resolution_status"] == "not_requested"


def test_no_files_generic_assistant_question_routes_to_general_chat(monkeypatch):
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
            query="what can you do?",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert isinstance(final_prompt, str) and final_prompt
    assert isinstance(rag_debug, dict)
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["requires_clarification"] is False
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"


def test_existing_file_chat_greeting_still_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [
            SimpleNamespace(
                id=file_id,
                file_type="txt",
                extension="txt",
                chunks_count=5,
                original_filename="notes.txt",
                custom_metadata={},
                active_processing=_ready_active_processing(),
            )
        ]

    async def fail_query_rag(**kwargs):  # noqa: ANN003
        raise AssertionError("query_rag should not run for general_chat route")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    final_prompt, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="hello",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert isinstance(final_prompt, str) and final_prompt
    assert isinstance(rag_debug, dict)
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["requires_clarification"] is False
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"


def test_explicit_file_question_without_files_returns_controlled_fallback(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="tell me about this file",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert context_docs == []
    assert rag_caveats == []
    assert rag_sources == []
    assert isinstance(final_prompt, str) and final_prompt
    assert rag_debug["retrieval_mode"] == "no_context_files"
    assert rag_debug["requires_clarification"] is True
    assert rag_debug["fallback_type"] == "no_context"
    assert rag_debug["fallback_reason"] == "no_ready_files_in_chat"
    assert rag_debug["file_resolution_status"] == "no_context_files"


def test_explicit_file_question_with_resolvable_file_uses_file_aware_route(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    ready_file = SimpleNamespace(
        id=file_id,
        embedding_model="local:nomic-embed-text:latest",
        file_type="txt",
        extension="txt",
        chunks_count=3,
        original_filename="test_requests_460_rows.xlsx",
        stored_filename=f"{file_id}_test_requests_460_rows.xlsx",
        custom_metadata={"display_name": "test_requests_460_rows.xlsx"},
    )

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    async def fake_get_user_ready_files_for_resolution(db, user_id, limit=300):  # noqa: ARG001
        return [ready_file]

    async def fake_add_file_to_conversation(db, file_id, conversation_id, attached_by_user_id=None):  # noqa: ARG001
        return SimpleNamespace(file_id=file_id, chat_id=conversation_id)

    async def fake_query_rag(**kwargs):  # noqa: ANN003
        ids = kwargs.get("file_ids") or []
        return {
            "docs": [
                {
                    "content": "rows=460",
                    "metadata": {"file_id": ids[0], "filename": "test_requests_460_rows.xlsx", "chunk_index": 0},
                    "similarity_score": 0.98,
                }
            ],
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"},
        }

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(
        rag_builder.crud_file,
        "get_user_ready_files_for_resolution",
        fake_get_user_ready_files_for_resolution,
    )
    monkeypatch.setattr(rag_builder.crud_file, "add_file_to_conversation", fake_add_file_to_conversation)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    final_prompt, rag_used, rag_debug, context_docs, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="tell me about file test_requests_460_rows.xlsx",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is True
    assert context_docs
    assert "test_requests_460_rows.xlsx" in final_prompt
    assert rag_debug["file_resolution_status"] == "resolved_unique"
    assert str(file_id) in rag_debug["resolved_file_ids"]
    assert rag_debug["retrieval_mode"] != "no_context_files"


def test_no_context_files_not_triggered_for_general_chat_intent(monkeypatch):
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
            query="help me understand this",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] != "no_context_files"
    assert rag_debug["fallback_type"] != "no_context"


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
    assert rag_debug["requires_clarification"] is False
    assert rag_debug["retrieval_mode"] == "assistant_direct"


def test_no_files_chart_request_routes_to_file_aware_fallback(monkeypatch):
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
            query="show chart by Status Code",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert rag_debug["requires_clarification"] is True
    assert rag_debug["retrieval_mode"] == "no_context_files"
    assert rag_debug["fallback_type"] == "no_context"
    assert rag_debug["detected_intent"] == "tabular_analytics"


def test_no_files_temporal_grouping_request_routes_to_file_aware_fallback(monkeypatch):
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
            query="show spending by month",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert rag_debug["requires_clarification"] is True
    assert rag_debug["retrieval_mode"] == "no_context_files"
    assert rag_debug["fallback_type"] == "no_context"
    assert rag_debug["detected_intent"] == "tabular_analytics"


def test_ambiguous_question_without_data_signals_prefers_general_chat(monkeypatch):
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
            query="what can you say?",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["requires_clarification"] is False

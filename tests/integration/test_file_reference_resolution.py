import asyncio
import uuid
from types import SimpleNamespace

from app.domain.chat.query_planner import (
    INTENT_NARRATIVE_RETRIEVAL,
    ROUTE_NARRATIVE_RETRIEVAL,
    QueryPlanDecision,
)
from app.services.chat import rag_prompt_builder as rag_builder


def _narrative_planner(*, query, files):  # noqa: ARG001
    return QueryPlanDecision(
        route=ROUTE_NARRATIVE_RETRIEVAL,
        intent=INTENT_NARRATIVE_RETRIEVAL,
        strategy_mode="semantic",
        confidence=0.9,
        requires_clarification=False,
        reason_codes=["test_narrative_route"],
    )


def test_file_reference_resolution_unique_match_attaches_and_uses_file(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    ready_file = SimpleNamespace(
        id=file_id,
        embedding_model="local:nomic-embed-text:latest",
        chunks_count=3,
        original_filename="test_requests_460_rows.xlsx",
        stored_filename=f"{file_id}_test_requests_460_rows.xlsx",
        custom_metadata={"display_name": "test_requests_460_rows.xlsx"},
    )

    attached = []

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    async def fake_get_user_ready_files_for_resolution(db, user_id, limit=300):  # noqa: ARG001
        return [ready_file]

    async def fake_add_file_to_conversation(db, file_id, conversation_id, attached_by_user_id=None):  # noqa: ARG001
        attached.append((str(file_id), str(conversation_id), str(attached_by_user_id)))
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
            query="расскажи мне про файл test_requests_460_rows.xlsx",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=_narrative_planner,
        )
    )

    assert rag_used is True
    assert context_docs
    assert "test_requests_460_rows.xlsx" in final_prompt
    assert attached and attached[0][0] == str(file_id)
    assert rag_debug["file_resolution_status"] == "resolved_unique"
    assert str(file_id) in rag_debug["resolved_file_ids"]
    assert "test_requests_460_rows.xlsx" in rag_debug["resolved_file_names"]
    assert rag_debug["detected_language"] == "ru"


def test_file_reference_resolution_not_found_returns_controlled_response(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    async def fake_get_user_ready_files_for_resolution(db, user_id, limit=300):  # noqa: ARG001
        return []

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(
        rag_builder.crud_file,
        "get_user_ready_files_for_resolution",
        fake_get_user_ready_files_for_resolution,
    )

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="расскажи про файл missing_report.xlsx",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=_narrative_planner,
        )
    )

    assert rag_used is False
    assert context_docs == []
    assert rag_caveats == []
    assert rag_sources == []
    assert "Не нашёл файл" in final_prompt
    assert rag_debug["file_resolution_status"] == "not_found"
    assert rag_debug["detected_language"] == "ru"


def test_file_reference_resolution_ambiguous_returns_disambiguation(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_a = uuid.uuid4()
    file_b = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    async def fake_get_user_ready_files_for_resolution(db, user_id, limit=300):  # noqa: ARG001
        return [
            SimpleNamespace(
                id=file_a,
                embedding_model="local:nomic-embed-text:latest",
                chunks_count=2,
                original_filename="monthly.xlsx",
                stored_filename=f"{file_a}_monthly.xlsx",
                custom_metadata={},
            ),
            SimpleNamespace(
                id=file_b,
                embedding_model="local:nomic-embed-text:latest",
                chunks_count=2,
                original_filename="monthly.xlsx",
                stored_filename=f"{file_b}_monthly.xlsx",
                custom_metadata={},
            ),
        ]

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(
        rag_builder.crud_file,
        "get_user_ready_files_for_resolution",
        fake_get_user_ready_files_for_resolution,
    )

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="покажи summary для monthly.xlsx",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=_narrative_planner,
        )
    )

    assert rag_used is False
    assert context_docs == []
    assert rag_caveats == []
    assert rag_sources == []
    assert "Уточните, какой файл использовать" in final_prompt
    assert rag_debug["file_resolution_status"] == "ambiguous"
    assert rag_debug["detected_language"] == "ru"

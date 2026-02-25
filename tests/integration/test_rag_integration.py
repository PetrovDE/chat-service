import asyncio
import uuid
from types import SimpleNamespace

from app.services import chat_orchestrator as chat_service
from app.rag.retriever import rag_retriever


def test_mixed_embedding_groups_merge(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    files = [
        SimpleNamespace(id=uuid.uuid4(), embedding_model="local:nomic-embed-text:latest"),
        SimpleNamespace(id=uuid.uuid4(), embedding_model="aihub:arctic"),
    ]

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return files

    async def fake_query_rag(
        query,  # noqa: ARG001
        top_k=5,  # noqa: ARG001
        fetch_k=None,  # noqa: ARG001
        conversation_id=None,  # noqa: ARG001
        user_id=None,  # noqa: ARG001
        file_ids=None,
        embedding_mode="local",
        embedding_model=None,
        score_threshold=None,  # noqa: ARG001
        debug_return=False,  # noqa: ARG001
        rag_mode=None,  # noqa: ARG001
    ):
        return {
            "docs": [
                {
                    "content": f"chunk-{embedding_mode}",
                    "metadata": {"file_id": file_ids[0], "chunk_index": 0, "filename": "f.txt"},
                    "similarity_score": 0.9,
                    "distance": 0.1,
                }
            ],
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid", "embedding_model": embedding_model},
        }

    monkeypatch.setattr(chat_service.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(chat_service.rag_retriever, "query_rag", fake_query_rag)

    final_prompt, rag_used, rag_debug, context_docs = asyncio.run(
        chat_service._try_build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="test query",
            top_k=8,
            model_source="local",
        )
    )

    assert rag_used is True
    assert isinstance(final_prompt, str) and final_prompt
    assert len(context_docs) >= 2
    assert rag_debug["mixed_embeddings"] is True
    assert rag_debug["group_count"] == 2


def test_full_file_truncation_flag(monkeypatch):
    rows = []
    for i in range(900):
        rows.append(
            {
                "id": f"id_{i}",
                "content": f"text-{i}",
                "metadata": {"file_id": "f1", "chunk_index": i, "filename": "big.txt"},
            }
        )

    monkeypatch.setattr(rag_retriever.vectorstore, "get_by_filter", lambda filter_dict=None, limit_per_collection=1000: rows)  # noqa: ARG005

    result = asyncio.run(
        rag_retriever.query_rag(
            "analyze",
            user_id="u1",
            conversation_id="c1",
            rag_mode="full_file",
            debug_return=True,
        )
    )

    debug = result["debug"]
    assert debug["retrieval_mode"] == "full_file"
    assert debug["full_file_limit_hit"] is True
    assert debug["full_file_max_chunks"] == 800


def test_mixed_group_partial_failure_fallback(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_a = SimpleNamespace(id=uuid.uuid4(), embedding_model="local:nomic-embed-text:latest")
    file_b = SimpleNamespace(id=uuid.uuid4(), embedding_model="aihub:arctic")

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [file_a, file_b]

    async def fake_query_rag(
        query,  # noqa: ARG001
        top_k=5,  # noqa: ARG001
        fetch_k=None,  # noqa: ARG001
        conversation_id=None,  # noqa: ARG001
        user_id=None,  # noqa: ARG001
        file_ids=None,
        embedding_mode="local",  # noqa: ARG001
        embedding_model=None,  # noqa: ARG001
        score_threshold=None,  # noqa: ARG001
        debug_return=False,  # noqa: ARG001
        rag_mode=None,  # noqa: ARG001
    ):
        if str(file_b.id) in (file_ids or []):
            raise RuntimeError("provider temporary failure")
        return {
            "docs": [
                {
                    "content": "survived-group",
                    "metadata": {"file_id": str(file_a.id), "chunk_index": 0, "filename": "ok.txt"},
                    "similarity_score": 0.8,
                    "distance": 0.2,
                }
            ],
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"},
        }

    monkeypatch.setattr(chat_service.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(chat_service.rag_retriever, "query_rag", fake_query_rag)

    final_prompt, rag_used, rag_debug, context_docs = asyncio.run(
        chat_service._try_build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="test query",
            top_k=8,
            model_source="local",
        )
    )

    assert rag_used is True
    assert context_docs
    assert "survived-group" in final_prompt
    assert rag_debug["mixed_embeddings"] is True

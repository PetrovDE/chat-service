import asyncio
import uuid
from types import SimpleNamespace

from app.services.chat import rag_prompt_builder as rag_builder


def _make_docs(count: int, file_id: str, filename: str = "doc.txt"):
    docs = []
    for idx in range(count):
        docs.append(
            {
                "content": f"chunk-{idx}",
                "metadata": {
                    "file_id": file_id,
                    "chunk_index": idx,
                    "filename": filename,
                },
                "similarity_score": 1.0,
                "distance": 0.0,
            }
        )
    return docs


def test_dynamic_top_k_short_auto_query_uses_20_percent(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    files = [
        SimpleNamespace(
            id=file_id,
            embedding_model="local:nomic-embed-text:latest",
            chunks_count=50,
            is_processed="completed",
            original_filename="doc.txt",
        )
    ]
    captured = {"top_k": []}

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return files

    async def fake_query_rag(
        query,  # noqa: ARG001
        top_k=5,
        fetch_k=None,  # noqa: ARG001
        conversation_id=None,  # noqa: ARG001
        user_id=None,  # noqa: ARG001
        file_ids=None,  # noqa: ARG001
        embedding_mode="local",  # noqa: ARG001
        embedding_model=None,  # noqa: ARG001
        score_threshold=None,  # noqa: ARG001
        debug_return=False,  # noqa: ARG001
        rag_mode=None,  # noqa: ARG001
    ):
        captured["top_k"].append(top_k)
        return {
            "docs": _make_docs(min(top_k, 50), str(file_id)),
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"},
        }

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)
    monkeypatch.setattr(rag_builder.settings, "RAG_DYNAMIC_ESCALATION_ENABLED", False)

    _, rag_used, rag_debug, context_docs, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="цена?",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is True
    assert captured["top_k"][0] == 10  # 20% from 50 chunks
    assert len(context_docs) == 10
    assert rag_debug["retrieval_policy"]["effective_top_k"] == 10


def test_full_file_mode_uses_100_percent_budget(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    files = [
        SimpleNamespace(
            id=file_id,
            embedding_model="local:nomic-embed-text:latest",
            chunks_count=32,
            is_processed="completed",
            original_filename="sheet.xlsx",
        )
    ]
    captured = {"top_k": []}

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return files

    async def fake_query_rag(
        query,  # noqa: ARG001
        top_k=5,
        fetch_k=None,  # noqa: ARG001
        conversation_id=None,  # noqa: ARG001
        user_id=None,  # noqa: ARG001
        file_ids=None,  # noqa: ARG001
        embedding_mode="local",  # noqa: ARG001
        embedding_model=None,  # noqa: ARG001
        score_threshold=None,  # noqa: ARG001
        debug_return=False,  # noqa: ARG001
        rag_mode=None,  # noqa: ARG001
    ):
        captured["top_k"].append(top_k)
        return {
            "docs": _make_docs(32, str(file_id), filename="sheet.xlsx"),
            "debug": {"intent": "analyze_full_file", "retrieval_mode": "full_file"},
        }

    async def fake_map_reduce(**kwargs):
        return "full-file prompt", {"enabled": True, "truncated_batches": False}

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    final_prompt, rag_used, rag_debug, context_docs, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="analyze full file",
            top_k=8,
            model_source="local",
            rag_mode="full_file",
            full_file_prompt_builder=fake_map_reduce,
        )
    )

    assert rag_used is True
    assert final_prompt == "full-file prompt"
    assert captured["top_k"][0] == 32
    assert len(context_docs) == 32
    assert rag_debug["retrieval_policy"]["ratio"] == 1.0


def test_low_coverage_auto_mode_escalates_to_full_file(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    files = [
        SimpleNamespace(
            id=file_id,
            embedding_model="local:nomic-embed-text:latest",
            chunks_count=60,
            is_processed="completed",
            original_filename="report.xlsx",
        )
    ]
    calls = []

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return files

    async def fake_query_rag(
        query,  # noqa: ARG001
        top_k=5,
        fetch_k=None,  # noqa: ARG001
        conversation_id=None,  # noqa: ARG001
        user_id=None,  # noqa: ARG001
        file_ids=None,  # noqa: ARG001
        embedding_mode="local",  # noqa: ARG001
        embedding_model=None,  # noqa: ARG001
        score_threshold=None,  # noqa: ARG001
        debug_return=False,  # noqa: ARG001
        rag_mode=None,
    ):
        calls.append({"top_k": top_k, "rag_mode": rag_mode})
        if rag_mode == "full_file":
            return {
                "docs": _make_docs(60, str(file_id), filename="report.xlsx"),
                "debug": {"intent": "analyze_full_file", "retrieval_mode": "full_file"},
            }

        return {
            "docs": _make_docs(min(top_k, 8), str(file_id), filename="report.xlsx"),
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"},
        }

    async def fake_map_reduce(**kwargs):
        return "full-file prompt", {"enabled": True, "truncated_batches": False}

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    final_prompt, rag_used, rag_debug, context_docs, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="проверь",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            full_file_prompt_builder=fake_map_reduce,
        )
    )

    assert rag_used is True
    assert final_prompt == "full-file prompt"
    assert len(calls) == 2
    assert calls[0]["top_k"] == 12  # short query => 20% from 60 chunks
    assert calls[1]["rag_mode"] == "full_file"
    assert calls[1]["top_k"] == 60
    assert len(context_docs) == 60
    assert rag_debug["retrieval_policy"]["escalation"]["attempted"] is True
    assert rag_debug["retrieval_policy"]["escalation"]["applied"] is True
    assert rag_debug["rag_mode_effective"] == "full_file"

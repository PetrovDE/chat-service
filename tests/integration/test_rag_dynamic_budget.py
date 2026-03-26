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
        **kwargs,  # noqa: ARG001
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
            query="price in this file?",
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
        **kwargs,  # noqa: ARG001
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
        **kwargs,  # noqa: ARG001
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
            query="check this file",
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


def test_full_file_row_coverage_escalation_repass(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    files = [
        SimpleNamespace(
            id=file_id,
            embedding_model="local:nomic-embed-text:latest",
            chunks_count=12,
            is_processed="completed",
            original_filename="wide.xlsx",
        )
    ]
    calls = []

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return files

    async def fake_query_rag(
        query,  # noqa: ARG001
        top_k=5,  # noqa: ARG001
        fetch_k=None,  # noqa: ARG001
        conversation_id=None,  # noqa: ARG001
        user_id=None,  # noqa: ARG001
        file_ids=None,  # noqa: ARG001
        embedding_mode="local",  # noqa: ARG001
        embedding_model=None,  # noqa: ARG001
        score_threshold=None,  # noqa: ARG001
        debug_return=False,  # noqa: ARG001
        rag_mode=None,
        full_file_max_chunks=None,
        **kwargs,  # noqa: ARG001
    ):
        calls.append({"rag_mode": rag_mode, "full_file_max_chunks": full_file_max_chunks})
        docs = []
        if full_file_max_chunks:
            ranges = [(1, 100), (101, 200), (201, 300)]
        else:
            ranges = [(201, 240), (241, 280), (281, 300)]
        for idx, (row_start, row_end) in enumerate(ranges):
            docs.append(
                {
                    "content": f"chunk-{idx}",
                    "metadata": {
                        "file_id": str(file_id),
                        "chunk_index": idx,
                        "filename": "wide.xlsx",
                        "sheet_name": "Sheet1",
                        "row_start": row_start,
                        "row_end": row_end,
                        "total_rows": 300,
                    },
                    "similarity_score": 1.0,
                    "distance": 0.0,
                }
            )
        return {
            "docs": docs,
            "debug": {"intent": "analyze_full_file", "retrieval_mode": "full_file"},
        }

    async def fake_map_reduce(**kwargs):
        return "full-file prompt", {"enabled": True, "truncated_batches": False}

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)
    monkeypatch.setattr(rag_builder.settings, "RAG_FULL_FILE_MAX_CHUNKS", 800)
    monkeypatch.setattr(rag_builder.settings, "RAG_FULL_FILE_ESCALATION_MAX_CHUNKS", 1600)
    monkeypatch.setattr(rag_builder.settings, "RAG_FULL_FILE_MIN_ROW_COVERAGE", 0.95)

    final_prompt, rag_used, rag_debug, context_docs, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="analyze the full file",
            top_k=8,
            model_source="local",
            rag_mode="full_file",
            full_file_prompt_builder=fake_map_reduce,
        )
    )

    assert rag_used is True
    assert final_prompt == "full-file prompt"
    assert len(calls) == 2
    assert calls[0]["full_file_max_chunks"] is None
    assert calls[1]["full_file_max_chunks"] == 1600
    assert len(context_docs) == 3
    assert rag_debug["retrieval_policy"]["row_escalation"]["attempted"] is True
    assert rag_debug["retrieval_policy"]["row_escalation"]["applied"] is True
    assert rag_debug["row_coverage_ratio"] == 1.0


def test_full_file_silent_row_loss_marks_truncated(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    files = [
        SimpleNamespace(
            id=file_id,
            embedding_model="local:nomic-embed-text:latest",
            chunks_count=3,
            is_processed="completed",
            original_filename="wide.xlsx",
        )
    ]

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return files

    async def fake_query_rag(
        query,  # noqa: ARG001
        top_k=5,  # noqa: ARG001
        fetch_k=None,  # noqa: ARG001
        conversation_id=None,  # noqa: ARG001
        user_id=None,  # noqa: ARG001
        file_ids=None,  # noqa: ARG001
        embedding_mode="local",  # noqa: ARG001
        embedding_model=None,  # noqa: ARG001
        score_threshold=None,  # noqa: ARG001
        debug_return=False,  # noqa: ARG001
        rag_mode=None,  # noqa: ARG001
        full_file_max_chunks=None,  # noqa: ARG001
        **kwargs,  # noqa: ARG001
    ):
        docs = []
        for idx, (row_start, row_end) in enumerate([(250, 266), (267, 283), (284, 300)]):
            docs.append(
                {
                    "content": f"chunk-{idx}",
                    "metadata": {
                        "file_id": str(file_id),
                        "chunk_index": idx,
                        "filename": "wide.xlsx",
                        "sheet_name": "Sheet1",
                        "row_start": row_start,
                        "row_end": row_end,
                        "total_rows": 300,
                    },
                    "similarity_score": 1.0,
                    "distance": 0.0,
                }
            )
        return {
            "docs": docs,
            "debug": {"intent": "analyze_full_file", "retrieval_mode": "full_file"},
        }

    async def fake_map_reduce(**kwargs):
        return "full-file prompt", {"enabled": True, "truncated_batches": False}

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)
    monkeypatch.setattr(rag_builder.settings, "RAG_FULL_FILE_MIN_ROW_COVERAGE", 0.95)

    _, rag_used, rag_debug, _, rag_caveats, _ = asyncio.run(
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
    assert rag_debug["silent_row_loss_detected"] is True
    assert rag_debug["truncated"] is True
    assert rag_debug["row_coverage_ratio"] < 0.2
    assert any("Row-level coverage is incomplete" in caveat for caveat in rag_caveats)

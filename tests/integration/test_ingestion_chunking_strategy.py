import asyncio
import uuid
from pathlib import Path
from types import SimpleNamespace

from app.services import file as file_service


def test_spreadsheet_uses_loader_blocks_without_text_split(monkeypatch):
    calls = {"split_called": False}
    captured = {}

    docs = [
        SimpleNamespace(
            page_content="row block 1 with enough content " * 3,
            metadata={"sheet_name": "S1", "row_start": 1, "row_end": 40},
        ),
        SimpleNamespace(
            page_content="row block 2 with enough content " * 3,
            metadata={"sheet_name": "S1", "row_start": 41, "row_end": 80},
        ),
    ]

    class FakeResult:
        @staticmethod
        def scalars():
            return SimpleNamespace(all=lambda: [])

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        async def execute(self, query):  # noqa: ARG002
            return FakeResult()

    async def fake_update_processing_status(db, **kwargs):  # noqa: ARG001
        return SimpleNamespace()

    async def fake_load_file(path):  # noqa: ARG001
        return docs

    def fake_split_documents(_docs):  # noqa: ANN001
        calls["split_called"] = True
        return _docs

    async def fake_get_file(db, id):  # noqa: ARG001, A002
        return SimpleNamespace(
            id=id,
            user_id=uuid.uuid4(),
            original_filename="f.xlsx",
            file_type="xlsx",
        )

    class FakeEmb:
        def __init__(self, mode, model):  # noqa: ARG002
            pass

        async def embedd_documents_async(self, texts):
            return [[0.1, 0.2, 0.3] for _ in texts]

    def fake_add_document(**kwargs):  # noqa: ANN003
        return True

    async def fake_finalize_ingestion(**kwargs):
        captured["progress"] = kwargs["progress"]
        return "completed"

    monkeypatch.setattr(file_service, "AsyncSessionLocal", lambda: FakeSession())
    monkeypatch.setattr(file_service.crud_file, "update_processing_status", fake_update_processing_status)
    monkeypatch.setattr(file_service.document_loader, "load_file", fake_load_file)
    monkeypatch.setattr(file_service.text_splitter, "split_documents", fake_split_documents)
    monkeypatch.setattr(file_service.crud_file, "get", fake_get_file)
    monkeypatch.setattr(file_service, "EmbeddingsManager", FakeEmb)
    monkeypatch.setattr(file_service.vector_store, "delete_by_metadata", lambda f: 0)  # noqa: ARG005
    monkeypatch.setattr(file_service.vector_store, "add_document", fake_add_document)
    monkeypatch.setattr(file_service, "_finalize_ingestion", fake_finalize_ingestion)

    ok, _retryable = asyncio.run(
        file_service._process_file(
            file_id=uuid.uuid4(),
            file_path=Path("test.xlsx"),
            embedding_mode="aihub",
            embedding_model="arctic",
        )
    )

    assert ok is True
    assert calls["split_called"] is False
    assert captured["progress"]["total_chunks_expected"] == len(docs)


def test_ingestion_emits_stage_status_transitions(monkeypatch):
    statuses = []

    docs = [
        SimpleNamespace(
            page_content=f"row block {idx} with enough content " * 3,
            metadata={"sheet_name": "S1", "row_start": idx * 2 + 1, "row_end": idx * 2 + 2, "chunk_type": "row_group"},
        )
        for idx in range(40)
    ]

    class FakeResult:
        @staticmethod
        def scalars():
            return SimpleNamespace(all=lambda: [])

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        async def execute(self, query):  # noqa: ARG002
            return FakeResult()

    async def fake_update_processing_status(db, **kwargs):  # noqa: ARG001
        statuses.append(kwargs.get("status"))
        return SimpleNamespace()

    async def fake_load_file(path):  # noqa: ARG001
        return docs

    async def fake_get_file(db, id):  # noqa: ARG001, A002
        return SimpleNamespace(
            id=id,
            user_id=uuid.uuid4(),
            original_filename="f.xlsx",
            file_type="xlsx",
            custom_metadata={},
        )

    class FakeEmb:
        def __init__(self, mode, model):  # noqa: ARG002
            pass

        async def embedd_documents_async(self, texts):
            return [[0.1, 0.2, 0.3] for _ in texts]

    monkeypatch.setattr(file_service, "AsyncSessionLocal", lambda: FakeSession())
    monkeypatch.setattr(file_service.crud_file, "update_processing_status", fake_update_processing_status)
    monkeypatch.setattr(file_service.document_loader, "load_file", fake_load_file)
    monkeypatch.setattr(file_service.crud_file, "get", fake_get_file)
    monkeypatch.setattr(file_service, "EmbeddingsManager", FakeEmb)
    monkeypatch.setattr(file_service.vector_store, "delete_by_metadata", lambda f: 0)  # noqa: ARG005
    monkeypatch.setattr(file_service.vector_store, "add_document", lambda **kwargs: True)  # noqa: ANN003

    ok, retryable = asyncio.run(
        file_service._process_file(
            file_id=uuid.uuid4(),
            file_path=Path("transitions.xlsx"),
            embedding_mode="local",
            embedding_model="nomic-embed-text",
        )
    )

    assert ok is True
    assert retryable is False
    assert "parsing" in statuses
    assert "parsed" in statuses
    assert "chunking" in statuses
    assert "embedding" in statuses
    assert "indexing" in statuses
    assert statuses[-1] in {"completed", "partial_failed"}

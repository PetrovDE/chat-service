import asyncio
import uuid
from pathlib import Path
from types import SimpleNamespace

from app.services import file as file_service
from app.services.chat.postprocess import append_caveats_and_sources
from app.services.llm.exceptions import ProviderAuthError


def test_finalize_ingestion_partial_failed_counters(monkeypatch):
    captured = {}

    async def fake_update_processing_status(db, **kwargs):  # noqa: ARG001
        captured.update(kwargs)
        return SimpleNamespace()

    async def fake_load_lifecycle_context(db, *, file_id):  # noqa: ARG001
        return {"file_id": str(file_id), "user_id": None, "chat_ids": []}

    monkeypatch.setattr(file_service.crud_file, "update_processing_status", fake_update_processing_status)
    monkeypatch.setattr(file_service, "_load_file_lifecycle_context", fake_load_lifecycle_context)

    progress = {
        "status": "processing",
        "stage": "embed_upsert",
        "total_chunks_expected": 10,
        "chunks_processed": 10,
        "chunks_failed": 2,
        "chunks_indexed": 8,
        "started_at": "2026-01-01T00:00:00+00:00",
        "finished_at": None,
    }

    status = asyncio.run(
        file_service._finalize_ingestion(
            db=object(),
            file_id=uuid.uuid4(),
            progress=progress,
            embedding_mode="local",
            embedding_model="nomic-embed-text",
            error_message=None,
        )
    )

    assert status == "partial_failed"
    patch = captured["metadata_patch"]["ingestion_progress"]
    assert patch["total_chunks_expected"] == 10
    assert patch["chunks_processed"] == 10
    assert patch["chunks_indexed"] == 8
    assert patch["chunks_failed"] == 2
    assert patch["chunks_processed"] == patch["chunks_indexed"] + patch["chunks_failed"]


def test_finalize_ingestion_normalizes_processed_gt_expected(monkeypatch):
    captured = {}

    async def fake_update_processing_status(db, **kwargs):  # noqa: ARG001
        captured.update(kwargs)
        return SimpleNamespace()

    async def fake_load_lifecycle_context(db, *, file_id):  # noqa: ARG001
        return {"file_id": str(file_id), "user_id": None, "chat_ids": []}

    monkeypatch.setattr(file_service.crud_file, "update_processing_status", fake_update_processing_status)
    monkeypatch.setattr(file_service, "_load_file_lifecycle_context", fake_load_lifecycle_context)

    progress = {
        "status": "processing",
        "stage": "embed_upsert",
        "total_chunks_expected": 2,
        "chunks_processed": 3,
        "chunks_failed": 1,
        "chunks_indexed": 2,
        "started_at": "2026-01-01T00:00:00+00:00",
        "finished_at": None,
    }

    status = asyncio.run(
        file_service._finalize_ingestion(
            db=object(),
            file_id=uuid.uuid4(),
            progress=progress,
            embedding_mode="local",
            embedding_model="nomic-embed-text",
            error_message=None,
        )
    )

    assert status == "partial_failed"
    patch = captured["metadata_patch"]["ingestion_progress"]
    assert patch["total_chunks_expected"] == 3
    assert patch["chunks_processed"] == 3
    assert patch["chunks_indexed"] + patch["chunks_failed"] == patch["chunks_processed"]


def test_process_file_always_finalizes_on_exception(monkeypatch):
    finalized = {"called": False}

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
        raise RuntimeError("loader broken")

    async def fake_finalize_ingestion(**kwargs):
        finalized["called"] = True
        return "failed"

    monkeypatch.setattr(file_service, "AsyncSessionLocal", lambda: FakeSession())
    monkeypatch.setattr(file_service.crud_file, "update_processing_status", fake_update_processing_status)
    monkeypatch.setattr(file_service.document_loader, "load_file", fake_load_file)
    monkeypatch.setattr(file_service, "_finalize_ingestion", fake_finalize_ingestion)

    ok, retryable = asyncio.run(
        file_service._process_file(
            file_id=uuid.uuid4(),
            file_path=Path("broken.xlsx"),
            embedding_mode="local",
            embedding_model="nomic",
        )
    )

    assert finalized["called"] is True
    assert ok is False
    assert isinstance(retryable, bool)


def test_process_file_auth_error_marks_failed_without_retry(monkeypatch):
    status_updates = []

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
        status_updates.append(kwargs.get("status"))
        return SimpleNamespace()

    async def fake_load_file(path):  # noqa: ARG001
        return [
            SimpleNamespace(
                page_content="row block 1 with enough content " * 3,
                metadata={"sheet_name": "S1", "row_start": 1, "row_end": 40, "chunk_type": "row_group"},
            )
        ]

    async def fake_get_file(db, id):  # noqa: ARG001, A002
        return SimpleNamespace(
            id=id,
            user_id=uuid.uuid4(),
            original_filename="f.xlsx",
            file_type="xlsx",
            custom_metadata={},
        )

    class AuthFailEmb:
        def __init__(self, mode, model):  # noqa: ARG002
            pass

        async def embedd_documents_async(self, texts):  # noqa: ARG002
            raise ProviderAuthError("unauthorized", provider="aihub", status_code=401)

    monkeypatch.setattr(file_service, "AsyncSessionLocal", lambda: FakeSession())
    monkeypatch.setattr(file_service.crud_file, "update_processing_status", fake_update_processing_status)
    monkeypatch.setattr(file_service.document_loader, "load_file", fake_load_file)
    monkeypatch.setattr(file_service.crud_file, "get", fake_get_file)
    monkeypatch.setattr(file_service, "EmbeddingsManager", AuthFailEmb)
    monkeypatch.setattr(file_service.vector_store, "delete_by_metadata", lambda f: 0)  # noqa: ARG005
    monkeypatch.setattr(file_service.vector_store, "add_document", lambda **kwargs: True)  # noqa: ANN003

    ok, retryable = asyncio.run(
        file_service._process_file(
            file_id=uuid.uuid4(),
            file_path=Path("auth_fail.xlsx"),
            embedding_mode="aihub",
            embedding_model="arctic",
        )
    )

    assert ok is False
    assert retryable is False
    assert "failed" in status_updates
    assert status_updates[-1] == "failed"


def test_answer_keeps_detail_and_adds_limitations_section():
    original = "Detailed answer: revenue and margin by quarter."
    caveats = ["Some chunks failed for sheet Q4."]
    sources = ["finance.xlsx | sheet=Summary | chunk=12"]

    merged = append_caveats_and_sources(original, caveats, sources)

    assert original in merged
    assert "Ограничения/нехватка данных" in merged
    assert "Источники (кратко)" in merged
    assert "Q4" in merged

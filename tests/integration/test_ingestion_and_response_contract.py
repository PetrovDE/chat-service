import asyncio
import uuid
from pathlib import Path
from types import SimpleNamespace

from app.services import file as file_service
from app.services import chat_orchestrator as chat_service


def test_finalize_ingestion_partial_success_counters(monkeypatch):
    captured = {}

    async def fake_update_processing_status(db, **kwargs):  # noqa: ARG001
        captured.update(kwargs)
        return SimpleNamespace()

    monkeypatch.setattr(file_service.crud_file, "update_processing_status", fake_update_processing_status)

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

    assert status == "partial_success"
    patch = captured["metadata_patch"]["ingestion_progress"]
    assert patch["total_chunks_expected"] == 10
    assert patch["chunks_processed"] == 10
    assert patch["chunks_indexed"] == 8
    assert patch["chunks_failed"] == 2
    assert patch["chunks_processed"] == patch["chunks_indexed"] + patch["chunks_failed"]


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


def test_answer_keeps_detail_and_adds_limitations_section():
    original = "Detailed answer: revenue and margin by quarter."
    caveats = ["Some chunks failed for sheet Q4."]
    sources = ["finance.xlsx | sheet=Summary | chunk=12"]

    merged = chat_service._append_caveats_and_sources(original, caveats, sources)

    assert original in merged
    assert "Ограничения/нехватка данных" in merged
    assert "Источники (кратко)" in merged
    assert "Q4" in merged

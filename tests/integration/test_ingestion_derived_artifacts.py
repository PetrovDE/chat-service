from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from types import SimpleNamespace

from langchain_core.documents import Document

from app.services import file as file_service


def test_ingestion_persists_derived_artifacts_and_vector_metadata(monkeypatch, tmp_path: Path):
    docs = [
        Document(
            page_content="Table file summary: csv with sales data",
            metadata={"chunk_type": "file_summary", "artifact_type": "file_summary", "source_type": "tabular"},
        ),
        Document(
            page_content="Sheet summary: sheet=CSV total_rows=2 selected_columns=2",
            metadata={"chunk_type": "sheet_summary", "sheet_name": "CSV", "artifact_type": "sheet_summary"},
        ),
        Document(
            page_content="sheet=CSV rows=1-2/2\nRow 1: client: A | amount: 10\nRow 2: client: B | amount: 20",
            metadata={
                "chunk_type": "row_group",
                "sheet_name": "CSV",
                "row_start": 1,
                "row_end": 2,
                "total_rows": 2,
                "artifact_type": "row_group",
            },
        ),
    ]

    captured = {"vector_meta": None, "finalize_kwargs": None}
    file_id = uuid.uuid4()
    processing_id = uuid.uuid4()
    user_id = uuid.uuid4()
    csv_path = tmp_path / "sales.csv"
    csv_path.write_text("client,amount\nA,10\nB,20\n", encoding="utf-8")

    class FakeResult:
        @staticmethod
        def scalars():
            return SimpleNamespace(all=lambda: [uuid.uuid4()])

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

    async def fake_get_file(db, id):  # noqa: ARG001, A002
        return SimpleNamespace(
            id=id,
            user_id=user_id,
            original_filename="sales.csv",
            extension="csv",
            file_type="csv",
            custom_metadata={},
        )

    class FakeEmb:
        def __init__(self, mode, model):  # noqa: ARG002
            pass

        async def embedd_documents_async(self, texts):
            return [[0.1, 0.2, 0.3] for _ in texts]

    def fake_add_document(**kwargs):  # noqa: ANN003
        if captured["vector_meta"] is None:
            captured["vector_meta"] = dict(kwargs.get("metadata") or {})
        return True

    async def fake_finalize_ingestion(**kwargs):
        captured["finalize_kwargs"] = kwargs
        return "completed"

    monkeypatch.setattr(file_service.settings, "RUNTIME_FILE_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    monkeypatch.setattr(file_service, "AsyncSessionLocal", lambda: FakeSession())
    monkeypatch.setattr(file_service.crud_file, "update_processing_status", fake_update_processing_status)
    monkeypatch.setattr(file_service.document_loader, "load_file", fake_load_file)
    monkeypatch.setattr(file_service.crud_file, "get", fake_get_file)
    monkeypatch.setattr(file_service, "EmbeddingsManager", FakeEmb)
    monkeypatch.setattr(file_service.vector_store, "delete_by_metadata", lambda f: 0)  # noqa: ARG005
    monkeypatch.setattr(file_service.vector_store, "add_document", fake_add_document)
    monkeypatch.setattr(file_service, "_finalize_ingestion", fake_finalize_ingestion)

    ok, retryable = asyncio.run(
        file_service._process_file(
            file_id=file_id,
            processing_id=processing_id,
            file_path=csv_path,
            embedding_mode="local",
            embedding_model="nomic-embed-text",
            pipeline_version="pipeline-v2",
            parser_version="parser-v2",
            artifact_version="artifact-v2",
        )
    )

    assert ok is True
    assert retryable is False
    assert captured["vector_meta"] is not None
    assert captured["vector_meta"]["owner_user_id"] == str(user_id)
    assert captured["vector_meta"]["processing_id"] == str(processing_id)
    assert captured["vector_meta"]["source_type"] == "tabular"
    assert captured["vector_meta"]["artifact_type"] in {"file_summary", "sheet_summary", "row_group"}
    assert captured["vector_meta"]["pipeline_version"] == "pipeline-v2"

    finalize_kwargs = captured["finalize_kwargs"] or {}
    extra_metadata = dict(finalize_kwargs.get("extra_metadata") or {})
    derived = dict(extra_metadata.get("derived_artifacts") or {})
    assert derived
    manifest_path = Path(str(derived.get("manifest_path")))
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["source_type"] == "tabular"
    assert manifest["artifact_counts"]["row_group"] >= 1
    assert "tabular" in manifest

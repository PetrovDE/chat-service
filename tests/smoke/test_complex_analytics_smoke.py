import asyncio
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.chat import rag_prompt_builder as rag_builder
from app.services.tabular.storage_adapter import SharedDuckDBParquetStorageAdapter


def _write_csv(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows), encoding="utf-8")


def test_complex_analytics_request_routes_to_executor_with_artifacts(tmp_path: Path, monkeypatch):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "smoke_comments.csv"
    _write_csv(
        csv_path,
        [
            "application_id,comment_time,comment_text,office",
            "a1,2026-02-01 10:00:00,Need callback,EKB",
            "a2,2026-02-02 11:00:00,All good,MSK",
            "a3,2026-02-03 09:00:00,Need docs,EKB",
        ],
    )
    dataset = adapter.ingest(
        file_id="smoke-1",
        file_path=csv_path,
        file_type="csv",
        source_filename="smoke_comments.csv",
    )
    assert dataset is not None
    file_obj = SimpleNamespace(
        id=uuid.uuid4(),
        file_type="csv",
        embedding_model="local:nomic-embed-text",
        chunks_count=6,
        is_processed="completed",
        original_filename="smoke_comments.csv",
        custom_metadata={"tabular_dataset": dataset},
    )

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [file_obj]

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=uuid.uuid4(),
            conversation_id=uuid.uuid4(),
            query="Run Python/pandas NLP on comment_text and generate heatmap by office and comment_time",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert context_docs == []
    assert rag_caveats == []
    assert "Full Analytics Report" in final_prompt
    assert "```python" in final_prompt
    assert "](/uploads/" in final_prompt
    assert rag_debug["execution_route"] == "complex_analytics"
    assert rag_debug["executor_status"] == "success"
    assert int(rag_debug["artifacts_count"]) >= 1
    assert any(str(a.get("url") or "").startswith("/uploads/") for a in (rag_debug.get("artifacts") or []))
    assert rag_debug["short_circuit_response"] is True
    assert rag_sources

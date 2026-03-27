from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pytest

import app.rag.vector_store as vector_store_module
from app.rag.vector_store import VectorStoreManager


@dataclass
class _FakeCollection:
    name: str
    metadata: Dict[str, Any]
    rows: List[Dict[str, Any]] = field(default_factory=list)

    def count(self) -> int:
        return len(self.rows)

    def upsert(self, *, documents, metadatas, embeddings, ids=None):  # noqa: ANN001
        _ = embeddings
        for idx, document in enumerate(documents):
            self.rows.append(
                {
                    "id": (ids[idx] if ids else f"{self.name}:{len(self.rows)}"),
                    "document": document,
                    "metadata": dict(metadatas[idx] or {}),
                }
            )

    add = upsert

    def query(self, *, query_embeddings, n_results, where=None):  # noqa: ANN001
        _ = query_embeddings
        _ = where
        matched = list(self.rows[:n_results])
        return {
            "ids": [[row["id"] for row in matched]],
            "documents": [[row["document"] for row in matched]],
            "metadatas": [[row["metadata"] for row in matched]],
            "distances": [[0.1 for _ in matched]],
        }

    def get(self, *, where=None, include=None, limit=1000):  # noqa: ANN001
        _ = where
        _ = include
        matched = list(self.rows[:limit])
        return {
            "ids": [row["id"] for row in matched],
            "documents": [row["document"] for row in matched],
            "metadatas": [row["metadata"] for row in matched],
        }

    def delete(self, *, where=None):  # noqa: ANN001
        _ = where
        self.rows = []


class _TrackingPersistentClient:
    created_paths: List[str] = []

    def __init__(self, path: str):
        self.created_paths.append(path)
        self.collections: Dict[str, _FakeCollection] = {}

    def get_or_create_collection(self, *, name: str, metadata: Dict[str, Any]):
        if name not in self.collections:
            self.collections[name] = _FakeCollection(name=name, metadata=dict(metadata or {}))
        return self.collections[name]

    def list_collections(self):
        return list(self.collections.values())

    def get_collection(self, *, name: str):
        return self.collections[name]


class _TrackingEphemeralClient(_TrackingPersistentClient):
    created_count = 0

    def __init__(self):
        type(self).created_count += 1
        self.collections = {}


def _reset_shared_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(VectorStoreManager, "_shared_clients", {})


def test_chroma_client_is_lazy_initialized(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _reset_shared_clients(monkeypatch)
    _TrackingPersistentClient.created_paths = []
    monkeypatch.setattr(vector_store_module, "PersistentClient", _TrackingPersistentClient)
    monkeypatch.setattr(vector_store_module.settings, "VECTORDB_EPHEMERAL_MODE", False)

    store = VectorStoreManager(base_collection_name="documents", persist_directory=str(tmp_path))
    assert _TrackingPersistentClient.created_paths == []

    ok = store.add_document(
        content="hello",
        metadata={"file_id": "f1", "user_id": "u1"},
        embedding=[0.1, 0.2, 0.3],
        doc_id="doc-1",
    )
    assert ok is True
    assert _TrackingPersistentClient.created_paths == [str(tmp_path)]


def test_shared_client_reused_for_same_persist_path(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _reset_shared_clients(monkeypatch)
    _TrackingPersistentClient.created_paths = []
    monkeypatch.setattr(vector_store_module, "PersistentClient", _TrackingPersistentClient)
    monkeypatch.setattr(vector_store_module.settings, "VECTORDB_EPHEMERAL_MODE", False)

    store_a = VectorStoreManager(base_collection_name="documents", persist_directory=str(tmp_path))
    store_b = VectorStoreManager(base_collection_name="documents", persist_directory=str(tmp_path))

    assert store_a.add_document(
        content="a",
        metadata={"file_id": "f1", "user_id": "u1"},
        embedding=[0.1],
        doc_id="a1",
    )
    assert store_b.add_document(
        content="b",
        metadata={"file_id": "f1", "user_id": "u1"},
        embedding=[0.2],
        doc_id="b1",
    )

    assert len(_TrackingPersistentClient.created_paths) == 1
    assert _TrackingPersistentClient.created_paths[0] == str(tmp_path)


def test_ephemeral_mode_uses_ephemeral_client(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    _reset_shared_clients(monkeypatch)
    _TrackingPersistentClient.created_paths = []
    _TrackingEphemeralClient.created_count = 0
    monkeypatch.setattr(vector_store_module, "PersistentClient", _TrackingPersistentClient)
    monkeypatch.setattr(vector_store_module, "EphemeralClient", _TrackingEphemeralClient)
    monkeypatch.setattr(vector_store_module.settings, "VECTORDB_EPHEMERAL_MODE", True)

    store = VectorStoreManager(base_collection_name="documents", persist_directory=str(tmp_path))
    assert store.add_document(
        content="ephemeral",
        metadata={"file_id": "f1", "user_id": "u1"},
        embedding=[0.1],
        doc_id="e1",
    )

    assert _TrackingEphemeralClient.created_count == 1
    assert _TrackingPersistentClient.created_paths == []


def test_failure_logging_includes_recovery_hint(monkeypatch: pytest.MonkeyPatch, tmp_path, caplog) -> None:
    class _FailingPersistentClient:
        def __init__(self, path: str):  # noqa: ARG002
            raise RuntimeError("open failed")

    _reset_shared_clients(monkeypatch)
    monkeypatch.setattr(vector_store_module, "PersistentClient", _FailingPersistentClient)
    monkeypatch.setattr(vector_store_module.settings, "VECTORDB_EPHEMERAL_MODE", False)
    store = VectorStoreManager(base_collection_name="documents", persist_directory=str(tmp_path))

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError):
            _ = store.client

    joined = " ".join(record.getMessage() for record in caplog.records)
    assert "persist_directory" in joined
    assert "VECTORDB_EPHEMERAL_MODE=true" in joined

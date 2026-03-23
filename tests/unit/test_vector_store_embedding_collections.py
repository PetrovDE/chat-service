from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import app.rag.vector_store as vector_store_module
from app.rag.vector_store import VectorStoreManager


def _match_where(where: Optional[Dict[str, Any]], metadata: Dict[str, Any]) -> bool:
    if not where:
        return True
    if "$and" in where:
        return all(_match_where(item, metadata) for item in where["$and"] if isinstance(item, dict))
    if "$or" in where:
        return any(_match_where(item, metadata) for item in where["$or"] if isinstance(item, dict))
    for key, value in where.items():
        if isinstance(value, dict) and "$in" in value:
            if metadata.get(key) not in value["$in"]:
                return False
            continue
        if metadata.get(key) != value:
            return False
    return True


@dataclass
class _FakeCollection:
    name: str
    metadata: Dict[str, Any]
    rows: List[Dict[str, Any]] = field(default_factory=list)

    def count(self) -> int:
        return len(self.rows)

    def upsert(self, *, documents, metadatas, embeddings, ids=None):  # noqa: ANN001
        for idx, doc in enumerate(documents):
            self.rows.append(
                {
                    "id": (ids[idx] if ids else f"{self.name}:{len(self.rows)}"),
                    "document": doc,
                    "metadata": dict(metadatas[idx] or {}),
                    "embedding": list(embeddings[idx] or []),
                }
            )

    add = upsert

    def query(self, *, query_embeddings, n_results, where=None):  # noqa: ANN001
        _ = query_embeddings
        matched = [row for row in self.rows if _match_where(where, row["metadata"])]
        matched = matched[:n_results]
        return {
            "ids": [[row["id"] for row in matched]],
            "documents": [[row["document"] for row in matched]],
            "metadatas": [[row["metadata"] for row in matched]],
            "distances": [[0.1 for _ in matched]],
        }

    def get(self, *, where=None, include=None, limit=1000):  # noqa: ANN001
        _ = include
        matched = [row for row in self.rows if _match_where(where, row["metadata"])]
        matched = matched[:limit]
        return {
            "ids": [row["id"] for row in matched],
            "documents": [row["document"] for row in matched],
            "metadatas": [row["metadata"] for row in matched],
        }

    def delete(self, *, where=None):  # noqa: ANN001
        self.rows = [row for row in self.rows if not _match_where(where, row["metadata"])]


class _FakePersistentClient:
    def __init__(self, path: str):  # noqa: ARG002
        self.collections: Dict[str, _FakeCollection] = {}

    def get_or_create_collection(self, *, name: str, metadata: Dict[str, Any]):
        if name not in self.collections:
            self.collections[name] = _FakeCollection(name=name, metadata=dict(metadata or {}))
        return self.collections[name]

    def list_collections(self):
        return list(self.collections.values())

    def get_collection(self, *, name: str):
        return self.collections[name]


def test_model_scoped_collections_prevent_embedding_mix(monkeypatch, tmp_path):
    monkeypatch.setattr(vector_store_module, "PersistentClient", _FakePersistentClient)
    store = VectorStoreManager(base_collection_name="documents", persist_directory=str(tmp_path))

    qwen_meta = {
        "file_id": "file-qwen",
        "user_id": "u1",
        "embedding_mode": "local",
        "embedding_model": "qwen3-emb",
    }
    arctic_meta = {
        "file_id": "file-arctic",
        "user_id": "u1",
        "embedding_mode": "aihub",
        "embedding_model": "arctic",
    }

    assert store.add_document(content="qwen row", metadata=qwen_meta, embedding=[0.1, 0.2, 0.3], doc_id="q1")
    assert store.add_document(content="arctic row", metadata=arctic_meta, embedding=[0.1, 0.2, 0.3], doc_id="a1")

    collections = list(store.client.collections.keys())
    assert len(collections) == 2
    assert collections[0] != collections[1]
    assert any("qwen3-emb" in name for name in collections)
    assert any("arctic" in name for name in collections)

    qwen_rows = store.query(
        embedding_query=[0.1, 0.2, 0.3],
        top_k=5,
        filter_dict={"embedding_mode": "local", "embedding_model": "qwen3-emb"},
    )
    assert len(qwen_rows) == 1
    assert qwen_rows[0]["metadata"]["file_id"] == "file-qwen"
    assert qwen_rows[0]["metadata"]["embedding_model"] == "qwen3-emb"
    assert "qwen3-emb" in str(qwen_rows[0]["metadata"].get("collection") or "")


def test_collection_dimension_follows_embedding_vector_size(monkeypatch, tmp_path):
    monkeypatch.setattr(vector_store_module, "PersistentClient", _FakePersistentClient)
    store = VectorStoreManager(base_collection_name="documents", persist_directory=str(tmp_path))

    metadata = {
        "file_id": "file-4096",
        "user_id": "u1",
        "embedding_mode": "aihub",
        "embedding_model": "qwen3-emb",
    }
    vector = [0.01] * 4096

    assert store.add_document(content="row-4096", metadata=metadata, embedding=vector, doc_id="doc-4096")
    collections = list(store.client.collections.keys())
    assert len(collections) == 1
    assert "_4096d_" in collections[0]

    rows = store.query(
        embedding_query=vector,
        top_k=5,
        filter_dict={"embedding_mode": "aihub", "embedding_model": "qwen3-emb"},
    )
    assert len(rows) == 1
    assert rows[0]["metadata"]["embedding_dimension"] == 4096

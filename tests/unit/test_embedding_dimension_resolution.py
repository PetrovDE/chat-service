import asyncio

import pytest

from app.rag import embeddings as embeddings_module
from app.rag.embeddings import EmbeddingsManager
from app.services.llm.provider_clients import ProviderRegistry


def _reset_provider_registry(monkeypatch):
    registry = ProviderRegistry(embeddings_module.llm_manager.providers)
    monkeypatch.setattr(embeddings_module.llm_manager, "provider_registry", registry)
    return registry


def test_aihub_qwen3_emb_expected_dimension_is_4096(monkeypatch):
    monkeypatch.setattr(embeddings_module.settings, "EMBEDDINGS_DIM", 0)
    monkeypatch.setattr(embeddings_module.settings, "EMBEDDING_MODEL_DIMENSIONS", "aihub:qwen3-emb=4096")
    registry = _reset_provider_registry(monkeypatch)

    async def _fake_generate_embedding(*, text, model_source=None, model_name=None):  # noqa: ARG001
        return [0.0] * 4096

    monkeypatch.setattr(embeddings_module.llm_manager, "generate_embedding", _fake_generate_embedding)

    manager = EmbeddingsManager(mode="aihub", model="qwen3-emb")
    vectors = asyncio.run(manager.embedd_documents_async(["hello"]))

    assert len(vectors) == 1
    assert len(vectors[0]) == 4096
    dim = registry.resolve_embedding_dimension_decision("aihub", "qwen3-emb")
    assert dim.dimension == 4096
    assert dim.source == "model_metadata"


def test_local_embedding_dimension_comes_from_active_model_metadata(monkeypatch):
    monkeypatch.setattr(embeddings_module.settings, "EMBEDDINGS_DIM", 0)
    monkeypatch.setattr(
        embeddings_module.settings,
        "EMBEDDING_MODEL_DIMENSIONS",
        "local:nomic-embed-text:latest=768",
    )
    registry = _reset_provider_registry(monkeypatch)

    async def _fake_generate_embedding(*, text, model_source=None, model_name=None):  # noqa: ARG001
        return [0.0] * 768

    monkeypatch.setattr(embeddings_module.llm_manager, "generate_embedding", _fake_generate_embedding)

    manager = EmbeddingsManager(mode="local", model="nomic-embed-text:latest")
    vectors = asyncio.run(manager.embedd_documents_async(["hello"]))

    assert len(vectors) == 1
    assert len(vectors[0]) == 768
    dim = registry.resolve_embedding_dimension_decision("local", "nomic-embed-text:latest")
    assert dim.dimension == 768
    assert dim.source == "model_metadata"


def test_dimension_mismatch_fails_with_clear_error(monkeypatch):
    monkeypatch.setattr(embeddings_module.settings, "EMBEDDINGS_DIM", 0)
    monkeypatch.setattr(embeddings_module.settings, "EMBEDDING_MODEL_DIMENSIONS", "aihub:qwen3-emb=4096")
    _reset_provider_registry(monkeypatch)

    async def _fake_generate_embedding(*, text, model_source=None, model_name=None):  # noqa: ARG001
        return [0.0] * 1024

    monkeypatch.setattr(embeddings_module.llm_manager, "generate_embedding", _fake_generate_embedding)

    manager = EmbeddingsManager(mode="aihub", model="qwen3-emb")
    with pytest.raises(RuntimeError, match="expected=4096 actual=1024"):
        asyncio.run(manager.embedd_documents_async(["hello"]))

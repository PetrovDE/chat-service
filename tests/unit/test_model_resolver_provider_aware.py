import pytest

from app.services.llm.model_resolver import ProviderModelResolver


def test_aihub_embedding_default_is_qwen3_emb():
    resolver = ProviderModelResolver()
    decision = resolver.resolve_embedding("aihub", None)
    assert decision.resolved_model == "qwen3-emb"
    assert decision.provider == "aihub"


def test_chat_and_embedding_defaults_are_resolved_independently():
    resolver = ProviderModelResolver()
    chat = resolver.resolve_chat("local", None)
    emb = resolver.resolve_embedding("local", None)
    assert chat.capability == "chat"
    assert emb.capability == "embedding"
    assert chat.resolved_model != emb.resolved_model


def test_local_model_tag_not_misparsed_as_provider_prefix():
    resolver = ProviderModelResolver()
    decision = resolver.resolve_embedding("local", "nomic-embed-text:latest")
    assert decision.source == "override"
    assert decision.resolved_model == "nomic-embed-text:latest"


def test_cross_provider_override_is_rejected():
    resolver = ProviderModelResolver()
    with pytest.raises(ValueError, match="provider mismatch"):
        resolver.resolve_embedding("local", "aihub:qwen3-emb")

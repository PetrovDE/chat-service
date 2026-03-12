import asyncio

import pytest

from app.core.config import Settings
from app.services import file as file_service
from app.services.llm.exceptions import ProviderConfigError
from app.services.llm.manager import LLMManager
from app.services.llm.provider_clients import ProviderRegistry


class _EmbeddingSpyProvider:
    def __init__(self, *, result=None, error: Exception | None = None):
        self.result = result if result is not None else [0.1, 0.2, 0.3]
        self.error = error
        self.calls = []

    async def get_available_models(self):  # noqa: D401
        return []

    async def generate_embedding(self, text: str, model=None):
        self.calls.append({"text": text, "model": model})
        if self.error is not None:
            raise self.error
        return list(self.result)

    async def generate_response(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise NotImplementedError

    async def generate_response_stream(self, *args, **kwargs):  # noqa: ANN002, ANN003
        if False:
            yield ""


def test_settings_provider_aware_embedding_defaults():
    cfg = Settings(
        _env_file=None,
        DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/db",
        ALEMBIC_DATABASE_URL="postgresql://user:pass@localhost:5432/db",
        JWT_SECRET_KEY="secret",
    )
    assert cfg.AIHUB_EMBEDDING_MODEL == "qwen3-emb"
    assert cfg.EMBEDDINGS_MODEL == "nomic-embed-text:latest"
    assert cfg.OLLAMA_EMBED_MODEL == "nomic-embed-text:latest"


def test_explicit_override_to_arctic_is_supported_for_aihub():
    provider, model, source, reason = file_service._resolve_embedding_model("aihub", "arctic")
    assert provider == "aihub"
    assert model == "arctic"
    assert source == "override"
    assert reason in {"catalog_match", "inferred_ok"}


def test_local_embedding_route_does_not_call_aihub():
    mgr = LLMManager()
    ollama = _EmbeddingSpyProvider(result=[0.9, 0.8])
    aihub = _EmbeddingSpyProvider(result=[0.1, 0.2])
    openai = _EmbeddingSpyProvider(result=[0.3, 0.4])
    mgr.providers = {"ollama": ollama, "aihub": aihub, "openai": openai}
    mgr.provider_registry = ProviderRegistry(mgr.providers)

    out = asyncio.run(mgr.generate_embedding("hello", model_source="local", model_name="nomic-embed-text:latest"))
    assert out == [0.9, 0.8]
    assert len(ollama.calls) == 1
    assert len(aihub.calls) == 0


def test_aihub_embedding_route_has_no_silent_local_fallback():
    mgr = LLMManager()
    err = ProviderConfigError("model missing", provider="aihub", status_code=404)
    ollama = _EmbeddingSpyProvider(result=[0.9, 0.8])
    aihub = _EmbeddingSpyProvider(error=err)
    openai = _EmbeddingSpyProvider(result=[0.3, 0.4])
    mgr.providers = {"ollama": ollama, "aihub": aihub, "openai": openai}
    mgr.provider_registry = ProviderRegistry(mgr.providers)

    with pytest.raises(ProviderConfigError):
        asyncio.run(mgr.generate_embedding("hello", model_source="aihub", model_name="qwen3-emb"))
    assert len(aihub.calls) == 1
    assert len(ollama.calls) == 0


def test_local_invalid_chat_override_falls_back_to_local_embedding_default_not_qwen(monkeypatch):
    monkeypatch.setattr(file_service.settings, "OLLAMA_EMBED_MODEL", "nomic-embed-text:latest")
    monkeypatch.setattr(file_service.settings, "EMBEDDINGS_MODEL", "nomic-embed-text:latest")

    provider, model, source, reason = file_service._resolve_embedding_model("local", "llama3.2:latest")
    assert provider == "ollama"
    assert model == "nomic-embed-text:latest"
    assert source == "provider_default"
    assert reason.startswith("invalid_override:chat_model_for_embedding")


def test_embedding_preflight_fails_clearly_when_model_unavailable(monkeypatch):
    async def _raise_config_error(*, text, model_source, model_name):  # noqa: ANN001
        _ = (text, model_source, model_name)
        raise ProviderConfigError("not found", provider="ollama", status_code=404)

    monkeypatch.setattr(file_service.settings, "EMBEDDING_PREFLIGHT_VALIDATE", True)
    monkeypatch.setattr(file_service.llm_manager, "generate_embedding", _raise_config_error)

    with pytest.raises(ValueError, match="provider=ollama model=nomic-embed-text:latest"):
        asyncio.run(
            file_service._preflight_validate_embedding(
                embedding_mode="local",
                requested_embedding_model=None,
                embedding_model="nomic-embed-text:latest",
                resolution_source="provider_default",
                resolution_reason="no_override",
            )
        )


def test_runtime_resolution_uses_local_available_embedding_model(monkeypatch):
    monkeypatch.setattr(file_service.settings, "OLLAMA_EMBED_MODEL", "missing-local-embed")

    async def _models(source):  # noqa: ANN001
        assert source == "ollama"
        return ["llama3.2:latest", "nomic-embed-text:latest"]

    monkeypatch.setattr(file_service.llm_manager, "get_available_models", _models)

    provider, model, source, reason = asyncio.run(file_service._resolve_runtime_embedding_model("local", None))
    assert provider == "ollama"
    assert model == "nomic-embed-text:latest"
    assert source == "provider_capability"
    assert reason.startswith("default_unavailable:")

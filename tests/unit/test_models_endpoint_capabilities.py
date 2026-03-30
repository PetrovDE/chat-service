import asyncio

from app.api.v1.endpoints.models import list_models


def test_models_list_embedding_is_capability_aware_local(monkeypatch):
    async def _available(source, capability=None):  # noqa: ANN001
        assert source == "ollama"
        assert capability == "embedding"
        return ["llama3.2:latest", "nomic-embed-text:latest"]

    monkeypatch.setattr("app.api.v1.endpoints.models.llm_manager.get_available_models", _available)

    payload = asyncio.run(list_models(mode="local", capability="embedding"))
    names = [row["name"] for row in payload["models"]]

    assert payload["capability"] == "embedding"
    assert payload["default_model"] == "nomic-embed-text:latest"
    assert "nomic-embed-text:latest" in names
    assert "llama3.2:latest" not in names


def test_models_list_aihub_embedding_default_is_qwen(monkeypatch):
    async def _available_detailed(source, capability=None):  # noqa: ANN001
        assert source == "aihub"
        assert capability == "embedding"
        return [
            {"name": "arctic", "type": "embedding"},
            {"name": "qwen3-emb", "type": "embedding"},
            {"name": "nemo_12b", "type": "chatbot"},
            {"name": "yandex", "type": "chatbot"},
        ]

    monkeypatch.setattr("app.api.v1.endpoints.models.llm_manager.get_available_models_detailed", _available_detailed)

    payload = asyncio.run(list_models(mode="aihub", capability="embedding"))
    names = [row["name"] for row in payload["models"]]
    assert payload["capability"] == "embedding"
    assert payload["default_model"] == "qwen3-emb"
    assert "qwen3-emb" in names
    assert "arctic" in names
    assert "nemo_12b" not in names
    assert "yandex" not in names


def test_models_list_does_not_inject_unavailable_provider_default(monkeypatch):
    monkeypatch.setattr("app.services.llm.model_resolver.settings.OLLAMA_CHAT_MODEL", "missing-local-chat")

    async def _available(source, capability=None):  # noqa: ANN001
        assert source == "ollama"
        assert capability == "chat"
        return ["llama3.2:latest"]

    monkeypatch.setattr("app.api.v1.endpoints.models.llm_manager.get_available_models", _available)

    payload = asyncio.run(list_models(mode="local", capability="chat"))
    names = [row["name"] for row in payload["models"]]

    assert payload["default_model"] is None
    assert "missing-local-chat" not in names
    assert names == ["llama3.2:latest"]


def test_models_list_local_embedding_excludes_unknown_non_embedding_tags(monkeypatch):
    monkeypatch.setattr("app.api.v1.endpoints.models.settings.OLLAMA_EMBED_MODEL_CATALOG", "nomic-embed-text:latest")
    monkeypatch.setattr("app.services.llm.model_resolver.settings.OLLAMA_EMBED_MODEL_CATALOG", "nomic-embed-text:latest")

    async def _available(source, capability=None):  # noqa: ANN001
        assert source == "ollama"
        assert capability == "embedding"
        return [
            "llama3.2:latest",
            "custom-local:latest",
            "nomic-embed-text:latest",
            "qwen3-emb",
        ]

    monkeypatch.setattr("app.api.v1.endpoints.models.llm_manager.get_available_models", _available)

    payload = asyncio.run(list_models(mode="local", capability="embedding"))
    names = [row["name"] for row in payload["models"]]

    assert "nomic-embed-text:latest" in names
    assert "qwen3-emb" in names
    assert "llama3.2:latest" not in names
    assert "custom-local:latest" not in names


def test_models_list_aihub_chat_prefers_gpt_oss_when_available(monkeypatch):
    async def _available_detailed(source, capability=None):  # noqa: ANN001
        assert source == "aihub"
        assert capability == "chat"
        return [
            {"name": "vikhr", "type": "chatbot"},
            {"name": "gpt-oss", "type": "chatbot"},
            {"name": "qwen3-emb", "type": "embedding"},
        ]

    monkeypatch.setattr("app.api.v1.endpoints.models.llm_manager.get_available_models_detailed", _available_detailed)

    payload = asyncio.run(list_models(mode="aihub", capability="chat"))
    names = [row["name"] for row in payload["models"]]

    assert "gpt-oss" in names
    assert "vikhr" in names
    assert "qwen3-emb" not in names
    assert payload["default_model"] == "gpt-oss"


def test_models_list_aihub_only_one_preferred_available_uses_fallbacks(monkeypatch):
    async def _available_detailed(source, capability=None):  # noqa: ANN001
        assert source == "aihub"
        if capability == "chat":
            return [
                {"name": "vikhr", "type": "chatbot"},
                {"name": "nemo_12b", "type": "chatbot"},
                {"name": "qwen3-emb", "type": "embedding"},
            ]
        assert capability == "embedding"
        return [
            {"name": "qwen3-emb", "type": "embedding"},
            {"name": "arctic", "type": "embedding"},
            {"name": "vikhr", "type": "chatbot"},
        ]

    monkeypatch.setattr("app.api.v1.endpoints.models.llm_manager.get_available_models_detailed", _available_detailed)

    chat_payload = asyncio.run(list_models(mode="aihub", capability="chat"))
    emb_payload = asyncio.run(list_models(mode="aihub", capability="embedding"))

    assert chat_payload["default_model"] == "vikhr"
    assert emb_payload["default_model"] == "qwen3-emb"


def test_models_list_aihub_neither_preferred_nor_provider_default_available(monkeypatch):
    async def _available_detailed(source, capability=None):  # noqa: ANN001
        assert source == "aihub"
        if capability == "chat":
            return [
                {"name": "nemo_12b", "type": "chatbot"},
                {"name": "yandex", "type": "chatbot"},
            ]
        assert capability == "embedding"
        return [
            {"name": "arctic", "type": "embedding"},
            {"name": "bge", "type": "embedding"},
        ]

    monkeypatch.setattr("app.api.v1.endpoints.models.llm_manager.get_available_models_detailed", _available_detailed)

    chat_payload = asyncio.run(list_models(mode="aihub", capability="chat"))
    emb_payload = asyncio.run(list_models(mode="aihub", capability="embedding"))

    assert chat_payload["default_model"] is None
    assert emb_payload["default_model"] is None
    assert [row["name"] for row in chat_payload["models"]] == ["nemo_12b", "yandex"]
    assert [row["name"] for row in emb_payload["models"]] == ["arctic", "bge"]

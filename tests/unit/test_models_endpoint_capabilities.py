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

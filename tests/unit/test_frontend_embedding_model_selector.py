from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_index_contains_dedicated_embedding_selector():
    html = _read("frontend/index.html")
    assert 'id="embedding-model-selector"' in html


def test_settings_manager_requests_capability_specific_model_lists():
    src = _read("frontend/static/js/settings-manager.js")
    assert "/models/list?mode=${selectedMode}&capability=chat" in src
    assert "/models/list?mode=${selectedMode}&capability=embedding" in src


def test_file_flows_use_embedding_selector_not_chat_selector():
    files_manager_src = _read("frontend/static/js/files-sidebar-manager.js")
    assert "embedding-model-selector" in files_manager_src
    assert "getEmbeddingModel" in files_manager_src
    assert "uploadFile({" in files_manager_src
    assert "reprocessFile(fileId" in files_manager_src

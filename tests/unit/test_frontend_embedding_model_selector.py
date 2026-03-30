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


def test_settings_manager_does_not_reinsert_unavailable_default_model():
    src = _read("frontend/static/js/settings-manager.js")
    assert "options.unshift({ name: defaultModel, is_default: true });" not in src
    assert "availableDefaultModel = defaultModel && seen.has(defaultModel) ? defaultModel : null;" in src
    assert "replaced_with_provider_default" in src


def test_settings_manager_reports_model_replacement_after_provider_switch():
    src = _read("frontend/static/js/settings-manager.js")
    assert "updateModelSelectionHint(selectedMode, [chatSelection, embeddingSelection]);" in src
    assert 'is unavailable for provider' in src


def test_settings_manager_ignores_stale_async_model_responses():
    src = _read("frontend/static/js/settings-manager.js")
    assert "this.modelLoadRequestId = 0;" in src
    assert "const requestId = ++this.modelLoadRequestId;" in src
    assert "isLatestModelLoad(requestId, selectedMode)" in src
    assert "Ignoring stale model response" in src


def test_settings_manager_keeps_provider_scoped_model_state():
    src = _read("frontend/static/js/settings-manager.js")
    assert "providerScopedSelections" in src
    assert "setProviderScopedSelection('model'" in src
    assert "setProviderScopedSelection('embedding_model'" in src
    assert "No chat models are currently available for provider" in src


def test_file_flows_use_embedding_selector_not_chat_selector():
    files_manager_src = _read("frontend/static/js/files-sidebar-manager.js")
    assert "embedding-model-selector" in files_manager_src
    assert "getEmbeddingModel" in files_manager_src
    assert "uploadFile({" in files_manager_src
    assert "reprocessFile(fileId" in files_manager_src

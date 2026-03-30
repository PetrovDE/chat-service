from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_composer_keeps_provider_and_chat_model_visible():
    html = _read("frontend/index.html")
    assert 'id="mode-selector"' in html
    assert 'id="model-selector"' in html
    assert 'aria-label="Primary composer controls"' in html
    assert 'id="composerAttachBtn"' in html
    assert 'id="composerAdvancedToggle"' in html
    assert 'id="sendMessage"' in html


def test_embedding_and_rag_controls_are_moved_to_advanced_panel():
    html = _read("frontend/index.html")
    assert 'id="composerAdvancedPanel"' in html
    assert 'id="embedding-model-selector"' in html
    assert 'id="ragModeSelector"' in html
    advanced_index = html.index('id="composerAdvancedPanel"')
    embedding_index = html.index('id="embedding-model-selector"')
    rag_index = html.index('id="ragModeSelector"')
    assert embedding_index > advanced_index
    assert rag_index > advanced_index


def test_main_chat_flow_has_no_in_flow_files_block():
    html = _read("frontend/index.html")
    assert 'id="chatFilesPanel"' not in html
    assert 'id="chatFilesChips"' not in html
    assert 'id="openFilesDrawerBtn"' in html
    assert 'id="filesSidebar"' in html
    assert 'id="filesDrawerUploadBtn"' in html


def test_composer_file_menu_trigger_is_text_labeled():
    html = _read("frontend/index.html")
    assert 'id="openFilesDrawerBtn"' in html
    button_start = html.index('id="openFilesDrawerBtn"')
    button_end = html.index('</button>', button_start)
    button_html = html[button_start:button_end]
    assert "Files" in button_html
    assert "<svg" not in button_html
    assert 'id="openFilesDrawerBtn" class="file-drawer-btn icon-action-btn"' not in html


def test_provider_switch_listener_triggers_model_reload():
    src = _read("frontend/static/js/app.js")
    assert "this.settingsManager.setMode(newMode);" in src
    assert "await this.settingsManager.loadAvailableModels(newMode);" in src


def test_advanced_panel_has_outside_click_and_escape_close_handlers():
    src = _read("frontend/static/js/app.js")
    assert "closeAdvancedPanel" in src
    assert "document.addEventListener('click'" in src
    assert "event.key === 'Escape'" in src


def test_composer_advanced_controls_are_anchored_as_floating_panel():
    html = _read("frontend/index.html")
    css = _read("frontend/static/css/components/chat-layout-refactor.css")
    assert 'class="composer-advanced-anchor"' in html
    assert "composer-advanced-panel" in css
    assert "position: absolute;" in css
    assert "bottom: calc(100% + 8px);" in css
    assert ".composer-advanced-anchor {" in css
    assert "z-index: 40;" in css
    assert ".composer-advanced-panel {" in css
    assert "z-index: 80;" in css
    assert ".input-container {" in css
    assert "overflow: visible;" in css


def test_composer_keeps_message_row_primary_and_settings_row_secondary():
    html = _read("frontend/index.html")
    css = _read("frontend/static/css/components/chat-layout-refactor.css")
    assert ".input-container {" in css
    assert "grid-template-columns: minmax(0, 1fr);" in css
    assert "justify-items: stretch;" in css
    assert ".composer-main-row {" in css
    assert ".composer-main-row .message-input {" in css
    assert "width: 100%;" in css
    assert ".composer-footer-row {" in css
    assert "display: flex;" in css
    assert "justify-content: space-between;" in css
    assert ".composer-footer-left {" in css
    assert ".composer-footer-right {" in css
    assert ".composer-footer-left .control-with-hint {" in css
    assert "width: 190px;" in css
    footer_start = html.index('<div class="composer-footer-row"')
    footer_end = html.index('<small id="modelSelectionHint"', footer_start)
    footer_html = html[footer_start:footer_end]
    left_start = footer_html.index('<div class="composer-footer-left"')
    left_end = footer_html.index('<div class="composer-footer-right"', left_start)
    left_html = footer_html[left_start:left_end]
    right_html = footer_html[left_end:]
    left_order = [
        'id="composerAttachBtn"',
        'id="openFilesDrawerBtn"',
        'id="mode-selector"',
        'id="model-selector"',
    ]
    right_order = [
        'id="composerAdvancedToggle"',
        'id="sendMessage"',
    ]
    last = -1
    for token in left_order:
        idx = left_html.index(token)
        assert idx > last
        last = idx
    last = -1
    for token in right_order:
        idx = right_html.index(token)
        assert idx > last
        last = idx


def test_composer_hides_inline_helper_text_in_controls_row():
    css = _read("frontend/static/css/components/chat-layout-refactor.css")
    assert ".composer-footer-row .control-hint {" in css
    assert "display: none;" in css
    assert ".model-selection-hint {" in css
    assert "display: none;" in css


def test_files_manager_uses_overflow_actions_for_compact_file_cards():
    src = _read("frontend/static/js/files-sidebar-manager.js")
    assert "file-item-overflow" in src
    assert "<summary class=\"file-item-btn subtle\">More</summary>" in src
    assert "filesDrawerUploadBtn" in src


def test_assistant_message_style_is_lightweight_not_card_like():
    css = _read("frontend/static/css/components/chat-layout-refactor.css")
    assert ".message.assistant {" in css
    assert "gap: 2px;" in css
    assert ".message.assistant .message-bubble {" in css
    assert "background: transparent;" in css
    assert "border: none;" in css
    assert "padding: 1px 0;" in css
    assert ".message.assistant .message-bubble p {" in css
    assert "margin: 0.5em 0;" in css
    assert ".message.assistant .message-meta:empty {" in css
    assert ".message.assistant .message-time {" in css


def test_files_sidebar_layers_above_composer_for_interactivity():
    css = _read("frontend/static/css/components/chat-layout-refactor.css")
    assert ".input-area {" in css
    assert "z-index: 24;" in css
    assert ".files-sidebar {" in css
    assert "z-index: 36;" in css

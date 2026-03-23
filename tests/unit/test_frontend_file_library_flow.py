from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_index_has_file_library_and_chat_file_panel_controls():
    html = _read("frontend/index.html")
    assert 'id="filesSidebarQuota"' in html
    assert 'id="fileDetailsPanel"' in html
    assert 'id="chatFilesPanel"' in html
    assert 'id="uploadLibraryFileBtn"' in html
    assert 'id="chatFileSelect"' in html
    assert 'id="attachExistingFileBtn"' in html
    assert 'id="chatFilesList"' in html
    assert "Upload always stores file in your library." in html
    assert "Upload to library" in html


def test_api_service_supports_persistent_file_lifecycle_endpoints():
    src = _read("frontend/static/js/api-service.js")
    assert "/files/upload" in src
    assert "/files/processed" in src
    assert "/files/quota" in src
    assert "/files/${fileId}/status" in src
    assert "/files/${fileId}/attach" in src
    assert "/files/${fileId}/detach" in src
    assert "/files/${fileId}/reprocess" in src
    assert "/files/${fileId}/processing" in src
    assert "/files/${fileId}/processing/active" in src
    assert "/files/${fileId}/debug" in src


def test_chat_manager_uses_current_chat_links_not_legacy_attachment_state():
    src = _read("frontend/static/js/chat-manager.js")
    assert "getCurrentChatFileIds" in src
    assert "window.app?.fileManager" not in src


def test_files_manager_implements_upload_attach_detach_delete_reprocess():
    src = _read("frontend/static/js/files-sidebar-manager.js")
    assert "handleUpload(file)" in src
    assert "handleAttachToCurrentChat" in src
    assert "handleDetachFromCurrentChat" in src
    assert "handleDeleteFile" in src
    assert "handleReprocessFile" in src
    assert "uploaded and attached to this chat" in src
    assert "trackFileProgress(fileId)" in src


def test_files_manager_renders_status_and_debug_details():
    src = _read("frontend/static/js/files-sidebar-manager.js")
    assert "Tabular parsing ready for analytics." in src
    assert "getFileDebugInfo(fileId)" in src
    assert "debugRows = this.debugMode" in src
    assert "file_id" in src
    assert "processing_id" in src


def test_files_manager_maps_quota_and_access_errors():
    src = _read("frontend/static/js/files-sidebar-manager.js")
    assert "status === 401" in src
    assert "status === 403" in src
    assert "status === 413" in src
    assert "quota is exceeded" in src
    assert "Access denied" in src


def test_file_state_is_reloaded_after_file_mutations():
    src = _read("frontend/static/js/files-sidebar-manager.js")
    assert src.count("await this.loadFiles(true);") >= 4


def test_files_manager_blocks_attach_for_failed_files_and_uses_attachable_status_set():
    src = _read("frontend/static/js/files-sidebar-manager.js")
    assert "ATTACHABLE_STATUSES" in src
    assert "Only uploaded, processing, or ready files can be attached to a chat." in src


def test_files_manager_has_optimistic_attach_detach_updates_with_rollback():
    src = _read("frontend/static/js/files-sidebar-manager.js")
    assert "const previousChatIds = Array.isArray(target.chat_ids) ? [...target.chat_ids] : [];" in src
    assert "target.chat_ids = [...previousChatIds, this.currentConversationId];" in src
    assert "target.chat_ids = previousChatIds;" in src
    assert "this.setFileBusy(fileId, true);" in src
    assert "this.setFileBusy(fileId, false);" in src


def test_legacy_attachment_manager_removed_from_frontend_bundle():
    assert not (ROOT / "frontend/static/js/file-manager.js").exists()
    app_src = _read("frontend/static/js/app.js")
    assert "from './file-manager.js'" not in app_src


def test_unused_legacy_ui_modules_removed():
    assert not (ROOT / "frontend/static/js/auth-ui.js").exists()
    assert not (ROOT / "frontend/static/js/conversations-ui.js").exists()
    assert not (ROOT / "frontend/static/js/utils.js").exists()

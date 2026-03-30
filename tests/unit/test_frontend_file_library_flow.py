from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_index_keeps_file_library_drawer_without_in_flow_files_ui():
    html = _read("frontend/index.html")
    assert 'id="filesSidebarQuota"' in html
    assert 'id="fileDetailsPanel"' in html
    assert 'id="composerAttachBtn"' in html
    assert 'id="openFilesDrawerBtn"' in html
    assert 'id="filesDrawerUploadBtn"' in html
    assert 'id="chatFilesPanel"' not in html
    assert 'id="chatFilesChips"' not in html
    assert 'class="attach-btn icon-action-btn"' in html
    assert 'id="uploadLibraryFileBtn"' not in html
    assert 'id="chatFileSelect"' not in html
    assert 'id="attachExistingFileBtn"' not in html
    assert 'id="chatFilesList"' not in html


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
    assert "responseBody.error?.details" in src


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


def test_file_card_actions_keep_attach_and_delete_primary_with_more_overflow():
    src = _read("frontend/static/js/files-sidebar-manager.js")
    assert '<button class="file-item-btn is-primary" data-action="attach"' in src
    assert '<button class="file-item-btn delete" data-action="delete"' in src
    assert '<summary class="file-item-btn subtle">More</summary>' in src
    assert 'data-action="detach"' in src
    assert 'data-action="reprocess"' in src
    assert 'data-action="details"' in src


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


def test_file_upload_entrypoint_is_in_composer_and_drawer_controls():
    src = _read("frontend/static/js/files-sidebar-manager.js")
    assert "composerAttachBtn" in src
    assert "composerAttachBtn.addEventListener('click', openFilePicker);" in src
    assert "openFilesDrawerBtn" in src
    assert "openFilesDrawerBtn.addEventListener('click'" in src
    assert "window.toggleFilesSidebar?.()" in src
    assert "drawerUploadBtn" in src
    assert "filesDrawerUploadBtn" in src


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

import { formatRelativeTimestamp } from './time-format.js';

const REFRESH_INTERVAL_MS = 8000;
const UPLOAD_MAX_BYTES = 50 * 1024 * 1024;
const ALLOWED_EXTENSIONS = new Set(['pdf', 'docx', 'txt', 'md', 'csv', 'tsv', 'json', 'xlsx', 'xls']);
const TERMINAL_STATUSES = new Set(['ready', 'failed', 'deleted']);
const ACTIVE_STATUSES = new Set(['uploaded', 'processing', 'deleting']);
const TABULAR_EXTENSIONS = new Set(['csv', 'tsv', 'xlsx', 'xls']);
const ATTACHABLE_STATUSES = new Set(['uploaded', 'processing', 'ready']);

function normalizeStatus(status) {
    const raw = String(status || '').trim().toLowerCase();
    if (!raw) return 'uploaded';
    if (raw === 'completed' || raw === 'partial_success' || raw === 'partial_failed') return 'ready';
    return raw;
}

function extractFileId(file) {
    return String(file?.file_id || '');
}

function includesConversation(chatIds, conversationId) {
    if (!conversationId || !Array.isArray(chatIds)) return false;
    return chatIds.map(String).includes(String(conversationId));
}

export class FilesSidebarManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.files = [];
        this.fileStatusDetails = new Map();
        this.fileDebugDetails = new Map();
        this.quota = null;
        this.currentConversationId = null;
        this.selectedDetailsFileId = null;
        this.refreshInterval = null;
        this.loadVersion = 0;
        this.pendingActionFileIds = new Set();
        this.lastSilentErrorAt = 0;
        this.dom = {};
        this.listenersAttached = false;
        this.debugMode = this.resolveDebugMode();
    }

    resolveDebugMode() {
        try {
            const params = new URLSearchParams(window.location.search);
            return params.get('debug_ui') === '1' || localStorage.getItem('debug_ui') === '1';
        } catch (_) {
            return false;
        }
    }

    initialize(conversationId = null) {
        this.currentConversationId = conversationId;
        this.bindDom();
        this.attachEventListeners();
        this.loadFiles();

        this.refreshInterval = setInterval(() => {
            this.loadFiles(true);
        }, REFRESH_INTERVAL_MS);
    }

    bindDom() {
        this.dom = {
            fileInput: document.getElementById('fileInput'),
            attachBtn: document.querySelector('.attach-btn'),
            uploadBtn: document.getElementById('uploadLibraryFileBtn'),
            sidebarList: document.getElementById('filesSidebarList'),
            sidebarQuota: document.getElementById('filesSidebarQuota'),
            detailsPanel: document.getElementById('fileDetailsPanel'),
            chatFilesList: document.getElementById('chatFilesList'),
            chatAttachSelect: document.getElementById('chatFileSelect'),
            chatAttachBtn: document.getElementById('attachExistingFileBtn'),
            chatHint: document.getElementById('chatFilesHint'),
        };
    }

    attachEventListeners() {
        if (this.listenersAttached) return;
        this.listenersAttached = true;

        if (this.dom.fileInput) {
            this.dom.fileInput.addEventListener('change', (event) => this.handleFileInputChange(event));
        }

        const openFilePicker = () => this.dom.fileInput?.click();
        if (this.dom.attachBtn) this.dom.attachBtn.addEventListener('click', openFilePicker);
        if (this.dom.uploadBtn) this.dom.uploadBtn.addEventListener('click', openFilePicker);

        if (this.dom.sidebarList) {
            this.dom.sidebarList.addEventListener('click', (event) => this.handleActionClick(event));
        }

        if (this.dom.chatFilesList) {
            this.dom.chatFilesList.addEventListener('click', (event) => this.handleActionClick(event));
        }

        if (this.dom.chatAttachBtn) {
            this.dom.chatAttachBtn.addEventListener('click', async () => {
                const fileId = this.dom.chatAttachSelect?.value || '';
                if (!fileId) return;
                await this.handleAttachToCurrentChat(fileId);
            });
        }
    }

    destroy() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
            this.refreshInterval = null;
        }
    }

    setCurrentConversation(conversationId) {
        this.currentConversationId = conversationId || null;
        this.renderChatPanel();
        this.renderLibrary();
    }

    getCurrentChatFiles() {
        if (!this.currentConversationId) return [];
        return this.files.filter((file) => includesConversation(file.chat_ids, this.currentConversationId));
    }

    getCurrentChatFileIds() {
        return this.getCurrentChatFiles().map((file) => extractFileId(file)).filter(Boolean);
    }

    getEmbeddingProvider() {
        return document.getElementById('mode-selector')?.value || 'local';
    }

    getEmbeddingModel() {
        return document.getElementById('embedding-model-selector')?.value || null;
    }

    isFileBusy(fileId) {
        return this.pendingActionFileIds.has(String(fileId));
    }

    setFileBusy(fileId, busy) {
        const key = String(fileId || '');
        if (!key) return;
        if (busy) {
            this.pendingActionFileIds.add(key);
        } else {
            this.pendingActionFileIds.delete(key);
        }
    }

    pruneTransientState() {
        const liveIds = new Set(this.files.map((file) => extractFileId(file)));
        this.fileStatusDetails.forEach((_, fileId) => {
            if (!liveIds.has(String(fileId))) this.fileStatusDetails.delete(String(fileId));
        });
        this.fileDebugDetails.forEach((_, fileId) => {
            if (!liveIds.has(String(fileId))) this.fileDebugDetails.delete(String(fileId));
        });
        this.pendingActionFileIds.forEach((fileId) => {
            if (!liveIds.has(String(fileId))) this.pendingActionFileIds.delete(String(fileId));
        });
        if (this.selectedDetailsFileId && !liveIds.has(String(this.selectedDetailsFileId))) {
            this.selectedDetailsFileId = null;
        }
    }

    async loadFiles(silent = false) {
        const version = ++this.loadVersion;

        try {
            if (!silent) this.showLoadingState();

            const [filesResponse, quotaResponse] = await Promise.all([
                this.apiService.getFiles(),
                this.apiService.getFileQuota().catch(() => null),
            ]);
            if (version !== this.loadVersion) return;

            this.files = (Array.isArray(filesResponse) ? filesResponse : [])
                .filter((item) => normalizeStatus(item?.status) !== 'deleted')
                .sort((a, b) => new Date(b.created_at || 0).getTime() - new Date(a.created_at || 0).getTime());
            this.quota = quotaResponse;
            this.pruneTransientState();

            await this.refreshStatuses(version);
            if (version !== this.loadVersion) return;

            this.render();
        } catch (error) {
            if (version !== this.loadVersion) return;
            const message = this.humanizeError(error, 'Failed to load file library.');
            if (silent) {
                const now = Date.now();
                if ((now - this.lastSilentErrorAt) > 30000) {
                    this.lastSilentErrorAt = now;
                    this.showError(message);
                }
                return;
            }
            this.showErrorState(message);
        }
    }

    async refreshStatuses(version) {
        const targets = this.files.filter((file) => {
            const status = normalizeStatus(file?.status);
            return ACTIVE_STATUSES.has(status) || status === 'failed';
        });
        if (targets.length === 0) return;

        await Promise.all(
            targets.map(async (file) => {
                const fileId = extractFileId(file);
                if (!fileId) return;
                try {
                    const detail = await this.apiService.getFileStatus(fileId);
                    if (version !== this.loadVersion) return;
                    this.fileStatusDetails.set(fileId, detail);
                } catch (_) {
                    // no-op: keep last known detail
                }
            })
        );
    }

    render() {
        this.renderQuota();
        this.renderLibrary();
        this.renderChatPanel();
        this.renderDetailsPanel();
    }

    renderQuota() {
        if (!this.dom.sidebarQuota) return;
        if (!this.quota) {
            this.dom.sidebarQuota.textContent = 'Library quota: unavailable';
            return;
        }

        const used = Number(this.quota.quota_used_bytes || 0);
        const limit = Number(this.quota.quota_limit_bytes || 0);
        this.dom.sidebarQuota.textContent = `Library quota: ${this.formatFileSize(used)} / ${this.formatFileSize(limit)}`;
    }

    renderLibrary() {
        const container = this.dom.sidebarList;
        if (!container) return;

        if (this.files.length === 0) {
            container.innerHTML = `
                <div class="files-empty">
                    <div class="files-empty-icon">No files</div>
                    <p>Your file library is empty.</p>
                    <p class="files-empty-hint">Upload once, then attach to any chat.</p>
                </div>
            `;
            return;
        }

        container.innerHTML = this.files
            .map((file) => this.renderFileCard(file, { context: 'library' }))
            .join('');
    }

    renderChatPanel() {
        const hint = this.dom.chatHint;
        const list = this.dom.chatFilesList;
        const select = this.dom.chatAttachSelect;
        const attachBtn = this.dom.chatAttachBtn;
        if (!hint || !list || !select || !attachBtn) return;

        if (!this.currentConversationId) {
            hint.textContent = 'Open a chat to attach existing files. Upload still adds files to your library.';
            list.innerHTML = '<div class="chat-files-empty">No chat selected yet.</div>';
            select.innerHTML = '<option value="">No chat selected</option>';
            select.disabled = true;
            attachBtn.disabled = true;
            return;
        }

        const attached = this.getCurrentChatFiles();
        const attachedReady = attached.filter((file) => normalizeStatus(this.getEffectiveStatus(file)) === 'ready').length;
        const attachedNotReady = attached.length - attachedReady;
        const available = this.files.filter(
            (file) => !includesConversation(file.chat_ids, this.currentConversationId)
                && ATTACHABLE_STATUSES.has(this.getEffectiveStatus(file))
        );

        hint.textContent = attached.length > 0
            ? `${attached.length} file(s) connected: ${attachedReady} ready, ${attachedNotReady} processing.`
            : 'No files connected yet. Attach from library or upload a new file.';

        if (attached.length === 0) {
            list.innerHTML = '<div class="chat-files-empty">No files connected to this chat.</div>';
        } else {
            list.innerHTML = attached
                .map((file) => this.renderFileCard(file, { context: 'chat' }))
                .join('');
        }

        if (available.length === 0) {
            select.innerHTML = '<option value="">No attachable files</option>';
            select.disabled = true;
            attachBtn.disabled = true;
            return;
        }

        select.disabled = false;
        attachBtn.disabled = false;
        select.innerHTML = available.map((file) => {
            const fileId = extractFileId(file);
            return `<option value="${fileId}">${this.escapeHtml(file.original_filename)} (${this.humanStatusLabel(this.getEffectiveStatus(file))})</option>`;
        }).join('');
    }

    renderDetailsPanel() {
        const container = this.dom.detailsPanel;
        if (!container) return;

        if (!this.selectedDetailsFileId) {
            container.innerHTML = '<p class="file-details-empty">Select "Details" on a file to inspect lifecycle and processing state.</p>';
            return;
        }

        const details = this.fileDebugDetails.get(this.selectedDetailsFileId);
        if (!details) {
            container.innerHTML = '<p class="file-details-empty">Loading details...</p>';
            return;
        }

        const file = details.file || {};
        const active = details.active_processing || {};
        const versions = Array.isArray(details.processing_versions) ? details.processing_versions : [];
        const progress = active.ingestion_progress || {};
        const errorMessage = this.sanitizeError(active.error_message || file.error_message || '');
        const connectedChats = Array.isArray(file.chat_ids) ? file.chat_ids.length : 0;
        const uploadedLabel = formatRelativeTimestamp(file.created_at);
        const sizeLabel = this.formatFileSize(file.size_bytes);
        const statusValue = this.getEffectiveStatus(file);
        const debugRows = this.debugMode
            ? `
                <div class="file-details-row"><span>File ID</span><code>${this.escapeHtml(String(file.file_id || this.selectedDetailsFileId))}</code></div>
                <div class="file-details-row"><span>Active processing</span><code>${this.escapeHtml(String(active.processing_id || file.active_processing_id || 'n/a'))}</code></div>
            `
            : '';

        container.innerHTML = `
            <div class="file-details-content">
                <div class="file-details-row"><span>File</span><strong>${this.escapeHtml(file.original_filename || 'Unknown')}</strong></div>
                <div class="file-details-row"><span>Status</span><strong>${this.escapeHtml(this.humanStatusLabel(statusValue || 'uploaded'))}</strong></div>
                <div class="file-details-row"><span>Uploaded</span><strong>${this.escapeHtml(uploadedLabel || 'unknown')}</strong></div>
                <div class="file-details-row"><span>Size</span><strong>${this.escapeHtml(sizeLabel)}</strong></div>
                <div class="file-details-row"><span>Chats linked</span><strong>${connectedChats}</strong></div>
                <div class="file-details-row"><span>Versions</span><strong>${versions.length}</strong></div>
                <div class="file-details-row"><span>Progress</span><strong>${this.escapeHtml(this.renderProgressText(progress))}</strong></div>
                ${debugRows}
                ${errorMessage ? `<div class="file-details-error">${this.escapeHtml(errorMessage)}</div>` : ''}
            </div>
        `;
    }

    renderFileCard(file, { context }) {
        const fileId = extractFileId(file);
        const status = this.getEffectiveStatus(file);
        const statusClass = this.statusCssClass(status);
        const statusLabel = this.humanStatusLabel(status);
        const details = this.fileStatusDetails.get(fileId);
        const uploadDate = formatRelativeTimestamp(file.created_at);
        const isAttached = includesConversation(file.chat_ids, this.currentConversationId);
        const isBusy = this.isFileBusy(fileId);
        const progressText = this.fileStatusText(file, details);
        const safeName = this.escapeHtml(file.original_filename || 'Unnamed file');
        const actions = this.renderActions(file, { context, isAttached, isBusy });
        const libraryBadge = context === 'library' && isAttached
            ? '<span class="file-link-badge">In current chat</span>'
            : '';
        const debugMeta = this.debugMode
            ? `<div class="file-debug-meta">file_id: ${this.escapeHtml(fileId)}${file.active_processing_id ? ` | processing_id: ${this.escapeHtml(String(file.active_processing_id))}` : ''}</div>`
            : '';

        return `
            <article class="file-item ${context === 'chat' ? 'is-chat-item' : ''} ${isBusy ? 'is-busy' : ''}" data-file-id="${fileId}">
                <div class="file-item-header">
                    <div class="file-item-icon">${this.fileTypeLabel(file.extension)}</div>
                    <div class="file-item-info">
                        <h4 class="file-item-name" title="${safeName}">${safeName}</h4>
                        <div class="file-item-meta">
                            <span>${this.formatFileSize(file.size_bytes)}</span>
                            <span>${uploadDate}</span>
                            ${context === 'library' ? `<span>Chats: ${Array.isArray(file.chat_ids) ? file.chat_ids.length : 0}</span>` : ''}
                        </div>
                        <span class="file-item-status ${statusClass}">${statusLabel}</span>
                        ${libraryBadge}
                        ${progressText ? `<div class="file-item-progress">${this.escapeHtml(progressText)}</div>` : ''}
                        ${debugMeta}
                    </div>
                </div>
                <div class="file-item-actions">${actions}</div>
            </article>
        `;
    }

    renderActions(file, { context, isAttached, isBusy }) {
        const fileId = extractFileId(file);
        const status = normalizeStatus(this.getEffectiveStatus(file));
        const canAttach = Boolean(
            this.currentConversationId
            && !isAttached
            && ATTACHABLE_STATUSES.has(status)
            && !isBusy
        );
        const canDetach = Boolean(this.currentConversationId && isAttached && !isBusy);
        const canDelete = status !== 'deleting' && status !== 'deleted';
        const canReprocess = status !== 'deleting' && status !== 'deleted';
        const attachLabel = canAttach ? 'Attach' : '';
        const detachLabel = canDetach ? 'Detach' : '';
        const attachDisabled = !canAttach ? 'disabled aria-disabled="true"' : '';
        const detachDisabled = !canDetach ? 'disabled aria-disabled="true"' : '';
        const reprocessDisabled = (!canReprocess || isBusy) ? 'disabled aria-disabled="true"' : '';
        const deleteDisabled = (!canDelete || isBusy) ? 'disabled aria-disabled="true"' : '';
        const detailsDisabled = isBusy ? 'disabled aria-disabled="true"' : '';

        return `
            ${attachLabel ? `<button class="file-item-btn" data-action="attach" data-file-id="${fileId}" type="button" ${attachDisabled}>Attach</button>` : ''}
            ${detachLabel ? `<button class="file-item-btn" data-action="detach" data-file-id="${fileId}" type="button" ${detachDisabled}>Detach</button>` : ''}
            ${canReprocess ? `<button class="file-item-btn" data-action="reprocess" data-file-id="${fileId}" type="button" ${reprocessDisabled}>Reprocess</button>` : ''}
            ${canDelete ? `<button class="file-item-btn delete" data-action="delete" data-file-id="${fileId}" type="button" ${deleteDisabled}>Delete</button>` : ''}
            <button class="file-item-btn subtle" data-action="details" data-file-id="${fileId}" type="button" ${detailsDisabled}>Details</button>
            ${context === 'chat' ? '<span class="file-item-context">Linked</span>' : ''}
            ${isBusy ? '<span class="file-item-context">Updating...</span>' : ''}
        `;
    }

    getEffectiveStatus(file) {
        const fileId = extractFileId(file);
        const details = this.fileStatusDetails.get(fileId);
        if (details?.status) return normalizeStatus(details.status);
        if (file?.active_processing_status && normalizeStatus(file.active_processing_status) === 'failed') {
            return 'failed';
        }
        return normalizeStatus(file?.status);
    }

    statusCssClass(status) {
        switch (normalizeStatus(status)) {
            case 'ready':
                return 'completed';
            case 'processing':
            case 'deleting':
                return 'processing';
            case 'failed':
                return 'failed';
            case 'uploaded':
                return 'pending';
            default:
                return 'pending';
        }
    }

    humanStatusLabel(status) {
        switch (normalizeStatus(status)) {
            case 'uploaded':
                return 'Uploaded';
            case 'processing':
                return 'Processing';
            case 'ready':
                return 'Ready';
            case 'failed':
                return 'Failed';
            case 'deleting':
                return 'Deleting';
            case 'deleted':
                return 'Deleted';
            default:
                return 'Pending';
        }
    }

    fileStatusText(file, details) {
        const status = normalizeStatus(details?.status || file?.status);
        const progress = details ? this.renderProgressText(details) : '';
        const errorMessage = this.sanitizeError(details?.error_message);

        if (errorMessage) {
            return `Error: ${errorMessage}`;
        }
        if (progress) {
            return progress;
        }
        if (status === 'failed') {
            return 'Processing failed. Use Reprocess to make this file available in chat.';
        }
        if (status === 'ready' && TABULAR_EXTENSIONS.has(String(file.extension || '').toLowerCase())) {
            return 'Tabular parsing ready for analytics.';
        }
        return '';
    }

    renderProgressText(statusPayload) {
        if (!statusPayload || typeof statusPayload !== 'object') return '';
        const stage = String(statusPayload.stage || statusPayload.status || '').trim();
        const expected = Number(statusPayload.total_chunks_expected || 0);
        const processed = Number(statusPayload.chunks_processed || 0);
        const indexed = Number(statusPayload.chunks_indexed || 0);
        const failed = Number(statusPayload.chunks_failed || 0);

        if (expected > 0) {
            return `${stage || 'processing'} ${processed}/${expected} (ok=${indexed}, failed=${failed})`;
        }
        return stage;
    }

    async handleActionClick(event) {
        const button = event.target.closest('[data-action]');
        if (!button) return;

        event.preventDefault();
        event.stopPropagation();

        const action = button.dataset.action;
        const fileId = button.dataset.fileId;
        if (!fileId) return;

        if (action === 'attach') {
            await this.handleAttachToCurrentChat(fileId);
            return;
        }
        if (action === 'detach') {
            await this.handleDetachFromCurrentChat(fileId);
            return;
        }
        if (action === 'reprocess') {
            await this.handleReprocessFile(fileId);
            return;
        }
        if (action === 'delete') {
            await this.handleDeleteFile(fileId);
            return;
        }
        if (action === 'details') {
            await this.handleDetails(fileId);
        }
    }

    async handleFileInputChange(event) {
        const file = event.target?.files?.[0];
        if (!file) return;

        try {
            this.validateUploadFile(file);
            await this.handleUpload(file);
        } catch (error) {
            this.showError(this.humanizeError(error, 'File upload failed.'));
        } finally {
            event.target.value = '';
        }
    }

    validateUploadFile(file) {
        const ext = String(file.name.split('.').pop() || '').toLowerCase();
        if (!ALLOWED_EXTENSIONS.has(ext)) {
            throw new Error(`Unsupported file type .${ext}. Allowed: ${Array.from(ALLOWED_EXTENSIONS).join(', ')}`);
        }
        if (Number(file.size || 0) > UPLOAD_MAX_BYTES) {
            throw new Error('File too large. Maximum size is 50 MB.');
        }
    }

    async handleUpload(file) {
        const currentConversation = this.currentConversationId;
        this.uiController.showLoading('Uploading file to your library...');

        try {
            const result = await this.apiService.uploadFile({
                file,
                chatId: currentConversation || null,
                embeddingProvider: this.getEmbeddingProvider(),
                embeddingModel: this.getEmbeddingModel(),
                autoProcess: true,
            });

            const uploadedFileId = extractFileId(result?.file);
            const attachedToCurrentChat = includesConversation(result?.file?.chat_ids, currentConversation);

            await this.loadFiles(true);
            if (uploadedFileId) {
                await this.trackFileProgress(uploadedFileId);
            }

            if (attachedToCurrentChat) {
                this.uiController.showSuccess(`"${file.name}" uploaded and attached to this chat.`);
            } else {
                this.uiController.showSuccess(`"${file.name}" uploaded to your file library.`);
            }
        } finally {
            this.uiController.hideLoading();
        }
    }

    async trackFileProgress(fileId) {
        const maxAttempts = 60;
        for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
            const stillExists = this.files.some((file) => extractFileId(file) === String(fileId));
            if (!stillExists) return;
            try {
                const details = await this.apiService.getFileStatus(fileId);
                this.fileStatusDetails.set(String(fileId), details);
                this.renderLibrary();
                this.renderChatPanel();

                const status = normalizeStatus(details?.status);
                if (TERMINAL_STATUSES.has(status)) {
                    await this.loadFiles(true);
                    return;
                }
            } catch (_) {
                // no-op
            }
            await new Promise((resolve) => setTimeout(resolve, 1500));
        }
    }

    async handleAttachToCurrentChat(fileId) {
        if (!this.currentConversationId) {
            this.showError('Select or create a chat first, then attach files.');
            return;
        }

        const target = this.files.find((item) => extractFileId(item) === String(fileId));
        if (!target) {
            this.showError('File not found.');
            return;
        }

        const status = normalizeStatus(this.getEffectiveStatus(target));
        if (!ATTACHABLE_STATUSES.has(status)) {
            this.showError('Only uploaded, processing, or ready files can be attached to a chat.');
            return;
        }
        if (this.isFileBusy(fileId)) {
            return;
        }

        const previousChatIds = Array.isArray(target.chat_ids) ? [...target.chat_ids] : [];
        if (includesConversation(previousChatIds, this.currentConversationId)) {
            return;
        }

        target.chat_ids = [...previousChatIds, this.currentConversationId];
        this.setFileBusy(fileId, true);
        this.render();

        try {
            await this.apiService.attachFileToChat(fileId, this.currentConversationId);
            this.uiController.showSuccess('File attached to this chat.');
            await this.loadFiles(true);
        } catch (error) {
            target.chat_ids = previousChatIds;
            this.render();
            this.showError(this.humanizeError(error, 'Attach failed.'));
        } finally {
            this.setFileBusy(fileId, false);
            this.render();
        }
    }

    async handleDetachFromCurrentChat(fileId) {
        if (!this.currentConversationId) {
            this.showError('No active chat for detach action.');
            return;
        }

        const target = this.files.find((item) => extractFileId(item) === String(fileId));
        if (!target) {
            this.showError('File not found.');
            return;
        }
        if (this.isFileBusy(fileId)) {
            return;
        }

        const previousChatIds = Array.isArray(target.chat_ids) ? [...target.chat_ids] : [];
        target.chat_ids = previousChatIds.filter((chatId) => String(chatId) !== String(this.currentConversationId));
        this.setFileBusy(fileId, true);
        this.render();

        try {
            await this.apiService.detachFileFromChat(fileId, this.currentConversationId);
            this.uiController.showSuccess('File detached from this chat.');
            await this.loadFiles(true);
        } catch (error) {
            target.chat_ids = previousChatIds;
            this.render();
            this.showError(this.humanizeError(error, 'Detach failed.'));
        } finally {
            this.setFileBusy(fileId, false);
            this.render();
        }
    }

    async handleReprocessFile(fileId) {
        const confirmed = confirm('Start reprocessing for this file?');
        if (!confirmed) return;

        if (this.isFileBusy(fileId)) {
            return;
        }

        const target = this.files.find((item) => extractFileId(item) === String(fileId));
        const previousStatus = target?.status;
        this.setFileBusy(fileId, true);
        if (target) target.status = 'processing';
        this.render();

        try {
            this.uiController.showLoading('Scheduling reprocessing...');
            await this.apiService.reprocessFile(fileId, {
                embedding_provider: this.getEmbeddingProvider(),
                embedding_model: this.getEmbeddingModel(),
            });
            this.uiController.showSuccess('Reprocessing started.');
            this.setFileBusy(fileId, false);
            this.render();
            await this.loadFiles(true);
            await this.trackFileProgress(fileId);
        } catch (error) {
            if (target && previousStatus) target.status = previousStatus;
            this.setFileBusy(fileId, false);
            this.render();
            this.showError(this.humanizeError(error, 'Reprocess failed.'));
        } finally {
            this.uiController.hideLoading();
            this.setFileBusy(fileId, false);
            this.render();
        }
    }

    async handleDeleteFile(fileId) {
        const file = this.files.find((item) => extractFileId(item) === String(fileId));
        if (!file) {
            this.showError('File not found.');
            return;
        }

        const confirmed = confirm(`Delete "${file.original_filename}" from your library?`);
        if (!confirmed) return;
        if (this.isFileBusy(fileId)) return;

        const snapshot = [...this.files];
        this.setFileBusy(fileId, true);
        this.files = this.files.map((item) => (
            extractFileId(item) === String(fileId)
                ? { ...item, status: 'deleting' }
                : item
        ));
        this.render();

        try {
            const response = await this.apiService.deleteFile(fileId);
            this.files = this.files.filter((item) => extractFileId(item) !== String(fileId));
            this.fileStatusDetails.delete(String(fileId));
            this.fileDebugDetails.delete(String(fileId));
            if (response?.quota) {
                this.quota = response.quota;
            }
            if (this.selectedDetailsFileId === String(fileId)) {
                this.selectedDetailsFileId = null;
            }
            this.uiController.showSuccess('File deleted.');
            this.render();
            await this.loadFiles(true);
        } catch (error) {
            this.files = snapshot;
            this.render();
            this.showError(this.humanizeError(error, 'Delete failed.'));
        } finally {
            this.setFileBusy(fileId, false);
            this.render();
        }
    }

    async handleDetails(fileId) {
        this.selectedDetailsFileId = String(fileId);
        this.renderDetailsPanel();

        try {
            const details = await this.apiService.getFileDebugInfo(fileId);
            this.fileDebugDetails.set(String(fileId), details);
            this.renderDetailsPanel();
        } catch (error) {
            this.showError(this.humanizeError(error, 'Failed to load file details.'));
            this.selectedDetailsFileId = null;
            this.renderDetailsPanel();
        }
    }

    showLoadingState() {
        if (!this.dom.sidebarList) return;
        this.dom.sidebarList.innerHTML = `
            <div class="files-loading">
                <p>Loading file library...</p>
            </div>
        `;
    }

    showErrorState(message) {
        if (!this.dom.sidebarList) return;
        this.dom.sidebarList.innerHTML = `
            <div class="files-empty">
                <div class="files-empty-icon">!</div>
                <p>${this.escapeHtml(message)}</p>
            </div>
        `;
    }

    showError(message) {
        this.uiController.showError(message);
    }

    humanizeError(error, fallback) {
        const status = Number(error?.status || 0);
        const rawMessage = this.sanitizeError(error?.message);

        if (status === 401) {
            return 'Login expired. Please sign in again.';
        }
        if (status === 403) {
            return 'Access denied for this file or chat.';
        }
        if (status === 404) {
            return 'File or chat was not found. Try refreshing the page.';
        }
        if (status === 409) {
            return rawMessage || 'Operation conflict. The file may be missing or already changing state.';
        }
        if (status === 413) {
            if ((rawMessage || '').toLowerCase().includes('quota')) {
                return 'Upload blocked: your file library quota is exceeded.';
            }
            return 'Upload blocked: file size is above allowed limit.';
        }
        if (status === 422) {
            return rawMessage || 'Upload/reprocess parameters are invalid for this model setup.';
        }
        return rawMessage || fallback;
    }

    sanitizeError(text) {
        if (!text) return '';
        return String(text).replace(/\s+/g, ' ').trim().slice(0, 240);
    }

    fileTypeLabel(extension) {
        const ext = String(extension || '').toLowerCase();
        if (!ext) return 'FILE';
        return ext.length > 5 ? ext.slice(0, 5).toUpperCase() : ext.toUpperCase();
    }

    formatFileSize(bytes) {
        const value = Number(bytes || 0);
        if (value <= 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB'];
        const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
        const normalized = value / 1024 ** index;
        return `${normalized.toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
    }

    escapeHtml(value) {
        const div = document.createElement('div');
        div.textContent = String(value || '');
        return div.innerHTML;
    }
}

window.toggleFilesSidebar = function() {
    const sidebar = document.getElementById('filesSidebar');
    if (sidebar) {
        sidebar.classList.toggle('active');
    }
};

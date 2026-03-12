import { formatRelativeTimestamp } from './time-format.js';

export class FilesSidebarManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.files = [];
        this.refreshInterval = null;
        this.currentConversationId = null;
    }

    initialize(conversationId = null) {
        this.currentConversationId = conversationId;
        this.attachFileEventListeners();
        this.loadFiles();

        this.refreshInterval = setInterval(() => {
            this.loadFiles(true);
        }, 10000);
    }

    attachFileEventListeners() {
        const container = document.getElementById('filesSidebarList');
        if (!container) return;

        container.addEventListener('click', async (event) => {
            const deleteBtn = event.target.closest('[data-action="delete"]');
            if (deleteBtn) {
                event.preventDefault();
                event.stopPropagation();

                const fileId = deleteBtn.dataset.fileId;
                if (!fileId) return;
                await this.handleDeleteFile(fileId);
                return;
            }

            const retryBtn = event.target.closest('[data-action="retry"]');
            if (retryBtn) {
                event.preventDefault();
                event.stopPropagation();

                const fileId = retryBtn.dataset.fileId;
                if (!fileId) return;
                await this.handleRetryFile(fileId);
            }
        });
    }

    async loadFiles(silent = false) {
        try {
            if (!silent) this.showLoading();
            const response = await this.apiService.getFiles();
            const allFiles = Array.isArray(response) ? response : [];
            if (this.currentConversationId) {
                this.files = allFiles.filter((file) =>
                    Array.isArray(file.conversation_ids) &&
                    file.conversation_ids.map(String).includes(String(this.currentConversationId))
                );
            } else {
                this.files = allFiles;
            }
            this.render();
        } catch (error) {
            if (!silent) this.showError(error.message || 'Failed to load files');
        }
    }

    showLoading() {
        const container = document.getElementById('filesSidebarList');
        if (!container) return;

        container.innerHTML = `
            <div class="files-loading">
                <p>Loading files...</p>
            </div>
        `;
    }

    showError(message) {
        const container = document.getElementById('filesSidebarList');
        if (!container) return;

        container.innerHTML = `
            <div class="files-empty">
                <div class="files-empty-icon">!</div>
                <p>${this.escapeHtml(message)}</p>
            </div>
        `;
    }

    render() {
        const container = document.getElementById('filesSidebarList');
        if (!container) return;

        if (this.files.length === 0) {
            container.innerHTML = `
                <div class="files-empty">
                    <div class="files-empty-icon">No files</div>
                    <p>There are no uploaded files</p>
                    <p style="font-size: 0.8rem; margin-top: 0.5rem; color: #6b7280;">Upload documents for RAG context.</p>
                </div>
            `;
            return;
        }

        container.innerHTML = this.files.map((file) => this.renderFileItem(file)).join('');
    }

    renderFileItem(file) {
        const icon = this.getFileIcon(file.file_type);
        const statusBadge = this.getStatusBadge(file.is_processed);
        const fileSize = this.formatFileSize(file.file_size);
        const uploadDate = formatRelativeTimestamp(file.uploaded_at);
        const conversationInfo = this.renderConversationInfo(file.conversation_ids);

        return `
            <div class="file-item" data-file-id="${file.id}">
                <div class="file-item-header">
                    <div class="file-item-icon">${icon}</div>
                    <div class="file-item-info">
                        <h4 class="file-item-name" title="${this.escapeHtml(file.original_filename)}">${this.escapeHtml(file.original_filename)}</h4>
                        <div class="file-item-meta">
                            <span>Size: ${fileSize}</span>
                            <span>Date: ${uploadDate}</span>
                        </div>
                        ${statusBadge}
                        ${file.chunks_count > 0 ? `<div class="file-item-chunks">Chunks: ${file.chunks_count}</div>` : ''}
                        ${conversationInfo}
                    </div>
                </div>
                <div class="file-item-actions">
                    ${this.renderRetryAction(file)}
                    <button class="file-item-btn delete" data-action="delete" data-file-id="${file.id}" type="button">Delete</button>
                </div>
            </div>
        `;
    }

    renderRetryAction(file) {
        const status = String(file?.is_processed || '');
        if (status !== 'failed' && status !== 'partial_success' && status !== 'partial_failed') {
            return '';
        }
        return `<button class="file-item-btn" data-action="retry" data-file-id="${file.id}" type="button">Retry</button>`;
    }

    renderConversationInfo(conversationIds) {
        if (!Array.isArray(conversationIds) || conversationIds.length === 0) {
            return '<div class="file-item-chats">Chats: not used</div>';
        }

        return `<div class="file-item-chats">Chats: ${conversationIds.length}</div>`;
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text || '';
        return div.innerHTML;
    }

    getFileIcon(fileType) {
        const icons = {
            pdf: 'PDF',
            docx: 'DOCX',
            doc: 'DOC',
            txt: 'TXT',
            md: 'MD',
            csv: 'CSV',
            tsv: 'TSV',
            xlsx: 'XLSX',
            xls: 'XLS',
            json: 'JSON',
        };

        return icons[(fileType || '').toLowerCase()] || 'FILE';
    }

    getStatusBadge(status) {
        if (status === true || status === 'completed') {
            return '<span class="file-item-status completed">Completed</span>';
        }
        if (status === 'processing') {
            return '<span class="file-item-status processing">Processing</span>';
        }
        if (status === 'queued' || status === 'parsing' || status === 'parsed' || status === 'chunking' || status === 'embedding' || status === 'indexing') {
            return '<span class="file-item-status processing">Processing</span>';
        }
        if (status === 'failed') {
            return '<span class="file-item-status failed">Failed</span>';
        }
        if (status === 'partial_success' || status === 'partial_failed') {
            return '<span class="file-item-status partial">Partial</span>';
        }
        if (status === 'uploaded') {
            return '<span class="file-item-status pending">Uploaded</span>';
        }
        return '<span class="file-item-status pending">Pending</span>';
    }

    formatFileSize(bytes) {
        if (!bytes) return '0 B';
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${sizes[i]}`;
    }

    async handleDeleteFile(fileId) {
        const file = this.files.find((item) => String(item.id) === String(fileId));
        if (!file) {
            this.uiController.showToast('File not found', 'error');
            return;
        }

        const confirmed = confirm(`Delete file "${file.original_filename}"? This action cannot be undone.`);
        if (!confirmed) return;

        try {
            this.uiController.showLoading('Deleting file...');
            await this.apiService.deleteFile(fileId);
            this.uiController.hideLoading();
            this.uiController.showToast('File deleted', 'success');
            await this.loadFiles();
        } catch (error) {
            this.uiController.hideLoading();
            this.uiController.showToast(`Delete failed: ${error.message}`, 'error');
        }
    }

    async handleRetryFile(fileId) {
        const file = this.files.find((item) => String(item.id) === String(fileId));
        if (!file) {
            this.uiController.showToast('File not found', 'error');
            return;
        }

        const mode = document.getElementById('mode-selector')?.value || 'local';
        const model = document.getElementById('embedding-model-selector')?.value || null;

        try {
            this.uiController.showLoading('Scheduling reprocessing...');
            await this.apiService.reprocessFile(fileId, mode, model);
            this.uiController.hideLoading();
            this.uiController.showToast('Reprocessing scheduled', 'success');
            await this.loadFiles();
        } catch (error) {
            this.uiController.hideLoading();
            this.uiController.showToast(`Retry failed: ${error.message}`, 'error');
        }
    }

    setCurrentConversation(conversationId) {
        this.currentConversationId = conversationId;
    }

    destroy() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
            this.refreshInterval = null;
        }
    }
}

window.toggleFilesSidebar = function() {
    const sidebar = document.getElementById('filesSidebar');
    if (sidebar) {
        sidebar.classList.toggle('active');
    }
};

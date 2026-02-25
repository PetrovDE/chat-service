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
            if (!deleteBtn) return;

            event.preventDefault();
            event.stopPropagation();

            const fileId = deleteBtn.dataset.fileId;
            if (!fileId) return;

            await this.handleDeleteFile(fileId);
        });
    }

    async loadFiles(silent = false) {
        try {
            if (!silent) this.showLoading();
            const response = await this.apiService.getProcessedFiles(this.currentConversationId);
            this.files = Array.isArray(response) ? response : [];
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
        const uploadDate = this.formatDate(file.uploaded_at);
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
                    <button class="file-item-btn delete" data-action="delete" data-file-id="${file.id}" type="button">Delete</button>
                </div>
            </div>
        `;
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
        if (status === 'failed') {
            return '<span class="file-item-status failed">Failed</span>';
        }
        return '<span class="file-item-status pending">Pending</span>';
    }

    formatFileSize(bytes) {
        if (!bytes) return '0 B';
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${sizes[i]}`;
    }

    formatDate(dateString) {
        if (!dateString) return '';

        const date = new Date(dateString);
        const now = new Date();
        const diffMs = now - date;
        const diffMins = Math.floor(diffMs / 60000);
        const diffHours = Math.floor(diffMs / 3600000);
        const diffDays = Math.floor(diffMs / 86400000);

        if (diffMins < 1) return 'just now';
        if (diffMins < 60) return `${diffMins} min ago`;
        if (diffHours < 24) return `${diffHours} h ago`;
        if (diffDays < 7) return `${diffDays} d ago`;

        return date.toLocaleDateString();
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

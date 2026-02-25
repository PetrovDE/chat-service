class FileManager {
    constructor(chatManager) {
        this.chatManager = chatManager;
        this.attachedFiles = [];
    }

    initializeFileInput() {
        const fileInput = document.getElementById('fileInput');
        if (fileInput) {
            fileInput.addEventListener('change', (event) => this.handleFileSelect(event));
        }

        const attachBtn = document.querySelector('.attach-btn');
        if (attachBtn) {
            attachBtn.addEventListener('click', () => fileInput?.click());
        }
    }

    async handleFileSelect(event) {
        const file = event.target.files[0];
        if (!file) return;

        const allowedTypes = ['.pdf', '.docx', '.txt', '.md', '.csv', '.json', '.xlsx'];
        const fileExt = `.${file.name.split('.').pop().toLowerCase()}`;

        if (!allowedTypes.includes(fileExt)) {
            this.showError(`Unsupported file type. Allowed: ${allowedTypes.join(', ')}`);
            event.target.value = '';
            return;
        }

        const maxSize = 50 * 1024 * 1024;
        if (file.size > maxSize) {
            this.showError('File too large. Max file size is 50MB.');
            event.target.value = '';
            return;
        }

        try {
            await this.uploadAndProcess(file);
        } catch (error) {
            this.showError(error.message || 'File upload failed');
        } finally {
            event.target.value = '';
        }
    }

    getConversationId() {
        return this.chatManager?.getCurrentConversation?.() || null;
    }

    getEmbeddingMode() {
        return document.getElementById('mode-selector')?.value || 'local';
    }

    getEmbeddingModel() {
        return document.getElementById('model-selector')?.value || null;
    }

    async uploadAndProcess(file) {
        const conversationId = this.getConversationId();
        if (!conversationId) {
            throw new Error('Open or create a chat before attaching files');
        }

        const formData = new FormData();
        formData.append('file', file);
        formData.append('conversation_id', conversationId);
        formData.append('embedding_mode', this.getEmbeddingMode());

        const embeddingModel = this.getEmbeddingModel();
        if (embeddingModel) {
            formData.append('embedding_model', embeddingModel);
        }

        this.renderPendingAttachment(file.name, file.size);

        const token = localStorage.getItem('auth_token');
        const headers = token ? { Authorization: `Bearer ${token}` } : {};

        const response = await fetch('/api/v1/files/upload', {
            method: 'POST',
            headers,
            body: formData,
        });

        if (!response.ok) {
            const payload = await response.json().catch(() => ({}));
            throw new Error(payload.detail || `Upload failed (${response.status})`);
        }

        const result = await response.json();
        await this.waitForProcessingComplete(result.file_id, file.name, file.size);
    }

    async waitForProcessingComplete(fileId, fileName, fileSize) {
        const maxAttempts = 120;

        for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
            const token = localStorage.getItem('auth_token');
            const headers = token ? { Authorization: `Bearer ${token}` } : {};

            const response = await fetch(`/api/v1/files/${fileId}`, { headers });
            if (!response.ok) {
                throw new Error(`Failed to check file status (${response.status})`);
            }

            const fileInfo = await response.json();
            const status = fileInfo.is_processed;
            this.renderPendingAttachment(fileName, fileSize, status);

            if (status === 'completed') {
                this.attachedFiles.push({
                    id: fileId,
                    name: fileName,
                    size: fileSize,
                    type: fileInfo.file_type || '',
                });
                this.renderAttachedFiles();
                if (window.app?.uiController) {
                    window.app.uiController.showSuccess(`File "${fileName}" attached`);
                }
                return;
            }

            if (status === 'failed') {
                throw new Error('File processing failed');
            }

            await new Promise((resolve) => setTimeout(resolve, 1000));
        }

        throw new Error('File processing timeout');
    }

    renderPendingAttachment(name, size, status = 'processing') {
        const container = document.getElementById('attachedFilesContainer');
        const list = document.getElementById('attachedFilesList');
        if (!container || !list) return;

        container.style.display = 'block';
        list.innerHTML = `
            <div class="attached-file-item is-pending" role="status" aria-live="polite">
                <div class="file-icon">FILE</div>
                <div class="file-info">
                    <div class="file-name">${this.escapeHtml(name)}</div>
                    <div class="file-meta">${this.formatSize(size)} | ${this.escapeHtml(status)}</div>
                </div>
                <div class="spinner small"></div>
            </div>
        `;
    }

    renderAttachedFiles() {
        const container = document.getElementById('attachedFilesContainer');
        const list = document.getElementById('attachedFilesList');
        if (!container || !list) return;

        if (this.attachedFiles.length === 0) {
            container.style.display = 'none';
            return;
        }

        container.style.display = 'block';
        list.innerHTML = this.attachedFiles
            .map(
                (file, index) => `
                    <div class="attached-file-item">
                        <div class="file-icon">FILE</div>
                        <div class="file-info">
                            <div class="file-name">${this.escapeHtml(file.name)}</div>
                            <div class="file-meta">${this.formatSize(file.size)}</div>
                        </div>
                        <div class="file-actions">
                            <button class="icon-btn" type="button" aria-label="Remove attachment" onclick="window.removeAttachedFile(${index})">X</button>
                        </div>
                    </div>
                `
            )
            .join('');
    }

    removeFile(index) {
        this.attachedFiles.splice(index, 1);
        this.renderAttachedFiles();
    }

    clearAttachedFiles() {
        this.attachedFiles = [];
        this.renderAttachedFiles();
    }

    getAttachedFiles() {
        return [...this.attachedFiles];
    }

    showError(message) {
        if (window.app?.uiController) {
            window.app.uiController.showError(message);
            return;
        }
        alert(message);
    }

    formatSize(bytes) {
        if (!bytes) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB'];
        const index = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
        return `${(bytes / 1024 ** index).toFixed(index === 0 ? 0 : 1)} ${units[index]}`;
    }

    escapeHtml(value) {
        const div = document.createElement('div');
        div.textContent = value;
        return div.innerHTML;
    }
}

export { FileManager };

window.clearAttachedFiles = function() {
    if (window.app?.fileManager) {
        window.app.fileManager.clearAttachedFiles();
    }
};

window.removeAttachedFile = function(index) {
    if (window.app?.fileManager) {
        window.app.fileManager.removeFile(index);
    }
};


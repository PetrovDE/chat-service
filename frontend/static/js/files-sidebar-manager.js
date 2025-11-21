// frontend/static/js/files-sidebar-manager.js

export class FilesSidebarManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.files = [];
        this.refreshInterval = null;
    }

    initialize() {
        console.log('📁 Initializing Files Sidebar Manager');
        this.loadFiles();
            this.attachFileEventListeners();

        // Auto-refresh every 10 seconds
        this.refreshInterval = setInterval(() => {
            this.loadFiles(true);
        }, 10000);
    }

    async loadFiles(silent = false) {
        try {
            if (!silent) {
                this.showLoading();
            }

            const response = await this.apiService.getProcessedFiles();
            this.files = response || [];

            this.render();

            if (!silent) {
                console.log(`✓ Loaded ${this.files.length} processed files`);
            }
        } catch (error) {
            console.error('Error loading files:', error);
            if (!silent) {
                this.showError('Не удалось загрузить файлы');
            }
        }
    }

    showLoading() {
        const container = document.getElementById('filesSidebarList');
        if (container) {
            container.innerHTML = `
                <div class="files-loading">
                    <p>Загрузка файлов...</p>
                </div>
            `;
        }
    }

    showError(message) {
        const container = document.getElementById('filesSidebarList');
        if (container) {
            container.innerHTML = `
                <div class="files-empty">
                    <div class="files-empty-icon">⚠️</div>
                    <p>${message}</p>
                </div>
            `;
        }
    }

    render() {
        const container = document.getElementById('filesSidebarList');
        if (!container) return;

        if (this.files.length === 0) {
            container.innerHTML = `
                <div class="files-empty">
                    <div class="files-empty-icon">📭</div>
                    <p>Нет загруженных файлов</p>
                    <p style="font-size: 0.8rem; margin-top: 0.5rem;">
                        Загрузите документы для работы с RAG
                    </p>
                </div>
            `;
            return;
        }

        container.innerHTML = this.files.map(file => this.renderFileItem(file)).join('');

   }

    renderFileItem(file) {
        const icon = this.getFileIcon(file.file_type);
        const statusBadge = this.getStatusBadge(file.is_processed);
        const fileSize = this.formatFileSize(file.file_size);
        const uploadDate = this.formatDate(file.uploaded_at);

        return `
            <div class="file-item" data-file-id="${file.file_id}">
                <div class="file-item-header">
                    <div class="file-item-icon">${icon}</div>
                    <div class="file-item-info">
                        <h4 class="file-item-name" title="${file.original_filename}">
                            ${file.original_filename}
                        </h4>
                        <div class="file-item-meta">
                            <span>📊 ${fileSize}</span>
                            <span>📅 ${uploadDate}</span>
                        </div>
                        ${statusBadge}
                        ${file.chunks_count > 0 ? `
                            <div class="file-item-chunks">
                                📦 ${file.chunks_count} фрагментов
                            </div>
                        ` : ''}
                    </div>
                </div>
                <div class="file-item-actions">
                    <button class="file-item-btn delete" data-action="delete" data-file-id="${file.file_id}">
                        🗑️ Удалить
                    </button>
                </div>
            </div>
        `;
    }

    getFileIcon(fileType) {
        const icons = {
            'pdf': '📕',
            'docx': '📘',
            'doc': '📘',
            'txt': '📄',
            'md': '📝',
            'csv': '📊',
            'xlsx': '📗',
            'xls': '📗',
            'json': '📋',
        };
        return icons[fileType?.toLowerCase()] || '📄';
    }

    getStatusBadge(status) {
        const badges = {
            'completed': '<span class="file-item-status completed">✅ Обработан</span>',
            'processing': '<span class="file-item-status processing">⏳ Обработка...</span>',
            'pending': '<span class="file-item-status pending">⏸️ Ожидание</span>',
            'failed': '<span class="file-item-status failed">❌ Ошибка</span>',
        };
        return badges[status] || badges['pending'];
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

        if (diffMins < 1) return 'только что';
        if (diffMins < 60) return `${diffMins} мин назад`;
        if (diffHours < 24) return `${diffHours} ч назад`;
        if (diffDays < 7) return `${diffDays} дн назад`;

        return date.toLocaleDateString('ru-RU', {
            day: 'numeric',
            month: 'short',
            year: date.getFullYear() !== now.getFullYear() ? 'numeric' : undefined
        });
    }

    attachFileEventListeners() {
    // Use event delegation on the container for delete button clicks
    const container = document.getElementById('filesSidebarList');
    if (container) {
      container.addEventListener('click', async (e) => {
        const deleteBtn = e.target.closest('[data-action="delete"]');
        if (!deleteBtn) return;
        
        e.stopPropagation();
        const fileId = deleteBtn.dataset.fileId;
        await this.handleDeleteFile(fileId);
      });
    }
  }

    async handleDeleteFile(fileId) {
        const file = this.files.find(f => f.file_id === fileId);
        if (!file) return;

        const confirmed = confirm(
            `Вы уверены, что хотите удалить файл "${file.original_filename}"?\n\n` +
            `Это удалит:\n` +
            `• Файл с сервера\n` +
            `• Все embeddings из ChromaDB\n` +
            `• Все embeddings из PostgreSQL\n` +
            `• Запись из базы данных\n\n` +
            `Это действие нельзя отменить!`
        );

        if (!confirmed) return;

        try {
            this.uiController.showLoading('Удаление файла...');

            await this.apiService.deleteFile(fileId);

            this.uiController.hideLoading();
            this.uiController.showToast('✅ Файл успешно удален', 'success');

            // Reload files list
            await this.loadFiles();

        } catch (error) {
            console.error('Error deleting file:', error);
            this.uiController.hideLoading();
            this.uiController.showToast(
                `❌ Ошибка удаления: ${error.message}`,
                'error'
            );
        }
    }

    destroy() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
        }
    }
}

// Global toggle function
window.toggleFilesSidebar = function() {
    const sidebar = document.getElementById('filesSidebar');
    if (sidebar) {
        sidebar.classList.toggle('active');
    }
};

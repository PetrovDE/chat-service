// app/static/js/file-manager.js
export class FileManager {
    constructor(apiService, uiController, chatManager) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.chatManager = chatManager;
        this.uploadedFiles = [];
        this.fileInputInitialized = false;
    }

    initializeFileInput() {
        if (this.fileInputInitialized) {
            console.log('File input already initialized');
            return;
        }

        // Remove existing input if any
        const existing = document.getElementById('hiddenFileInput');
        if (existing) {
            existing.remove();
        }

        // Create hidden file input
        const fileInput = document.createElement('input');
        fileInput.type = 'file';
        fileInput.id = 'hiddenFileInput';
        fileInput.style.display = 'none';
        fileInput.accept = '.txt,.csv,.json,.pdf,.doc,.docx,.xlsx,.xls';
        fileInput.addEventListener('change', (e) => this.handleFileSelect(e));
        document.body.appendChild(fileInput);

        this.fileInputInitialized = true;
        console.log('✓ File input initialized');
    }

    openFileDialog() {
        console.log('FileManager.openFileDialog() called');

        // Ensure input exists
        if (!this.fileInputInitialized) {
            console.log('File input not initialized yet, initializing now...');
            this.initializeFileInput();
        }

        let fileInput = document.getElementById('hiddenFileInput');

        if (!fileInput) {
            console.error('File input element not found!');
            if (this.uiController) {
                this.uiController.showError('Ошибка: не удалось создать input для файлов');
            }
            return;
        }

        console.log('Triggering file input click...');
        fileInput.click();
    }

    async handleFileSelect(event) {
        const files = event.target.files;
        console.log(`Selected ${files.length} file(s)`);

        if (files.length === 0) return;

        for (let file of files) {
            await this.uploadFile(file);
        }

        // Clear input
        event.target.value = '';
    }

    async uploadFile(file) {
        try {
            if (this.uiController) {
                this.uiController.showLoading(`Загрузка ${file.name}...`);
            }

            const response = await this.apiService.uploadFile(file);

            this.uploadedFiles.push(response);

            if (this.uiController) {
                this.uiController.hideLoading();
                this.uiController.showSuccess(`Файл загружен: ${response.original_filename}`);
            }

            // Show file in attached files panel
            this.renderAttachedFiles();

            return response;

        } catch (error) {
            console.error('File upload error:', error);
            if (this.uiController) {
                this.uiController.hideLoading();
                this.uiController.showError('Ошибка загрузки файла: ' + error.message);
            }
            throw error;
        }
    }

    renderAttachedFiles() {
        const container = document.getElementById('attachedFilesContainer');
        if (!container) {
            console.warn('attachedFilesContainer not found');
            return;
        }

        if (this.uploadedFiles.length === 0) {
            container.style.display = 'none';
            return;
        }

        container.style.display = 'block';
        container.innerHTML = `
            <div class="attached-files-header">
                <span>📎 Прикреплённые файлы (${this.uploadedFiles.length})</span>
                <button class="icon-btn" onclick="window.app.fileManager.clearAllFiles()" title="Очистить все">
                    🗑️
                </button>
            </div>
            <div class="attached-files-list">
                ${this.uploadedFiles.map((file, index) => this.renderFileItem(file, index)).join('')}
            </div>
        `;
    }

    renderFileItem(file, index) {
        const sizeKB = (file.file_size / 1024).toFixed(1);
        return `
            <div class="attached-file-item" data-file-id="${file.file_id}">
                <div class="file-icon">${this.getFileIcon(file.file_type)}</div>
                <div class="file-info">
                    <div class="file-name">${this.escapeHtml(file.original_filename)}</div>
                    <div class="file-meta">${sizeKB} KB</div>
                </div>
                <div class="file-actions">
                    <button class="icon-btn" onclick="window.app.fileManager.analyzeFile('${file.file_id}')" title="Анализировать">
                        🔍
                    </button>
                    <button class="icon-btn" onclick="window.app.fileManager.removeFile(${index})" title="Удалить">
                        ❌
                    </button>
                </div>
            </div>
        `;
    }

    getFileIcon(fileType) {
        if (fileType.includes('text')) return '📄';
        if (fileType.includes('pdf')) return '📕';
        if (fileType.includes('csv')) return '📊';
        if (fileType.includes('json')) return '📋';
        if (fileType.includes('excel') || fileType.includes('spreadsheet')) return '📈';
        if (fileType.includes('word') || fileType.includes('document')) return '📝';
        return '📎';
    }

    async analyzeFile(fileId) {
        try {
            const file = this.uploadedFiles.find(f => f.file_id === fileId);
            if (!file) {
                throw new Error('File not found');
            }

            const query = prompt('Введите вопрос о файле (или оставьте пустым для общего анализа):');

            if (this.uiController) {
                this.uiController.showLoading('Анализ файла...');
            }

            const result = await this.apiService.post('/files/analyze', {
                file_id: fileId,
                query: query || null
            });

            if (this.uiController) {
                this.uiController.hideLoading();
            }

            // Add analysis result to chat
            if (this.chatManager && this.chatManager.addAnalysisToChat) {
                this.chatManager.addAnalysisToChat(file.original_filename, result.analysis);
            } else {
                // Fallback: add to chat messages directly
                const messagesContainer = document.getElementById('chatMessages');
                if (messagesContainer) {
                    const messageDiv = document.createElement('div');
                    messageDiv.className = 'message assistant';
                    messageDiv.innerHTML = `
                        <div class="message-bubble">
                            <strong>📊 Анализ файла: ${this.escapeHtml(file.original_filename)}</strong><br><br>
                            ${this.escapeHtml(result.analysis).replace(/\n/g, '<br>')}
                        </div>
                        <div class="message-time">${new Date().toLocaleTimeString('ru-RU', {hour: '2-digit', minute: '2-digit'})}</div>
                    `;
                    messagesContainer.appendChild(messageDiv);
                    messagesContainer.scrollTop = messagesContainer.scrollHeight;
                }
            }

            if (this.uiController) {
                this.uiController.showSuccess('Файл проанализирован');
            }

        } catch (error) {
            console.error('File analysis error:', error);
            if (this.uiController) {
                this.uiController.hideLoading();
                this.uiController.showError('Ошибка анализа файла: ' + error.message);
            }
        }
    }

    removeFile(index) {
        if (index >= 0 && index < this.uploadedFiles.length) {
            const file = this.uploadedFiles[index];
            this.uploadedFiles.splice(index, 1);
            this.renderAttachedFiles();
            if (this.uiController) {
                this.uiController.showSuccess(`Файл удалён: ${file.original_filename}`);
            }
        }
    }

    clearAllFiles() {
        if (confirm('Удалить все прикреплённые файлы?')) {
            this.uploadedFiles = [];
            this.renderAttachedFiles();
            if (this.uiController) {
                this.uiController.showSuccess('Все файлы удалены');
            }
        }
    }

    getUploadedFiles() {
        return this.uploadedFiles;
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}
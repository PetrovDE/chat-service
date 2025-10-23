// app/static/js/file-manager.js
// ⭐ ИСПРАВЛЕННЫЙ - ПРАВИЛЬНЫЙ URL ДЛЯ ANALYZE ⭐

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

        const existing = document.getElementById('hiddenFileInput');
        if (existing) {
            existing.remove();
        }

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

        if (!this.fileInputInitialized) {
            console.log('Initializing file input...');
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
                this.uiController.showSuccess(`✅ Файл загружен: ${response.original_filename}`);
            }

            this.renderAttachedFiles();
            return response;
        } catch (error) {
            console.error('File upload error:', error);
            if (this.uiController) {
                this.uiController.hideLoading();
                this.uiController.showError('Ошибка загрузки: ' + error.message);
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
            <div class="attached-files">
                <h4>📎 Прикрепленные файлы (${this.uploadedFiles.length})</h4>
                ${this.uploadedFiles.map(file => `
                    <div class="file-item" data-file-id="${file.file_id}">
                        <span class="file-icon">📄</span>
                        <span class="file-name">${file.original_filename}</span>
                        <span class="file-size">${this.formatFileSize(file.file_size)}</span>
                        <button onclick="window.app.fileManager.analyzeFile('${file.file_id}')" 
                                class="analyze-btn" title="Анализировать файл">
                            🔍
                        </button>
                        <button onclick="window.app.fileManager.removeFile('${file.file_id}')" 
                                class="remove-btn">
                            ✕
                        </button>
                    </div>
                `).join('')}
            </div>
        `;
    }

    async analyzeFile(fileId) {
        try {
            console.log('🔍 Analyzing file:', fileId);

            const query = prompt('Введите вопрос о файле (или оставьте пустым для общего анализа):');
            if (query === null) return; // Отмена

            if (this.uiController) {
                this.uiController.showLoading('Анализ файла...');
            }

            // ✅ ИСПРАВЛЕНО: правильный URL
            const response = await this.apiService.request('/files/analyze', {
                method: 'POST',
                body: JSON.stringify({
                    file_id: fileId,
                    query: query || null
                })
            });

            if (this.uiController) {
                this.uiController.hideLoading();
            }

            // Показать результат в чате
            if (this.chatManager) {
                this.chatManager.addAnalysisResult(response);
            } else {
                alert('Анализ завершен:\n\n' + response.analysis);
            }

            console.log('✅ Analysis completed');
        } catch (error) {
            console.error('❌ Analysis error:', error);
            if (this.uiController) {
                this.uiController.hideLoading();
                this.uiController.showError('Ошибка анализа: ' + error.message);
            }
        }
    }

    removeFile(fileId) {
        this.uploadedFiles = this.uploadedFiles.filter(f => f.file_id !== fileId);
        this.renderAttachedFiles();
        console.log('File removed from list:', fileId);
    }

    formatFileSize(bytes) {
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
        return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    }

    clearFiles() {
        this.uploadedFiles = [];
        this.renderAttachedFiles();
        console.log('All files cleared');
    }

    getUploadedFiles() {
        return this.uploadedFiles;
    }
}

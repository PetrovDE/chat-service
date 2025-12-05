// app/static/js/file-manager.js
class FileManager {
    constructor(chatManager) {
        this.chatManager = chatManager;
        this.attachedFiles = [];
        console.log('✓ FileManager initialized');
    }

    initializeFileInput() {
        const fileInput = document.getElementById('fileInput');
        if (fileInput) {
            fileInput.addEventListener('change', (e) => this.handleFileSelect(e));
            console.log('✓ File input initialized');
        }

        const attachBtn = document.querySelector('.attach-btn');
        if (attachBtn) {
            attachBtn.addEventListener('click', () => {
                fileInput?.click();
            });
        }
    }

    async handleFileSelect(event) {
        const file = event.target.files[0];
        if (!file) return;

        console.log('📄 File selected:', file.name, `(${(file.size / 1024).toFixed(2)} KB)`);

        // Validate file type
        const allowedTypes = ['.pdf', '.docx', '.txt', '.md', '.csv', '.json', '.xlsx'];
        const fileExt = '.' + file.name.split('.').pop().toLowerCase();

        if (!allowedTypes.includes(fileExt)) {
            alert(`Неподдерживаемый тип файла. Разрешены: ${allowedTypes.join(', ')}`);
            event.target.value = '';
            return;
        }

        // Validate file size (50MB max)
        const maxSize = 50 * 1024 * 1024;
        if (file.size > maxSize) {
            alert('Файл слишком большой. Максимальный размер: 50 МБ');
            event.target.value = '';
            return;
        }

        try {
            await this.uploadAndProcess(file);
            event.target.value = ''; // Clear input
        } catch (error) {
            console.error('❌ File upload error:', error);
            alert(`Ошибка загрузки файла: ${error.message}`);
            event.target.value = '';
        }
    }

    getConversationId() {
        // Try to get conversation ID from multiple sources
        let conversationId = null;

        // 1. Check chatManager.currentConversation (set after sending message)
        if (this.chatManager?.currentConversation) {
            conversationId = this.chatManager.currentConversation;
            console.log('✓ Using conversation ID from chatManager:', conversationId);
            return conversationId;
        }

        // 2. Try to get from URL if it contains conversation ID
        const urlParams = new URLSearchParams(window.location.search);
        const urlConversationId = urlParams.get('conversation_id');
        if (urlConversationId) {
            conversationId = urlConversationId;
            console.log('✓ Using conversation ID from URL:', conversationId);
            return conversationId;
        }

        // 3. Try to get from the active conversation in conversationsManager
        if (this.chatManager?.conversationsManager?.currentConversationId) {
            conversationId = this.chatManager.conversationsManager.currentConversationId;
            console.log('✓ Using conversation ID from conversationsManager:', conversationId);
            return conversationId;
        }

        console.warn('⚠️ Could not find conversation ID from any source');
        return null;
    }

    getEmbeddingMode() {
        const modeSelector = document.getElementById('mode-selector');
        if (modeSelector) {
            const mode = modeSelector.value;
            console.log('✓ Using embedding mode:', mode);
            return mode;
        }
        console.warn('⚠️ Mode selector not found, using default: local');
        return 'local';
    }

    getEmbeddingModel() {
        const modelSelector = document.getElementById('model-selector');
        if (modelSelector && modelSelector.value && modelSelector.value !== '') {
            const model = modelSelector.value;
            console.log('✓ Using embedding model:', model);
            return model;
        }
        console.log('ℹ️ No specific model selected, will use default');
        return null;
    }

    async uploadAndProcess(file) {
        console.log('📤 Uploading file:', file.name);

        // Get conversation ID from available sources
        const conversationId = this.getConversationId();

        if (!conversationId) {
            throw new Error('Не удалось определить активный чат. Пожалуйста, откройте или создайте беседу перед загрузкой файла.');
        }

        // Get embedding mode and model
        const embeddingMode = this.getEmbeddingMode();
        const embeddingModel = this.getEmbeddingModel();

        const formData = new FormData();
        formData.append('file', file);
        formData.append('conversation_id', conversationId);
        formData.append('embedding_mode', embeddingMode);

        if (embeddingModel) {
            formData.append('embedding_model', embeddingModel);
        }

        try {
            // Show loading
            const container = document.getElementById('attachedFilesContainer');
            if (container) {
                container.style.display = 'block';
                const list = document.getElementById('attachedFilesList');
                if (list) {
                    const modeText = embeddingMode === 'local' ? 'Локальные модели' : 'Корпоративный HUB';
                    list.innerHTML = `
                        <div style="display: flex; align-items: center; gap: 10px; padding: 10px; border-radius: 8px; background: #f0f7ff;">
                            <div class="spinner" style="width: 20px; height: 20px; border: 2px solid #e1e5e9; border-top-color: #007bff; border-radius: 50%; animation: spin 1s linear infinite;"></div>
                            <span>Загрузка и обработка файла (${modeText})...</span>
                        </div>
                    `;
                }
            }

            // Get auth token from localStorage
            const token = localStorage.getItem('auth_token');
            const headers = {};
            if (token) {
                headers['Authorization'] = `Bearer ${token}`;
            }

            const uploadUrl = `/api/v1/files/upload`;
            console.log('📡 Upload URL:', uploadUrl);
            console.log('📦 Embedding mode:', embeddingMode);
            console.log('🎯 Embedding model:', embeddingModel || 'default');

            const response = await fetch(uploadUrl, {
                method: 'POST',
                headers: headers,
                body: formData
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                console.error('❌ Server error response:', errorData);
                throw new Error(errorData.detail || `Upload failed: ${response.status} ${response.statusText}`);
            }

            const result = await response.json();
            console.log('✅ File uploaded, status:', result.is_processed);

            // Handle different processing statuses
            if (result.is_processed === 'pending' || result.is_processed === 'processing') {
                console.log('⏳ File is processing, waiting for completion...');
                await this.waitForProcessingComplete(result.file_id, file.name, embeddingMode);
            } else if (result.is_processed === 'completed') {
                console.log('✅ File already processed');
                this.attachedFiles.push({
                    id: result.file_id,
                    name: file.name,
                    size: file.size,
                    type: file.type
                });
                this.renderAttachedFiles();
                const modeText = embeddingMode === 'local' ? 'локальными моделями' : 'корпоративным HUB';
                alert(`Файл "${file.name}" успешно загружен и обработан ${modeText}!`);
            } else if (result.is_processed === 'failed') {
                throw new Error('Файл был загружен, но произошла ошибка при обработке');
            } else {
                console.log(`⚠️ Unknown status '${result.is_processed}', waiting for completion...`);
                await this.waitForProcessingComplete(result.file_id, file.name, embeddingMode);
            }

        } catch (error) {
            console.error('❌ Upload error:', error);
            // Hide loading
            const container = document.getElementById('attachedFilesContainer');
            if (container) {
                container.style.display = 'none';
            }
            throw error;
        }
    }

    async waitForProcessingComplete(fileId, fileName, embeddingMode) {
        const maxAttempts = 120; // Максимум 120 попыток (2 минуты)
        let attempts = 0;

        const checkStatus = async () => {
            try {
                const token = localStorage.getItem('auth_token');
                const headers = {};
                if (token) {
                    headers['Authorization'] = `Bearer ${token}`;
                }

                const response = await fetch(`/api/v1/files/${fileId}`, {
                    headers: headers
                });

                if (!response.ok) {
                    throw new Error(`Failed to check file status: ${response.status}`);
                }

                const fileInfo = await response.json();
                console.log(`📊 File status check (attempt ${attempts + 1}/${maxAttempts}): ${fileInfo.is_processed}`);

                if (fileInfo.is_processed === 'completed') {
                    console.log('✅ File processing completed!');
                    this.attachedFiles.push({
                        id: fileId,
                        name: fileName,
                        size: fileInfo.file_size,
                        type: fileInfo.file_type
                    });
                    this.renderAttachedFiles();
                    const modeText = embeddingMode === 'local' ? 'локальными моделями' : 'корпоративным HUB';
                    alert(`Файл "${fileName}" успешно загружен и обработан ${modeText}!`);
                    return true;
                } else if (fileInfo.is_processed === 'failed') {
                    throw new Error('Ошибка обработки файла на сервере');
                } else if (fileInfo.is_processed === 'pending' || fileInfo.is_processed === 'processing') {
                    attempts++;
                    if (attempts >= maxAttempts) {
                        throw new Error('Превышено время ожидания обработки файла (2 минуты)');
                    }
                    // Ждем 1 секунду и проверяем снова
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    return await checkStatus();
                } else {
                    console.warn(`⚠️ Unknown status: ${fileInfo.is_processed}`);
                    attempts++;
                    if (attempts >= maxAttempts) {
                        throw new Error('Превышено время ожидания обработки файла');
                    }
                    await new Promise(resolve => setTimeout(resolve, 1000));
                    return await checkStatus();
                }
            } catch (error) {
                console.error('❌ Error checking file status:', error);
                const container = document.getElementById('attachedFilesContainer');
                if (container) {
                    container.style.display = 'none';
                }
                throw error;
            }
        };

        await checkStatus();
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
        list.innerHTML = this.attachedFiles.map((file, index) => `
            <div style="display: flex; justify-content: space-between; align-items: center; padding: 10px; border-radius: 8px; background: #f8f9fa; margin-bottom: 5px;">
                <div style="display: flex; align-items: center; gap: 10px;">
                    <span>📄</span>
                    <div>
                        <div style="font-weight: 500;">${file.name}</div>
                        <div style="font-size: 0.85rem; color: #6c757d;">${(file.size / 1024).toFixed(2)} KB</div>
                    </div>
                </div>
                <button onclick="window.removeAttachedFile(${index})" style="background: none; border: none; color: #dc3545; cursor: pointer; font-size: 1.2rem;">✕</button>
            </div>
        `).join('');
    }

    removeFile(index) {
        console.log('🗑️ Removing file:', this.attachedFiles[index].name);
        this.attachedFiles.splice(index, 1);
        this.renderAttachedFiles();
    }

    clearAttachedFiles() {
        console.log('🗑️ Clearing all attached files');
        this.attachedFiles = [];
        this.renderAttachedFiles();
    }

    getAttachedFiles() {
        return this.attachedFiles;
    }
}

export { FileManager };

// Global functions for HTML onclick
window.clearAttachedFiles = function() {
    if (window.app && window.app.fileManager) {
        window.app.fileManager.clearAttachedFiles();
    }
};

window.removeAttachedFile = function(index) {
    if (window.app && window.app.fileManager) {
        window.app.fileManager.removeFile(index);
    }
};

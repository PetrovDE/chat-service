// app/static/js/file-manager.js
class FileManager {
  constructor(apiService, uiController, chatManager) {
    this.apiService = apiService;
    this.uiController = uiController;
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

  async uploadAndProcess(file) {
    console.log('📤 Uploading file:', file.name);

    // Get conversation ID from available sources
    const conversationId = this.getConversationId();
    if (!conversationId) {
      throw new Error('Не удалось определить активный чат. Пожалуйста, откройте или создайте беседу перед загрузкой файла.');
    }

    const formData = new FormData();
    formData.append('file', file);

    try {
      // Show loading
      const container = document.getElementById('attachedFilesContainer');
      if (container) {
        container.style.display = 'block';
        const list = document.getElementById('attachedFilesList');
        if (list) {
          list.innerHTML = `
            <div style="padding: 1rem; text-align: center; color: #666;">
              <div class="spinner" style="width: 30px; height: 30px; margin: 0 auto;"></div>
              <p style="margin-top: 0.5rem;">Загрузка файла...</p>
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

      // ВАЖНО: Pass conversation_id as query parameter
      const uploadUrl = `/api/v1/files/upload?conversation_id=${conversationId}`;
      console.log('📡 Upload URL:', uploadUrl);

      const response = await fetch(uploadUrl, {
        method: 'POST',
        headers: headers,
        body: formData
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        console.error('❌ Server error response:', errorData);
        throw new Error(`Upload failed: ${response.status} ${response.statusText}`);
      }

      const result = await response.json();
      console.log('✅ File uploaded:', result);

      this.attachedFiles.push({
        id: result.file_id,  // Changed from document_id to file_id
        name: file.name,
        size: file.size,
        type: file.type
      });

      this.renderAttachedFiles();
      alert(`Файл "${file.name}" успешно загружен и обработан!`);
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
      <div class="attached-file-item">
        <span class="file-icon">📄</span>
        <div class="file-info">
          <div class="file-name">${file.name}</div>
          <div class="file-meta">${(file.size / 1024).toFixed(2)} KB</div>
        </div>
        <div class="file-actions">
          <button class="icon-btn" onclick="window.app.fileManager.removeFile(${index})" title="Удалить">
            ✕
          </button>
        </div>
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

// Global function for HTML onclick
window.clearAttachedFiles = function() {
  if (window.app && window.app.fileManager) {
    window.app.fileManager.clearAttachedFiles();
  }
};

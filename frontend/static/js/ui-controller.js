// frontend/static/js/ui-controller.js

class UIController {
    constructor() {
        this.modeSelector = document.getElementById('mode-selector');
        this.modelSelector = document.getElementById('model-selector');
        this.sendButton = document.getElementById('sendMessage');
        this.promptInput = document.getElementById('messageInput');
        this.chatHistory = document.getElementById('chatMessages');
        this.toastTimeout = null;

        console.log('✓ UIController initialized');
    }

    renderHistory(messages) {
        if (!this.chatHistory) return;

        this.chatHistory.innerHTML = messages.map(msg => `
            <div class="message ${msg.role}">
                <div class="message-bubble">${msg.content}</div>
            </div>
        `).join('');
    }

    showError(message) {
        console.error('UI Error:', message);
        this.showToast(message, 'error');
    }

    showSuccess(message) {
        console.log('UI Success:', message);
        this.showToast(message, 'success');
    }

    showToast(message, type = 'success') {
        // Создаем или находим контейнер для тоста
        let toast = document.getElementById('toastNotification');
        if (!toast) {
            toast = document.createElement('div');
            toast.id = 'toastNotification';
            toast.className = 'toast-notification';
            document.body.appendChild(toast);
        }

        toast.className = `toast-notification ${type}`;
        toast.innerHTML = `
            <div class="toast-content">
                <span class="toast-icon">${type === 'success' ? '✅' : '❌'}</span>
                <span class="toast-message">${message}</span>
            </div>
        `;
        toast.classList.add('show');

        if (this.toastTimeout) clearTimeout(this.toastTimeout);
        this.toastTimeout = setTimeout(() => {
            toast.classList.remove('show');
        }, 3000);
    }

    showLoading(text = 'Загрузка...') {
        let overlay = document.getElementById('loadingOverlay');
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.id = 'loadingOverlay';
            overlay.className = 'loading-overlay';
            overlay.innerHTML = `
                <div class="loading-content">
                    <div class="spinner"></div>
                    <p id="loadingText">${text}</p>
                </div>
            `;
            document.body.appendChild(overlay);
        }
        const loadingText = document.getElementById('loadingText');
        if (loadingText) {
            loadingText.innerText = text;
        }
        overlay.style.display = 'flex';
    }

    hideLoading() {
        const overlay = document.getElementById('loadingOverlay');
        if (overlay) {
            overlay.style.display = 'none';
        }
    }
}

export { UIController };

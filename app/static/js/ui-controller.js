// app/static/js/ui-controller.js
export class UIController {
    constructor() {
        this.messageCounter = 0;
    }

    addMessage(role, content, isStreaming = false) {
        const messagesContainer = document.getElementById('chatMessages');
        if (!messagesContainer) return null;

        const messageId = `msg-${Date.now()}-${this.messageCounter++}`;
        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${role}`;
        messageDiv.id = messageId;

        const bubble = document.createElement('div');
        bubble.className = 'message-bubble';
        bubble.textContent = content || (isStreaming ? '▋' : '');

        const timeDiv = document.createElement('div');
        timeDiv.className = 'message-time';
        timeDiv.textContent = new Date().toLocaleTimeString('ru-RU', {
            hour: '2-digit',
            minute: '2-digit'
        });

        messageDiv.appendChild(bubble);
        messageDiv.appendChild(timeDiv);
        messagesContainer.appendChild(messageDiv);

        // Прокрутить вниз
        messagesContainer.scrollTop = messagesContainer.scrollHeight;

        return messageId;
    }

    updateMessageContent(messageId, content) {
        const messageDiv = document.getElementById(messageId);
        if (!messageDiv) return;

        const bubble = messageDiv.querySelector('.message-bubble');
        if (bubble) {
            bubble.textContent = content + '▋'; // Добавить курсор

            // Прокрутить вниз
            const container = document.getElementById('chatMessages');
            if (container) {
                container.scrollTop = container.scrollHeight;
            }
        }
    }

    finalizeMessage(messageId) {
        const messageDiv = document.getElementById(messageId);
        if (!messageDiv) return;

        const bubble = messageDiv.querySelector('.message-bubble');
        if (bubble) {
            // Убрать курсор
            bubble.textContent = bubble.textContent.replace('▋', '');
        }
    }

    clearMessages() {
        const messagesContainer = document.getElementById('chatMessages');
        if (messagesContainer) {
            messagesContainer.innerHTML = `
                <div class="message assistant">
                    <div class="message-bubble">
                        Привет! Я Llama 3.1 8B. Чем могу помочь?
                    </div>
                    <div class="message-time">Сейчас</div>
                </div>
            `;
        }
    }

    showLoading(text = 'Загрузка...') {
        const overlay = document.getElementById('loadingOverlay');
        const loadingText = document.getElementById('loadingText');

        if (overlay) {
            overlay.style.display = 'flex';
        }

        if (loadingText) {
            loadingText.textContent = text;
        }
    }

    hideLoading() {
        const overlay = document.getElementById('loadingOverlay');
        if (overlay) {
            overlay.style.display = 'none';
        }
    }

    showError(message) {
        console.error(message);

        // Create toast notification
        const toast = document.createElement('div');
        toast.className = 'toast-notification error';
        toast.innerHTML = `
            <div class="toast-content">
                <span class="toast-icon">❌</span>
                <span class="toast-message">${this.escapeHtml(message)}</span>
            </div>
        `;

        document.body.appendChild(toast);

        // Animate in
        setTimeout(() => toast.classList.add('show'), 10);

        // Remove after 5 seconds
        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => toast.remove(), 300);
        }, 5000);
    }

    showSuccess(message) {
        console.log(message);

        // Create toast notification
        const toast = document.createElement('div');
        toast.className = 'toast-notification success';
        toast.innerHTML = `
            <div class="toast-content">
                <span class="toast-icon">✅</span>
                <span class="toast-message">${this.escapeHtml(message)}</span>
            </div>
        `;

        document.body.appendChild(toast);

        // Animate in
        setTimeout(() => toast.classList.add('show'), 10);

        // Remove after 3 seconds
        setTimeout(() => {
            toast.classList.remove('show');
            setTimeout(() => toast.remove(), 300);
        }, 3000);
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}
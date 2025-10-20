// app/static/js/ui-controller.js
export class UIController {
    constructor() {
        this.messageCounter = 0;

        // 🆕 Настройка marked.js
        if (typeof marked !== 'undefined') {
            marked.setOptions({
                highlight: function(code, lang) {
                    if (lang && hljs.getLanguage(lang)) {
                        try {
                            return hljs.highlight(code, { language: lang }).value;
                        } catch (err) {}
                    }
                    return hljs.highlightAuto(code).value;
                },
                breaks: true,  // Поддержка переносов строк
                gfm: true      // GitHub Flavored Markdown
            });
        }
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

        // 🆕 Рендерим Markdown если это assistant и есть контент
        if (role === 'assistant' && content && !isStreaming) {
            bubble.innerHTML = this.renderMarkdown(content);
        } else {
            bubble.textContent = content || (isStreaming ? '▋' : '');
        }

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
            // 🆕 Во время streaming показываем текст + курсор
            bubble.textContent = content + '▋';

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
            // 🆕 Убрать курсор и отрендерить Markdown
            const content = bubble.textContent.replace('▋', '');
            bubble.innerHTML = this.renderMarkdown(content);

            // 🆕 Применить подсветку кода
            bubble.querySelectorAll('pre code').forEach((block) => {
                if (typeof hljs !== 'undefined') {
                    hljs.highlightElement(block);
                }
            });
        }
    }

    // 🆕 Метод для рендеринга Markdown
    renderMarkdown(text) {
        if (!text) return '';

        // Если marked.js доступен - используем его
        if (typeof marked !== 'undefined') {
            try {
                return marked.parse(text);
            } catch (e) {
                console.error('Markdown parsing error:', e);
                return this.escapeHtml(text).replace(/\n/g, '<br>');
            }
        }

        // Fallback: простая замена переносов строк
        return this.escapeHtml(text).replace(/\n/g, '<br>');
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
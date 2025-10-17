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

    showError(message) {
        // Можно использовать toast notifications или alert
        console.error(message);
        alert(message);
    }

    showSuccess(message) {
        console.log(message);
        // Можно добавить toast notification
    }
}
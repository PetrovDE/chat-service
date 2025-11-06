// app/static/js/ui-controller.js

class UIController {
    constructor() {
        this.modeSelector = document.getElementById('mode-selector');
        this.modelSelector = document.getElementById('model-selector');
        this.sendButton = document.getElementById('sendMessage');
        this.promptInput = document.getElementById('messageInput');
        this.chatHistory = document.getElementById('chatMessages');

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
        alert(message);
    }

    showSuccess(message) {
        console.log('UI Success:', message);
    }
}

export { UIController };

// app/static/js/chat-manager.js

class ChatManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.currentConversation = null;
        this.isGenerating = false;
        this.abortController = null;
        console.log('✓ ChatManager initialized');
    }

    async sendMessage(message, conversationId, settings) {
        console.log('📤 Sending message:', message);

        if (this.isGenerating) {
            console.warn('⚠️ Already generating, please wait');
            return;
        }

        try {
            this.isGenerating = true;
            this.showGenerating(true);

            // Add user message to UI
            this.addMessageToUI('user', message);

            // Prepare request
            const payload = {
                message: message,
                conversation_id: conversationId,
                model: settings.model || 'llama3',
                temperature: settings.temperature || 0.7,
                max_tokens: settings.max_tokens || 2048
            };

            console.log('📡 Request payload:', payload);

            // Send to API
            const response = await this.apiService.post('/chat/send', payload);
            console.log('✓ Response received:', response);

            // Add assistant response to UI
            if (response.response) {
                this.addMessageToUI('assistant', response.response);
            }

            this.isGenerating = false;
            this.showGenerating(false);

            return response;
        } catch (error) {
            console.error('❌ Send message error:', error);
            this.isGenerating = false;
            this.showGenerating(false);
            this.addMessageToUI('assistant', `Ошибка: ${error.message}`);
            throw error;
        }
    }

    addMessageToUI(role, content) {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return;

        // Remove welcome message if exists
        const welcome = chatMessages.querySelector('[style*="text-align: center"]');
        if (welcome) {
            welcome.remove();
        }

        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${role}`;
        messageDiv.innerHTML = `
            <div class="message-bubble">${this.formatMessage(content)}</div>
            <div class="message-time">${new Date().toLocaleTimeString()}</div>
        `;

        chatMessages.appendChild(messageDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    formatMessage(text) {
        // Simple formatting - can be enhanced with markdown parser
        return text
            .replace(/\n/g, '<br>')
            .replace(/```(.*?)```/gs, '<pre><code>$1</code></pre>')
            .replace(/`([^`]+)`/g, '<code>$1</code>');
    }

    showGenerating(show) {
        const sendBtn = document.getElementById('sendMessage');
        const stopBtn = document.getElementById('stopGeneration');

        if (show) {
            if (sendBtn) sendBtn.style.display = 'none';
            if (stopBtn) stopBtn.style.display = 'block';
        } else {
            if (sendBtn) sendBtn.style.display = 'block';
            if (stopBtn) stopBtn.style.display = 'none';
        }
    }

    getCurrentConversation() {
        return this.currentConversation;
    }

    setCurrentConversation(id) {
        this.currentConversation = id;
        console.log('✓ Current conversation set:', id);
    }

    stopGeneration() {
        console.log('⏹️ Stopping generation');
        this.isGenerating = false;
        this.showGenerating(false);

        if (this.abortController) {
            this.abortController.abort();
            this.abortController = null;
        }
    }
}

export { ChatManager };

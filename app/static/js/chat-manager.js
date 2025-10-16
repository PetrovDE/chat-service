// Chat manager - handles chat UI and interactions
import { formatTime } from './utils.js';

export class ChatManager {
    constructor(apiService, uiController, conversationsManager) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.conversationsManager = conversationsManager;
        this.currentConversationId = null;
    }

    async sendMessage(message) {
        try {
            // Add user message to UI
            this.addMessage('user', message);

            // Show loading
            this.uiController.showLoading('Generating response...');

            // Get current conversation ID
            const conversationId = this.conversationsManager.getCurrentConversationId();

            // Send to API
            const response = await this.apiService.chat(
                message,
                conversationId
            );

            // Hide loading
            this.uiController.hideLoading();

            // Add assistant response
            this.addMessage('assistant', response.response);

            // Update conversation ID
            if (response.conversation_id) {
                this.conversationsManager.setCurrentConversationId(response.conversation_id);

                // Reload conversations list to show new conversation
                await this.conversationsManager.loadConversations();
            }

        } catch (error) {
            console.error('Error sending message:', error);
            this.uiController.hideLoading();
            this.uiController.showError('Failed to get response: ' + error.message);
        }
    }

    addMessage(role, content) {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return;

        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${role}`;

        // Format content with basic markdown
        const formattedContent = content
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\n/g, '<br>');

        messageDiv.innerHTML = `
            <div class="message-bubble">${formattedContent}</div>
            <div class="message-time">${formatTime(new Date())}</div>
        `;

        chatMessages.appendChild(messageDiv);

        // Scroll to bottom
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    clearChat() {
        const chatMessages = document.getElementById('chatMessages');
        if (chatMessages) {
            chatMessages.innerHTML = `
                <div class="message assistant">
                    <div class="message-bubble">
                        Chat cleared. Start a new conversation!
                    </div>
                    <div class="message-time">${formatTime(new Date())}</div>
                </div>
            `;
        }

        // Reset conversation ID
        this.conversationsManager.setCurrentConversationId(null);
    }
}
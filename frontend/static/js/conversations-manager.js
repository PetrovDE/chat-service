// app/static/js/conversations-manager.js

class ConversationsManager {
    constructor(apiService, uiController, chatManager) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.chatManager = chatManager;
        this.conversations = [];
        console.log('✓ ConversationsManager initialized');
    }

    async loadConversations() {
        console.log('📋 Loading conversations');
        try {
            const response = await this.apiService.get('/api/v1/conversations');
            this.conversations = response || [];
            console.log(`✓ Loaded ${this.conversations.length} conversations`);
            this.renderConversations();
        } catch (error) {
            console.error('❌ Load conversations error:', error);
            this.conversations = [];
            this.renderConversations();
        }
    }

    renderConversations() {
        const container = document.getElementById('conversationsList');
        if (!container) return;

        if (this.conversations.length === 0) {
            container.innerHTML = '<div class="conversations-loading">Нет разговоров</div>';
        } else {
            container.innerHTML = this.conversations.map(conv => `
                <div class="conversation-item" onclick="loadConversation('${conv.id}')">
                    <div class="conversation-title">${conv.title || 'Разговор'}</div>
                </div>
            `).join('');
        }
    }

    // ИСПРАВЛЕНО: не вызываем POST, просто очищаем чат
    createNewConversation() {
        console.log('➕ Creating new conversation');

        // Очищаем текущий разговор - новый создастся автоматически при первом сообщении
        this.chatManager.setCurrentConversation(null);

        // Очищаем чат
        const chatMessages = document.getElementById('chatMessages');
        if (chatMessages) {
            chatMessages.innerHTML = `
                <div style="text-align: center; padding: 4rem 2rem; color: #8e8e93;">
                    <h2 style="font-size: 2rem; margin-bottom: 1rem; color: #1f2937;">💬 Новый разговор</h2>
                    <p>Напишите сообщение чтобы начать</p>
                </div>
            `;
        }

        console.log('✅ Ready for new conversation');
    }
}

export { ConversationsManager };

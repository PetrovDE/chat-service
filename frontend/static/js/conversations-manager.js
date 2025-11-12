// frontend/static/js/conversations-manager.js
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
            // ИСПРАВЛЕНО: убрали /api/v1 из пути - он уже в baseURL
            const response = await this.apiService.get('/conversations');
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
                <div class="conversation-item" onclick="window.app.conversationsManager.loadConversation('${conv.id}')">
                    <div class="conversation-title">${conv.title || 'Разговор'}</div>
                    <div class="conversation-date">${new Date(conv.created_at).toLocaleDateString('ru-RU')}</div>
                </div>
            `).join('');
        }
    }

    async loadConversation(conversationId) {
        console.log('📖 Loading conversation:', conversationId);
        try {
            // ИСПРАВЛЕНО: убрали /api/v1 из пути
            const messages = await this.apiService.get(`/conversations/${conversationId}/messages`);

            // Установить текущий разговор
            this.chatManager.setCurrentConversation(conversationId);

            // Очистить и отобразить сообщения
            const chatMessages = document.getElementById('chatMessages');
            if (chatMessages) {
                chatMessages.innerHTML = '';

                messages.forEach(msg => {
                    this.chatManager.addMessageToUI(msg.role, msg.content);
                });
            }

            console.log(`✓ Loaded ${messages.length} messages`);
        } catch (error) {
            console.error('❌ Load conversation error:', error);
        }
    }

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

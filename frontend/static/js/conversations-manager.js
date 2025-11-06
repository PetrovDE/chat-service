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
            const response = await this.apiService.get('/conversations');
            this.conversations = response.conversations || [];
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

    async createNewConversation() {
        console.log('➕ Creating new conversation');
        try {
            const response = await this.apiService.post('/conversations', { title: 'Новый разговор' });
            console.log('✓ Conversation created:', response);
            await this.loadConversations();
            return response;
        } catch (error) {
            console.error('❌ Create conversation error:', error);
            throw error;
        }
    }
}

export { ConversationsManager };

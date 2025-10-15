// Conversations history manager
import { formatTime } from './utils.js';

export class ConversationsManager {
    constructor(apiService, uiController, authManager) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.authManager = authManager;
        this.conversations = [];
        this.currentConversationId = null;
    }

    async loadConversations() {
        try {
            // Only load if user is authenticated
            if (!this.authManager.isAuthenticated()) {
                this.showEmptyState('Войдите для просмотра истории бесед');
                return;
            }

            const conversationsList = document.getElementById('conversationsList');
            if (!conversationsList) return;

            conversationsList.innerHTML = '<div class="conversations-loading">Загрузка...</div>';

            const data = await this.apiService.get('/conversations?limit=50');
            this.conversations = data.conversations || [];

            if (this.conversations.length === 0) {
                this.showEmptyState('У вас пока нет бесед');
                return;
            }

            this.renderConversations();

        } catch (error) {
            console.error('Error loading conversations:', error);
            this.showEmptyState('Ошибка загрузки бесед');
        }
    }

    renderConversations() {
        const conversationsList = document.getElementById('conversationsList');
        if (!conversationsList) return;

        conversationsList.innerHTML = '';

        this.conversations.forEach(conv => {
            const item = this.createConversationItem(conv);
            conversationsList.appendChild(item);
        });
    }

    createConversationItem(conversation) {
        const div = document.createElement('div');
        div.className = 'conversation-item';
        div.dataset.conversationId = conversation.id;

        // Mark as active if current
        if (this.currentConversationId === conversation.id) {
            div.classList.add('active');
        }

        // Format date
        const date = new Date(conversation.updated_at);
        const dateStr = this.formatConversationDate(date);

        div.innerHTML = `
            <div class="conversation-title">
                <span class="conv-title-text">${this.escapeHtml(conversation.title)}</span>
                <div class="conversation-actions">
                    <button class="icon-btn" onclick="window.renameConversation('${conversation.id}')" title="Переименовать">
                        ✏️
                    </button>
                    <button class="icon-btn" onclick="window.deleteConversation('${conversation.id}')" title="Удалить">
                        🗑️
                    </button>
                </div>
            </div>
            <div class="conversation-meta">
                <span>${conversation.message_count || 0} сообщений</span>
                <span>•</span>
                <span>${dateStr}</span>
            </div>
        `;

        // Click to load conversation
        div.addEventListener('click', (e) => {
            // Don't trigger if clicking action buttons
            if (e.target.closest('.conversation-actions')) {
                return;
            }
            this.loadConversation(conversation.id);
        });

        return div;
    }

    formatConversationDate(date) {
        const now = new Date();
        const diff = now - date;
        const minutes = Math.floor(diff / 60000);
        const hours = Math.floor(diff / 3600000);
        const days = Math.floor(diff / 86400000);

        if (minutes < 1) return 'Только что';
        if (minutes < 60) return `${minutes} мин назад`;
        if (hours < 24) return `${hours} ч назад`;
        if (days < 7) return `${days} д назад`;

        return date.toLocaleDateString('ru-RU', {
            day: 'numeric',
            month: 'short'
        });
    }

    async loadConversation(conversationId) {
        try {
            this.uiController.showLoading('Загрузка беседы...');

            const data = await this.apiService.get(`/conversations/${conversationId}`);

            this.currentConversationId = conversationId;

            // Update active state in sidebar
            document.querySelectorAll('.conversation-item').forEach(item => {
                item.classList.remove('active');
            });
            const activeItem = document.querySelector(`[data-conversation-id="${conversationId}"]`);
            if (activeItem) {
                activeItem.classList.add('active');
            }

            // Load messages into chat
            this.loadMessagesIntoChat(data.messages);

            this.uiController.hideLoading();

        } catch (error) {
            console.error('Error loading conversation:', error);
            this.uiController.hideLoading();
            this.uiController.showError('Ошибка загрузки беседы');
        }
    }

    loadMessagesIntoChat(messages) {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return;

        // Clear current chat
        chatMessages.innerHTML = '';

        // Add all messages
        messages.forEach(msg => {
            this.addMessageToChat(msg.role, msg.content, new Date(msg.created_at));
        });

        // Scroll to bottom
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    addMessageToChat(role, content, timestamp) {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return;

        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${role}`;

        const formattedContent = content
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\n/g, '<br>');

        messageDiv.innerHTML = `
            <div class="message-bubble">${formattedContent}</div>
            <div class="message-time">${formatTime(timestamp)}</div>
        `;

        chatMessages.appendChild(messageDiv);
    }

    async createNewConversation() {
        try {
            // Clear current chat
            this.currentConversationId = null;

            const chatMessages = document.getElementById('chatMessages');
            if (chatMessages) {
                chatMessages.innerHTML = `
                    <div class="message assistant">
                        <div class="message-bubble">
                            Привет! Я Llama 3.1 8B. Чем могу помочь?
                        </div>
                        <div class="message-time">Сейчас</div>
                    </div>
                `;
            }

            // Clear active state
            document.querySelectorAll('.conversation-item').forEach(item => {
                item.classList.remove('active');
            });

            this.uiController.showSuccess('Новая беседа начата');

        } catch (error) {
            console.error('Error creating conversation:', error);
            this.uiController.showError('Ошибка создания беседы');
        }
    }

    async renameConversation(conversationId) {
        const conversation = this.conversations.find(c => c.id === conversationId);
        if (!conversation) return;

        const newTitle = prompt('Новое название беседы:', conversation.title);
        if (!newTitle || newTitle === conversation.title) return;

        try {
            await this.apiService.request(`/conversations/${conversationId}`, {
                method: 'PATCH',
                body: JSON.stringify({ title: newTitle })
            });

            // Update local state
            conversation.title = newTitle;

            // Re-render
            this.renderConversations();

            this.uiController.showSuccess('Беседа переименована');

        } catch (error) {
            console.error('Error renaming conversation:', error);
            this.uiController.showError('Ошибка переименования');
        }
    }

    async deleteConversation(conversationId) {
        if (!confirm('Удалить эту беседу? Это действие необратимо.')) {
            return;
        }

        try {
            await this.apiService.delete(`/conversations/${conversationId}`);

            // Remove from local state
            this.conversations = this.conversations.filter(c => c.id !== conversationId);

            // If it was current conversation, clear chat
            if (this.currentConversationId === conversationId) {
                this.currentConversationId = null;
                const chatMessages = document.getElementById('chatMessages');
                if (chatMessages) {
                    chatMessages.innerHTML = `
                        <div class="message assistant">
                            <div class="message-bubble">
                                Беседа удалена. Начните новую беседу!
                            </div>
                            <div class="message-time">Сейчас</div>
                        </div>
                    `;
                }
            }

            // Re-render
            this.renderConversations();

            this.uiController.showSuccess('Беседа удалена');

        } catch (error) {
            console.error('Error deleting conversation:', error);
            this.uiController.showError('Ошибка удаления беседы');
        }
    }

    showEmptyState(message) {
        const conversationsList = document.getElementById('conversationsList');
        if (!conversationsList) return;

        conversationsList.innerHTML = `
            <div class="empty-conversations">
                <svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                    <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
                </svg>
                <p>${message}</p>
            </div>
        `;
    }

    getCurrentConversationId() {
        return this.currentConversationId;
    }

    setCurrentConversationId(id) {
        this.currentConversationId = id;
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
}
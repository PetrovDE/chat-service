class ConversationsManager {
    constructor(apiService, uiController, chatManager) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.chatManager = chatManager;
        this.conversations = [];
        this.filteredConversations = [];
        this.searchTerm = '';
    }

    bindSearchInput() {
        const input = document.getElementById('conversationsSearch');
        if (!input) return;

        input.addEventListener('input', (event) => {
            this.searchTerm = event.target.value.trim().toLowerCase();
            this.filterConversations();
            this.renderConversations();
        });
    }

    filterConversations() {
        if (!this.searchTerm) {
            this.filteredConversations = [...this.conversations];
            return;
        }

        this.filteredConversations = this.conversations.filter((conversation) => {
            const title = (conversation.title || '').toLowerCase();
            return title.includes(this.searchTerm);
        });
    }

    renderLoadingSkeleton() {
        const container = document.getElementById('conversationsList');
        if (!container) return;

        container.innerHTML = Array.from({ length: 6 })
            .map(
                () => `
                    <div class="conversation-item skeleton-item" aria-hidden="true">
                        <div class="skeleton skeleton-title"></div>
                        <div class="skeleton skeleton-meta"></div>
                    </div>
                `
            )
            .join('');
    }

    async loadConversations() {
        this.renderLoadingSkeleton();

        try {
            const response = await this.apiService.getConversations();
            this.conversations = Array.isArray(response) ? response : [];
            this.filterConversations();
            this.renderConversations();
        } catch (error) {
            this.conversations = [];
            this.filteredConversations = [];
            this.renderConversations(error.message || 'Failed to load conversations');
        }
    }

    renderConversations(error = null) {
        const container = document.getElementById('conversationsList');
        if (!container) return;

        if (error) {
            container.innerHTML = `
                <div class="conversations-state">
                    <p>Could not load chats</p>
                    <button class="text-btn" id="retryConversationsBtn" type="button">Retry</button>
                </div>
            `;

            const retryBtn = document.getElementById('retryConversationsBtn');
            if (retryBtn) retryBtn.addEventListener('click', () => this.loadConversations());
            return;
        }

        if (this.filteredConversations.length === 0) {
            const message = this.searchTerm ? 'No chats found' : 'No conversations yet';
            container.innerHTML = `<div class="conversations-state">${message}</div>`;
            return;
        }

        const currentConversation = this.chatManager.getCurrentConversation();

        container.innerHTML = this.filteredConversations
            .map((conversation) => {
                const isActive = String(currentConversation || '') === String(conversation.id);
                const updatedAt = conversation.updated_at || conversation.created_at;
                return `
                    <div class="conversation-row">
                        <button
                            type="button"
                            class="conversation-item ${isActive ? 'active' : ''}"
                            data-conversation-id="${conversation.id}"
                            aria-current="${isActive ? 'true' : 'false'}"
                            aria-label="Open chat ${this.escapeHtml(conversation.title || 'Untitled')}"
                        >
                            <span class="conversation-title">${this.escapeHtml(conversation.title || 'Untitled')}</span>
                            <span class="conversation-date">${this.formatRelativeDate(updatedAt)}</span>
                        </button>
                        <button
                            type="button"
                            class="conversation-delete-btn"
                            data-action="delete-conversation"
                            data-conversation-id="${conversation.id}"
                            aria-label="Delete chat ${this.escapeHtml(conversation.title || 'Untitled')}"
                            title="Delete chat"
                        >
                            <svg viewBox="0 0 24 24" width="14" height="14" aria-hidden="true" focusable="false">
                                <path fill="currentColor" d="M9 3h6l1 2h4v2H4V5h4l1-2zm-3 6h12l-1 11a2 2 0 0 1-2 2H9a2 2 0 0 1-2-2L6 9zm4 2v8h2v-8h-2zm4 0v8h2v-8h-2z"/>
                            </svg>
                        </button>
                    </div>
                `;
            })
            .join('');

        container.querySelectorAll('.conversation-item').forEach((item) => {
            item.addEventListener('click', async () => {
                await this.loadConversation(item.dataset.conversationId);
            });

            item.addEventListener('keydown', async (event) => {
                if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault();
                    await this.loadConversation(item.dataset.conversationId);
                }
            });
        });

        container.querySelectorAll('[data-action="delete-conversation"]').forEach((btn) => {
            btn.addEventListener('click', async (event) => {
                event.preventDefault();
                event.stopPropagation();
                await this.deleteConversation(btn.dataset.conversationId);
            });
        });
    }

    async loadConversation(conversationId) {
        try {
            const messages = await this.apiService.getConversationMessages(conversationId);
            this.chatManager.setCurrentConversation(conversationId);
            const renderHistory = this.chatManager.renderConversationHistory || this.chatManager.renderConversationHistori;
            if (typeof renderHistory === 'function') {
                renderHistory.call(this.chatManager, messages);
            } else {
                throw new Error('Chat render method is unavailable');
            }

            if (window.app?.filesSidebarManager) {
                window.app.filesSidebarManager.setCurrentConversation(conversationId);
                window.app.filesSidebarManager.loadFiles(true);
            }

            this.renderConversations();
            document.body.classList.add('sidebar-collapsed-mobile');
        } catch (error) {
            this.uiController.showError(error.message || 'Failed to load messages');
        }
    }

    createNewConversation() {
        this.chatManager.setCurrentConversation(null);
        const renderWelcome = this.chatManager.renderWelcomeState || this.chatManager.renderWelcome;
        if (typeof renderWelcome === 'function') {
            renderWelcome.call(this.chatManager);
        } else {
            const chatMessages = document.getElementById('chatMessages');
            if (chatMessages) {
                chatMessages.innerHTML = `
                    <section class="chat-empty-state" aria-live="polite">
                        <h2>Start a new conversation</h2>
                        <p>Ask anything or attach a file to work with RAG context.</p>
                    </section>
                `;
            }
        }
        this.renderConversations();

        if (window.app?.filesSidebarManager) {
            window.app.filesSidebarManager.setCurrentConversation(null);
            window.app.filesSidebarManager.loadFiles(true);
        }
    }

    async deleteConversation(conversationId) {
        const target = this.conversations.find((conversation) => String(conversation.id) === String(conversationId));
        if (!target) {
            this.uiController.showError('Conversation not found');
            return;
        }

        const confirmed = confirm(`Delete chat "${target.title || 'Untitled'}"? This action cannot be undone.`);
        if (!confirmed) return;

        try {
            this.uiController.showLoading('Deleting chat...');
            await this.apiService.deleteConversation(conversationId);
            this.uiController.hideLoading();
            this.uiController.showSuccess('Chat deleted');

            const currentConversation = this.chatManager.getCurrentConversation();
            if (String(currentConversation || '') === String(conversationId)) {
                this.createNewConversation();
            }

            await this.loadConversations();
            if (window.app?.filesSidebarManager) {
                await window.app.filesSidebarManager.loadFiles(true);
            }
        } catch (error) {
            this.uiController.hideLoading();
            this.uiController.showError(error.message || 'Failed to delete chat');
        }
    }

    escapeHtml(value) {
        const div = document.createElement('div');
        div.textContent = value;
        return div.innerHTML;
    }

    formatRelativeDate(dateValue) {
        if (!dateValue) return '';

        const date = new Date(dateValue);
        const now = new Date();
        const dayDiff = Math.floor((now - date) / 86400000);

        if (dayDiff <= 0) return 'Today';
        if (dayDiff === 1) return 'Yesterday';
        if (dayDiff < 7) return `${dayDiff}d ago`;

        return date.toLocaleDateString();
    }
}

export { ConversationsManager };

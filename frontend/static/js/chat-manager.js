import { formatMessage } from './formatters.js';

class ChatManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.currentConversation = null;
        this.isGenerating = false;
        this.abortController = null;
        this.conversationsManager = null;
        this.lastRenderedDate = null;
        this.autoScrollObserver = null;
        this.initAutoScroll();
    }

    setConversationsManager(conversationsManager) {
        this.conversationsManager = conversationsManager;
    }

    getCurrentConversation() {
        return this.currentConversation;
    }

    setCurrentConversation(id) {
        this.currentConversation = id;
        this.lastRenderedDate = null;
    }

    renderWelcomeState() {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return;

        chatMessages.innerHTML = `
            <section class="chat-empty-state" aria-live="polite">
                <h2>Start a new conversation</h2>
                <p>Ask anything or attach a file to work with RAG context.</p>
            </section>
        `;
        this.lastRenderedDate = null;
    }

    renderConversationHistory(messages) {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return;

        chatMessages.innerHTML = '';
        this.lastRenderedDate = null;

        if (!messages || messages.length === 0) {
            this.renderWelcomeState();
            return;
        }

        messages.forEach((message) => {
            this.addMessageToUI(message.role, message.content, message.timestamp);
        });

        this.scrollToBottom();
    }

    async sendMessage(message, conversationId, settings) {
        if (this.isGenerating) return;

        const normalizedMessage = (message || '').trim();
        if (!normalizedMessage) return;

        try {
            this.isGenerating = true;
            this.showGenerating(true);

            this.addMessageToUI('user', normalizedMessage, new Date().toISOString());

            const attachedFiles = window.app?.fileManager?.getAttachedFiles?.() || [];
            const fileIds = attachedFiles.map((file) => file.id);
            const ragMode = this.inferRagMode(normalizedMessage, fileIds);

            const payload = {
                message: normalizedMessage,
                conversation_id: conversationId || null,
                model_source: settings.mode || 'local',
                model_name: settings.model || 'llama3',
                temperature: settings.temperature || 0.7,
                max_tokens: settings.max_tokens || 2048,
                prompt_max_chars: settings.prompt_max_chars || null,
                file_ids: fileIds,
                rag_mode: ragMode,
            };

            await this.streamResponse(payload);

            if (window.app?.fileManager?.clearAttachedFiles) {
                window.app.fileManager.clearAttachedFiles();
            }
        } catch (error) {
            this.addMessageToUI('assistant', `Error: ${error.message || 'Failed to send message'}`, new Date().toISOString());
            throw error;
        } finally {
            this.isGenerating = false;
            this.showGenerating(false);
        }
    }

    async streamResponse(payload) {
        this.abortController = new AbortController();
        const wasNewConversation = !payload.conversation_id;
        let newConversationId = null;

        const assistantMessageDiv = this.createAssistantMessageElement();
        if (!assistantMessageDiv) {
            throw new Error('Chat container not found');
        }

        const assistantBubble = assistantMessageDiv.querySelector('.message-bubble');
        const response = await this.apiService.streamChat(payload, this.abortController.signal);

        const reader = response.body?.getReader();
        if (!reader) {
            throw new Error('Streaming not supported by this browser');
        }

        const decoder = new TextDecoder();
        let buffer = '';
        let rawResponseText = '';

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n');
            buffer = lines.pop() || '';

            for (const line of lines) {
                if (!line.startsWith('data: ')) continue;
                const data = line.slice(6);
                if (data === '[DONE]') continue;

                let chunk;
                try {
                    chunk = JSON.parse(data);
                } catch (_) {
                    continue;
                }

                if (chunk.type === 'start' && chunk.conversation_id) {
                    newConversationId = chunk.conversation_id;
                    this.setCurrentConversation(chunk.conversation_id);
                    if (window.app?.filesSidebarManager) {
                        window.app.filesSidebarManager.setCurrentConversation(chunk.conversation_id);
                    }
                }

                if (chunk.type === 'chunk' && chunk.content) {
                    rawResponseText += chunk.content;
                    assistantBubble.textContent = rawResponseText;
                    this.scrollToBottom();
                }

                if (chunk.type === 'final_refinement' && chunk.content) {
                    rawResponseText = chunk.content;
                    assistantBubble.innerHTML = formatMessage(rawResponseText);
                    this.scrollToBottom();
                }

                if (chunk.type === 'done') {
                    if (chunk.content) {
                        rawResponseText = chunk.content;
                    }
                    assistantBubble.innerHTML = formatMessage(rawResponseText);
                    this.scrollToBottom();

                    if (wasNewConversation && newConversationId && this.conversationsManager) {
                        this.conversationsManager.loadConversations();
                    }
                }

                if (chunk.type === 'error') {
                    throw new Error(chunk.message || 'Streaming failed');
                }
            }
        }

        assistantBubble.innerHTML = formatMessage(rawResponseText);

        this.scrollToBottom();
        this.abortController = null;
    }

    createAssistantMessageElement() {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return null;

        const emptyState = chatMessages.querySelector('.chat-empty-state');
        if (emptyState) emptyState.remove();

        this.ensureDateDivider(new Date().toISOString());

        const messageDiv = document.createElement('article');
        messageDiv.className = 'message assistant';
        messageDiv.innerHTML = `
            <div class="message-bubble"></div>
            <time class="message-time">${new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</time>
        `;

        chatMessages.appendChild(messageDiv);
        return messageDiv;
    }

    addMessageToUI(role, content, timestamp = null) {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return;

        const emptyState = chatMessages.querySelector('.chat-empty-state');
        if (emptyState) emptyState.remove();

        const messageTime = timestamp || new Date().toISOString();
        this.ensureDateDivider(messageTime);

        const messageDiv = document.createElement('article');
        messageDiv.className = `message ${role}`;

        const html = role === 'assistant' ? formatMessage(content) : this.escapeText(content).replace(/\n/g, '<br>');
        const timeLabel = new Date(messageTime).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

        messageDiv.innerHTML = `
            <div class="message-bubble">${html}</div>
            <time class="message-time">${timeLabel}</time>
        `;

        chatMessages.appendChild(messageDiv);
        this.scrollToBottom();
    }

    ensureDateDivider(timestamp) {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return;

        const date = new Date(timestamp);
        if (Number.isNaN(date.getTime())) return;

        const dateKey = date.toISOString().slice(0, 10);
        if (dateKey === this.lastRenderedDate) return;

        this.lastRenderedDate = dateKey;

        const divider = document.createElement('div');
        divider.className = 'date-divider';

        const today = new Date();
        const isToday = today.toDateString() === date.toDateString();
        divider.textContent = isToday ? 'Today' : date.toLocaleDateString();

        chatMessages.appendChild(divider);
    }

    showGenerating(show) {
        const sendBtn = document.getElementById('sendMessage');
        const stopBtn = document.getElementById('stopGeneration');

        if (sendBtn) {
            sendBtn.disabled = show;
            sendBtn.style.display = show ? 'none' : 'inline-flex';
        }
        if (stopBtn) {
            stopBtn.style.display = show ? 'inline-flex' : 'none';
            stopBtn.classList.toggle('is-generating', show);
        }
    }

    stopGeneration() {
        if (this.abortController) {
            this.abortController.abort();
            this.abortController = null;
        }

        this.isGenerating = false;
        this.showGenerating(false);
    }

    inferRagMode(message, fileIds) {
        if (!Array.isArray(fileIds) || fileIds.length === 0) {
            return 'auto';
        }

        const text = (message || '').toLowerCase();
        const fullFileHints = [
            'whole file',
            'full file',
            'all rows',
            'analyze file',
            'summarize the file',
            'ves fail',
            'po vsemu failu',
            'proanaliziruy fail',
        ];

        return fullFileHints.some((hint) => text.includes(hint)) ? 'full_file' : 'auto';
    }

    escapeText(text) {
        return String(text)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
    }

    scrollToBottom() {
        const scrollContainer = this.getScrollContainer();
        if (!scrollContainer) return;

        scrollContainer.scrollTop = scrollContainer.scrollHeight;
        requestAnimationFrame(() => {
            scrollContainer.scrollTop = scrollContainer.scrollHeight;
        });
    }

    initAutoScroll() {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages || this.autoScrollObserver) return;

        this.autoScrollObserver = new MutationObserver(() => {
            this.scrollToBottom();
        });

        this.autoScrollObserver.observe(chatMessages, {
            childList: true,
            subtree: true,
            characterData: true,
        });
    }

    getScrollContainer() {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return null;

        return chatMessages.closest('.chat-container') || chatMessages;
    }
}

export { ChatManager };

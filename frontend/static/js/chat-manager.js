import { formatMessage } from './formatters.js';
import {
    formatMessageDateLabel,
    formatMessageTimeLabel,
    nowTimestampISO,
    parseAppTimestamp,
    toLocalDateKey,
} from './time-format.js';

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

    // Backward-compatible alias for mixed cached frontend bundles.
    renderConversationHistori(messages) {
        this.renderConversationHistory(messages);
    }

    async sendMessage(message, conversationId, settings) {
        if (this.isGenerating) return;

        const normalizedMessage = (message || '').trim();
        if (!normalizedMessage) return;

        try {
            this.isGenerating = true;
            this.showGenerating(true);

            this.addMessageToUI('user', normalizedMessage, nowTimestampISO());

            const attachedFiles = window.app?.fileManager?.getAttachedFiles?.() || [];
            const fileIds = attachedFiles.map((file) => file.id);
            const ragMode = this.resolveRagMode(normalizedMessage, fileIds);

            const payload = {
                message: normalizedMessage,
                conversation_id: conversationId || null,
                model_source: settings.mode || 'local',
                provider_mode: (settings.mode === 'local' || settings.mode === 'ollama' || settings.mode === 'openai')
                    ? 'explicit'
                    : 'policy',
                model_name: settings.model || 'llama3',
                temperature: settings.temperature || 0.7,
                max_tokens: settings.max_tokens || 2048,
                prompt_max_chars: settings.prompt_max_chars || null,
                file_ids: fileIds,
                rag_mode: ragMode,
                rag_debug: Boolean(settings.rag_debug),
            };

            await this.streamResponse(payload);

            if (window.app?.fileManager?.clearAttachedFiles) {
                window.app.fileManager.clearAttachedFiles();
            }
        } catch (error) {
            this.addMessageToUI('assistant', `Error: ${error.message || 'Failed to send message'}`, nowTimestampISO());
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
        const showRagDebug = Boolean(payload?.rag_debug);
        let ragDebugPayload = null;
        let ragSources = [];
        let artifacts = [];

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

                if (showRagDebug && chunk.type === 'start' && chunk.rag_debug) {
                    ragDebugPayload = chunk.rag_debug;
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
                    if (Array.isArray(chunk.sources)) {
                        ragSources = chunk.sources;
                    }
                    if (Array.isArray(chunk.artifacts)) {
                        artifacts = chunk.artifacts;
                    }
                    assistantBubble.innerHTML = formatMessage(rawResponseText);
                    this.renderAssistantMeta(assistantMessageDiv, {
                        sources: ragSources,
                        artifacts,
                        ragDebug: showRagDebug ? ragDebugPayload : null,
                    });
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
        this.renderAssistantMeta(assistantMessageDiv, {
            sources: ragSources,
            artifacts,
            ragDebug: showRagDebug ? ragDebugPayload : null,
        });

        this.scrollToBottom();
        this.abortController = null;
    }

    createAssistantMessageElement() {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return null;

        const emptyState = chatMessages.querySelector('.chat-empty-state');
        if (emptyState) emptyState.remove();

        const nowIso = nowTimestampISO();
        this.ensureDateDivider(nowIso);

        const messageDiv = document.createElement('article');
        messageDiv.className = 'message assistant';
        messageDiv.innerHTML = `
            <div class="message-bubble"></div>
            <div class="message-meta"></div>
            <time class="message-time">${formatMessageTimeLabel(nowIso)}</time>
        `;

        chatMessages.appendChild(messageDiv);
        return messageDiv;
    }

    addMessageToUI(role, content, timestamp = null) {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return;

        const emptyState = chatMessages.querySelector('.chat-empty-state');
        if (emptyState) emptyState.remove();

        const messageTime = timestamp || nowTimestampISO();
        this.ensureDateDivider(messageTime);

        const messageDiv = document.createElement('article');
        messageDiv.className = `message ${role}`;

        const html = role === 'assistant' ? formatMessage(content) : this.escapeText(content).replace(/\n/g, '<br>');
        const timeLabel = formatMessageTimeLabel(messageTime);

        messageDiv.innerHTML = `
            <div class="message-bubble">${html}</div>
            <div class="message-meta"></div>
            <time class="message-time">${timeLabel}</time>
        `;

        chatMessages.appendChild(messageDiv);
        this.scrollToBottom();
    }

    renderAssistantMeta(messageDiv, { sources = [], artifacts = [], ragDebug = null } = {}) {
        const metaContainer = messageDiv?.querySelector('.message-meta');
        if (!metaContainer) return;

        const blocks = [];
        if (Array.isArray(artifacts) && artifacts.length > 0) {
            const cards = artifacts.slice(0, 8).map((artifact) => {
                const name = this.escapeText(String(artifact?.name || artifact?.kind || 'artifact'));
                const kind = this.escapeText(String(artifact?.kind || 'chart'));
                const url = String(artifact?.url || '').trim();
                if (!url) {
                    return '';
                }
                const safeUrl = this.escapeText(url);
                return `
                    <a class="artifact-card" href="${safeUrl}" target="_blank" rel="noopener noreferrer">
                        <img src="${safeUrl}" alt="${name}" loading="lazy" />
                        <span>${kind}: ${name}</span>
                    </a>
                `;
            }).filter(Boolean).join('');

            if (cards) {
                blocks.push(`
                    <details class="assistant-meta-block" open>
                        <summary>Charts (${artifacts.length})</summary>
                        <div class="artifact-gallery">${cards}</div>
                    </details>
                `);
            }
        }

        if (Array.isArray(sources) && sources.length > 0) {
            const list = sources.slice(0, 12).map((src) => `<li>${this.escapeText(src)}</li>`).join('');
            blocks.push(`
                <details class="assistant-meta-block">
                    <summary>Sources (${sources.length})</summary>
                    <ul>${list}</ul>
                </details>
            `);
        }

        if (ragDebug && typeof ragDebug === 'object') {
            const debugJson = this.escapeText(JSON.stringify(ragDebug, null, 2));
            blocks.push(`
                <details class="assistant-meta-block">
                    <summary>RAG debug</summary>
                    <pre><code>${debugJson}</code></pre>
                </details>
            `);
        }

        metaContainer.innerHTML = blocks.join('');
    }

    ensureDateDivider(timestamp) {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return;

        const date = parseAppTimestamp(timestamp);
        if (!date || Number.isNaN(date.getTime())) return;

        const dateKey = toLocalDateKey(date);
        if (dateKey === this.lastRenderedDate) return;

        this.lastRenderedDate = dateKey;

        const divider = document.createElement('div');
        divider.className = 'date-divider';
        divider.textContent = formatMessageDateLabel(date);

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

    resolveRagMode(message, fileIds) {
        const selectorValue = document.getElementById('ragModeSelector')?.value || 'auto';
        if (selectorValue === 'hybrid' || selectorValue === 'full_file') {
            return selectorValue;
        }
        return this.inferRagMode(message, fileIds);
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

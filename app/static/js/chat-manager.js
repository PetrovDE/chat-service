// Chat management functionality
import { formatTime } from './utils.js';

export class ChatManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.conversationHistory = [];
        this.isGenerating = false;
        this.streamingEnabled = true;
    }

    async sendMessage() {
        const messageInput = document.getElementById('messageInput');
        const message = messageInput.value.trim();

        if (!message || this.isGenerating) return;

        // Add user message to chat
        this.addMessage('user', message);

        // Clear input and disable sending
        messageInput.value = '';
        messageInput.style.height = 'auto';
        this.setGenerating(true);

        try {
            if (this.streamingEnabled) {
                await this.sendStreamingMessage(message);
            } else {
                await this.sendNormalMessage(message);
            }
        } catch (error) {
            console.error('Error sending message:', error);
            this.uiController.showError(`Error: ${error.message}`);
        } finally {
            this.setGenerating(false);
        }
    }

    async sendStreamingMessage(message) {
        const requestData = {
            message: message,
            conversation_history: this.conversationHistory,
            temperature: parseFloat(document.getElementById('temperatureInput').value),
            max_tokens: parseInt(document.getElementById('maxTokensInput').value)
        };

        // Create streaming response container
        const streamingDiv = this.createStreamingMessage();
        let fullResponse = '';

        try {
            const response = await this.apiService.streamChat(requestData);
            const reader = response.body.getReader();
            const decoder = new TextDecoder();

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                const chunk = decoder.decode(value);
                const lines = chunk.split('\n');

                for (const line of lines) {
                    if (line.startsWith('data: ')) {
                        try {
                            const data = JSON.parse(line.slice(6));

                            if (data.type === 'token') {
                                fullResponse += data.content;
                                this.updateStreamingMessage(streamingDiv, fullResponse);
                            } else if (data.type === 'done') {
                                this.finalizeStreamingMessage(streamingDiv, data.content);
                                // Add to conversation history
                                this.conversationHistory.push({
                                    role: 'assistant',
                                    content: data.content,
                                    timestamp: new Date().toISOString()
                                });
                            } else if (data.type === 'error') {
                                this.uiController.showError(data.content);
                                streamingDiv.remove();
                            }
                        } catch (e) {
                            // Ignore JSON parse errors for partial chunks
                        }
                    }
                }
            }
        } catch (error) {
            streamingDiv.remove();
            throw error;
        }
    }

    async sendNormalMessage(message) {
        const requestData = {
            message: message,
            conversation_history: this.conversationHistory,
            temperature: parseFloat(document.getElementById('temperatureInput').value),
            max_tokens: parseInt(document.getElementById('maxTokensInput').value)
        };

        const response = await this.apiService.sendChat(requestData);
        this.addMessage('assistant', response.response);
    }

    createStreamingMessage() {
        const chatMessages = document.getElementById('chatMessages');
        const messageDiv = document.createElement('div');
        messageDiv.className = 'message assistant';

        messageDiv.innerHTML = `
            <div class="message-bubble">
                <span class="streaming-text"></span>
                <span class="cursor">|</span>
            </div>
            <div class="message-time">${formatTime(new Date())}</div>
        `;

        chatMessages.appendChild(messageDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;

        // Add blinking cursor
        const cursor = messageDiv.querySelector('.cursor');
        const blinkInterval = setInterval(() => {
            cursor.style.opacity = cursor.style.opacity === '0' ? '1' : '0';
        }, 500);

        // Store interval ID for cleanup
        messageDiv.setAttribute('data-blink-interval', blinkInterval);

        return messageDiv;
    }

    updateStreamingMessage(streamingDiv, content) {
        const textSpan = streamingDiv.querySelector('.streaming-text');
        textSpan.textContent = content;

        const chatMessages = document.getElementById('chatMessages');
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    finalizeStreamingMessage(streamingDiv, finalContent) {
        const intervalId = streamingDiv.getAttribute('data-blink-interval');
        if (intervalId) {
            clearInterval(parseInt(intervalId));
            streamingDiv.removeAttribute('data-blink-interval');
        }

        const textSpan = streamingDiv.querySelector('.streaming-text');
        const cursor = streamingDiv.querySelector('.cursor');

        textSpan.textContent = finalContent;
        if (cursor) {
            cursor.remove();
        }
    }

    addMessage(role, content) {
        const chatMessages = document.getElementById('chatMessages');
        const now = new Date();

        // Create message object for history
        const messageObj = {
            role: role,
            content: content,
            timestamp: now.toISOString()
        };

        // Add to conversation history
        this.conversationHistory.push(messageObj);

        // Create DOM element
        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${role}`;

        // Handle markdown-style formatting
        const formattedContent = content
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\n/g, '<br>');

        messageDiv.innerHTML = `
            <div class="message-bubble">${formattedContent}</div>
            <div class="message-time">${formatTime(now)}</div>
        `;

        chatMessages.appendChild(messageDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    setGenerating(generating) {
        this.isGenerating = generating;
        const sendButton = document.getElementById('sendButton');
        const messageInput = document.getElementById('messageInput');

        sendButton.disabled = generating;
        messageInput.disabled = generating;
    }

    setStreamingEnabled(enabled) {
        this.streamingEnabled = enabled;
    }

    clearChat() {
        if (confirm('Are you sure you want to clear the chat history?')) {
            this.conversationHistory = [];
            const chatMessages = document.getElementById('chatMessages');
            chatMessages.innerHTML = `
                <div class="message assistant">
                    <div class="message-bubble">
                        Hello! I'm Llama 3.1 8B. How can I help you today? You can chat with me or upload files for analysis!
                    </div>
                    <div class="message-time">Just now</div>
                </div>
            `;
        }
    }

    getConversationHistory() {
        return this.conversationHistory;
    }
}
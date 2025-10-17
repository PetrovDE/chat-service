// app/static/js/chat-manager.js
export class ChatManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.currentConversationId = null;
        this.isGenerating = false;
    }

    async sendMessage(message, conversationId = null) {
        if (this.isGenerating) {
            this.uiController.showError('Пожалуйста, дождитесь завершения генерации');
            return;
        }

        this.isGenerating = true;
        const sendButton = document.getElementById('sendMessage');
        const messageInput = document.getElementById('messageInput');

        try {
            // Добавить сообщение пользователя в UI
            this.uiController.addMessage('user', message);

            // Очистить input
            if (messageInput) {
                messageInput.value = '';
                messageInput.style.height = 'auto';
            }

            // Отключить кнопку отправки
            if (sendButton) {
                sendButton.disabled = true;
                sendButton.textContent = 'Генерация...';
            }

            // Создать placeholder для ответа ассистента
            const assistantMessageId = this.uiController.addMessage('assistant', '', true);

            // Использовать EventSource для SSE
            await this.streamResponse(message, conversationId, assistantMessageId);

        } catch (error) {
            console.error('Error sending message:', error);
            this.uiController.showError('Ошибка отправки сообщения');
        } finally {
            this.isGenerating = false;
            if (sendButton) {
                sendButton.disabled = false;
                sendButton.textContent = 'Отправить';
            }
        }
    }

    async streamResponse(message, conversationId, assistantMessageId) {
        return new Promise((resolve, reject) => {
            const token = localStorage.getItem('token');
            const url = new URL('/chat/stream', window.location.origin);

            // EventSource не поддерживает custom headers и POST,
            // поэтому используем fetch с ReadableStream
            this.fetchStreamingResponse(message, conversationId, assistantMessageId)
                .then(resolve)
                .catch(reject);
        });
    }

    async fetchStreamingResponse(message, conversationId, assistantMessageId) {
        const token = localStorage.getItem('token');

        const payload = {
            message: message,
            conversation_id: conversationId
        };

        const response = await fetch('/chat/stream', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                ...(token && { 'Authorization': `Bearer ${token}` })
            },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let fullResponse = '';
        let buffer = '';

        while (true) {
            const { done, value } = await reader.read();

            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n\n');
            buffer = lines.pop(); // Сохранить неполную строку

            for (const line of lines) {
                if (line.startsWith('data: ')) {
                    const data = JSON.parse(line.substring(6));

                    switch (data.type) {
                        case 'start':
                            // Сохранить conversation_id
                            if (data.conversation_id) {
                                this.currentConversationId = data.conversation_id;
                            }
                            break;

                        case 'chunk':
                            // Добавить chunk к ответу
                            fullResponse += data.content;
                            this.uiController.updateMessageContent(
                                assistantMessageId,
                                fullResponse
                            );
                            break;

                        case 'done':
                            // Генерация завершена
                            console.log(`Generation completed in ${data.generation_time}s`);
                            this.uiController.finalizeMessage(assistantMessageId);

                            // Обновить sidebar с беседами
                            if (window.app && window.app.conversationsManager) {
                                window.app.conversationsManager.loadConversations();
                            }
                            break;

                        case 'error':
                            this.uiController.showError(data.message);
                            throw new Error(data.message);
                    }
                }
            }
        }
    }

    setCurrentConversation(conversationId) {
        this.currentConversationId = conversationId;
    }

    getCurrentConversation() {
        return this.currentConversationId;
    }

    clearCurrentConversation() {
        this.currentConversationId = null;
        this.uiController.clearMessages();
    }
}
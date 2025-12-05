// frontend/static/js/chat-manager.js
// Импортируем функции для форматирования
import { formatMessage } from './formatters.js';

class ChatManager {
  constructor(apiService, uiController) {
    this.apiService = apiService;
    this.uiController = uiController;
    this.currentConversation = null;
    this.isGenerating = false;
    this.abortController = null;
    this.conversationsManager = null;
    console.log('✓ ChatManager initialized');
  }

  setConversationsManager(conversationsManager) {
    this.conversationsManager = conversationsManager;
    console.log('✓ ConversationsManager linked to ChatManager');
  }

  async sendMessage(message, conversationId, settings) {
    console.log('📤 Sending message:', message);

    if (this.isGenerating) {
      console.warn('⚠️ Already generating, please wait');
      return;
    }

    try {
      this.isGenerating = true;
      this.showGenerating(true);

      // Add user message to UI
      this.addMessageToUI('user', message);

      // Prepare request with correct mapping
      const modelSource = settings.mode || 'local';
      console.log('🔌 Model source:', modelSource); // Debug

      // НОВОЕ: Получаем file_ids из FileManager
      const fileIds = [];
      if (window.app?.fileManager?.getAttachedFiles) {
        const attachedFiles = window.app.fileManager.getAttachedFiles();
        fileIds.push(...attachedFiles.map(f => f.id));
        if (fileIds.length > 0) {
          console.log('📎 Attached files:', fileIds);
        }
      }

      const payload = {
        message: message,
        conversation_id: conversationId || null,
        model_source: modelSource,
        model_name: settings.model || 'llama3',
        temperature: settings.temperature || 0.7,
        max_tokens: settings.max_tokens || 2048,
        file_ids: fileIds  // НОВОЕ: Добавляем file_ids если есть
      };

      console.log('📡 Request payload:', payload);

      // Send to streaming endpoint
      await this.streamResponse(payload);

      // НОВОЕ: После успешной отправки, очищаем прикрепленные файлы
      if (window.app?.fileManager?.clearAttachedFiles) {
        window.app.fileManager.clearAttachedFiles();
      }

      return { success: true };
    } catch (error) {
      console.error('❌ Send message error:', error);
      this.isGenerating = false;
      this.showGenerating(false);
      this.addMessageToUI('assistant', `Ошибка: ${error.message}`);
      throw error;
    }
  }

  async streamResponse(payload) {
    this.abortController = new AbortController();
    const wasNewConversation = !payload.conversation_id;
    let newConversationId = null;

    try {
      // ИСПРАВЛЕНО: Добавляем токен авторизации
      const token = localStorage.getItem('auth_token');
      const headers = {
        'Content-Type': 'application/json',
      };

      // Добавляем токен если он есть
      if (token) {
        headers['Authorization'] = `Bearer ${token}`;
      }

      const response = await fetch(`${this.apiService.baseURL}/chat/stream`, {
        method: 'POST',
        headers: headers,
        body: JSON.stringify(payload),
        signal: this.abortController.signal
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let assistantMessageDiv = null;
      let assistantBubble = null;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (!line.trim() || !line.startsWith('data: ')) continue;

          const data = line.slice(6);
          if (data === '[DONE]') continue;

          try {
            const chunk = JSON.parse(data);

            if (chunk.type === 'start') {
              console.log('🔄 Stream started');
              if (chunk.conversation_id) {
                newConversationId = chunk.conversation_id;
                this.setCurrentConversation(chunk.conversation_id);
                console.log('✅ Conversation ID set:', chunk.conversation_id);

                // НОВОЕ: Если было загружено файлов БЕЗ conversation_id,
                // они уже были отправлены в payload и связаны с conversation_id на backend
              }

              // Create assistant message element
              assistantMessageDiv = this.createAssistantMessageElement();
              assistantBubble = assistantMessageDiv.querySelector('.message-bubble');
            }
            // ===== НАКАПЛИВАЕМ ТЕКСТ, НЕ ФОРМАТИРУЯ =====
            else if (chunk.type === 'chunk' && chunk.content) {
              if (assistantBubble) {
                // Добавляем текст как есть (без HTML)
                assistantBubble.textContent += chunk.content;
                this.scrollToBottom();
              }
            }
            // ===== ФОРМАТИРУЕМ ВЕСЬ ТЕКСТ ОДИН РАЗ КОГДА ГОТОВО =====
            else if (chunk.type === 'done') {
              console.log('✅ Stream completed');
              this.isGenerating = false;
              this.showGenerating(false);

              // ФОРМАТИРУЕМ MARKDOWN И КОД ПОСЛЕ ПОЛУЧЕНИЯ ВСЕХ ДАННЫХ
              if (assistantBubble) {
                const rawText = assistantBubble.textContent;
                try {
                  assistantBubble.innerHTML = formatMessage(rawText);
                } catch (e) {
                  console.error('❌ Error formatting message:', e);
                  // Если ошибка, оставляем как текст
                }
              }

              // Обновляем список разговоров если был создан новый
              if (wasNewConversation && newConversationId && this.conversationsManager) {
                console.log('🔄 Reloading conversations list after creating new conversation');
                setTimeout(() => {
                  this.conversationsManager.loadConversations();
                }, 300);
              }
            } else if (chunk.type === 'error') {
              console.error('❌ Stream error:', chunk.message);
              throw new Error(chunk.message || 'Stream error');
            }
          } catch (parseError) {
            console.error('Parse error:', parseError, 'Line:', data);
          }
        }
      }
    } catch (error) {
      console.error('❌ Stream error:', error);
      throw error;
    } finally {
      this.abortController = null;
    }
  }

  createAssistantMessageElement() {
    const chatMessages = document.getElementById('chatMessages');
    if (!chatMessages) return null;

    const welcome = chatMessages.querySelector('[style*="text-align: center"]');
    if (welcome) {
      welcome.remove();
    }

    const messageDiv = document.createElement('div');
    messageDiv.className = 'message assistant';
    messageDiv.innerHTML = `
      <div class="message-bubble"></div>
      <div class="message-time">${new Date().toLocaleTimeString()}</div>
    `;
    chatMessages.appendChild(messageDiv);
    return messageDiv;
  }

  // ===== НОВЫЙ МЕТОД: Для загрузки сохраненных сообщений с форматированием =====
  addFormattedMessageToUI(role, content) {
    const chatMessages = document.getElementById('chatMessages');
    if (!chatMessages) return;

    const welcome = chatMessages.querySelector('[style*="text-align: center"]');
    if (welcome) {
      welcome.remove();
    }

    const messageDiv = document.createElement('div');
    messageDiv.className = `message ${role}`;

    let formattedContent;
    if (role === 'assistant') {
      // Для ответов ассистента - используем полное форматирование markdown
      try {
        formattedContent = formatMessage(content);
      } catch (e) {
        console.error('❌ Error formatting assistant message:', e);
        formattedContent = this.formatMessage(content);
      }
    } else {
      // Для пользовательских сообщений - простое форматирование
      formattedContent = this.formatMessage(content);
    }

    messageDiv.innerHTML = `
      <div class="message-bubble">${formattedContent}</div>
      <div class="message-time">${new Date().toLocaleTimeString()}</div>
    `;
    chatMessages.appendChild(messageDiv);
    chatMessages.scrollTop = chatMessages.scrollHeight;
  }

  // ===== СТАРЫЙ МЕТОД: для новых сообщений (используется при отправке) =====
  addMessageToUI(role, content) {
    const chatMessages = document.getElementById('chatMessages');
    if (!chatMessages) return;

    const welcome = chatMessages.querySelector('[style*="text-align: center"]');
    if (welcome) {
      welcome.remove();
    }

    const messageDiv = document.createElement('div');
    messageDiv.className = `message ${role}`;
    messageDiv.innerHTML = `
      <div class="message-bubble">${this.formatMessage(content)}</div>
      <div class="message-time">${new Date().toLocaleTimeString()}</div>
    `;
    chatMessages.appendChild(messageDiv);
    chatMessages.scrollTop = chatMessages.scrollHeight;
  }

  formatMessage(text) {
    // Базовое форматирование для текстовых сообщений (без markdown)
    return text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/\n/g, '<br>')
      .replace(/\t/g, '&nbsp;&nbsp;&nbsp;&nbsp;');
  }

  showGenerating(show) {
    const sendBtn = document.getElementById('sendMessage');
    const stopBtn = document.getElementById('stopGeneration');

    if (show) {
      if (sendBtn) sendBtn.style.display = 'none';
      if (stopBtn) stopBtn.style.display = 'block';
    } else {
      if (sendBtn) sendBtn.style.display = 'block';
      if (stopBtn) stopBtn.style.display = 'none';
    }
  }

  getCurrentConversation() {
    return this.currentConversation;
  }

  setCurrentConversation(id) {
    this.currentConversation = id;
    console.log('✓ Current conversation set:', id);
  }

  stopGeneration() {
    console.log('⏹️ Stopping generation');
    this.isGenerating = false;
    this.showGenerating(false);

    if (this.abortController) {
      this.abortController.abort();
      this.abortController = null;
    }
  }

  scrollToBottom() {
    const chatMessages = document.getElementById('chatMessages');
    if (chatMessages) {
      chatMessages.scrollTop = chatMessages.scrollHeight;
    }
  }
}

export { ChatManager };

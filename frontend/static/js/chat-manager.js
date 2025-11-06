import settingsManager from './settings-manager.js';

class ChatManager {
  constructor() {
    this.conversationId = null;
    this.currentMode = settingsManager.mode;
    this.currentModel = settingsManager.model;
    settingsManager.onChange((mode, model) => {
      this.currentMode = mode;
      this.currentModel = model;
    });
  }

  async startConversation(title = 'Новая беседа') {
    const resp = await fetch('/api/conversations/new', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title,
        model_source: this.currentMode,
        model_name: this.currentModel
      }),
    });
    if (!resp.ok) throw new Error('Failed to start conversation');
    const { conversation_id } = await resp.json();
    this.conversationId = conversation_id;
    return conversation_id;
  }

  async sendMessage(prompt) {
    const resp = await fetch('/api/chat/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        conversation_id: this.conversationId,
        prompt,
        model_source: this.currentMode,
        model_name: this.currentModel
      }),
    });
    if (!resp.ok) throw new Error('Failed to send message');
    return await resp.json();
  }

  async getHistory(limit = 50) {
    if (!this.conversationId) return [];
    const resp = await fetch(`/api/chat/history?conversation_id=${this.conversationId}&limit=${limit}`);
    if (!resp.ok) throw new Error('Failed to load history');
    return await resp.json();
  }

  setConversationId(conversationId) {
    this.conversationId = conversationId;
  }
}

const chatManager = new ChatManager();
export default chatManager;

import settingsManager from './settings-manager.js';
import chatManager from './chat-manager.js';
import fileManager from './file-manager.js';

class UIController {
  constructor() {
    this.modeSelector = document.getElementById('mode-selector');
    this.modelSelector = document.getElementById('model-selector');
    this.sendButton = document.getElementById('send-btn');
    this.promptInput = document.getElementById('prompt');
    this.chatHistory = document.getElementById('chat-history');

    // Инициализация UI
    this._bindModeSelector();
    this._bindModelSelector();
    this._bindSendButton();

    settingsManager.onChange((mode, model, models) => {
      this._updateModelSelector(models, model);
      this._updateModeSelector(mode);
    });

    // Auto-populate selectors on load
    this._updateModelSelector(settingsManager.models, settingsManager.model);
    this._updateModeSelector(settingsManager.mode);
  }

  _bindModeSelector() {
    this.modeSelector.addEventListener('change', e => {
      settingsManager.setMode(e.target.value);
    });
  }

  _bindModelSelector() {
    this.modelSelector.addEventListener('change', e => {
      settingsManager.setModel(e.target.value);
    });
  }

  _bindSendButton() {
    this.sendButton.addEventListener('click', async () => {
      const userPrompt = this.promptInput.value.trim();
      if (!userPrompt) return;
      await chatManager.sendMessage(userPrompt);
      const history = await chatManager.getHistory();
      this._renderHistory(history);
      this.promptInput.value = '';
    });
  }

  _updateModeSelector(selectedMode) {
    this.modeSelector.value = selectedMode;
  }

  _updateModelSelector(models, selectedModel) {
    this.modelSelector.innerHTML = '';
    for (const m of models) {
      const opt = document.createElement('option');
      opt.value = m;
      opt.textContent = m;
      if (m === selectedModel) opt.selected = true;
      this.modelSelector.appendChild(opt);
    }
  }

  _renderHistory(history) {
    this.chatHistory.innerHTML = '';
    for (const msg of history) {
      const div = document.createElement('div');
      div.className = `chat-message chat-${msg.role}`;
      div.innerHTML = `<span class="msg-role">${msg.role}</span>: <span class="msg-content">${msg.content}</span>`;
      this.chatHistory.appendChild(div);
    }
  }
}

const uiController = new UIController();
export default uiController;

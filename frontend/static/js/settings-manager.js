// settings-manager.js

class SettingsManager {
  constructor() {
    this.mode = 'local'; // "local" (Ollama) или "corporate" (HUB)
    this.model = null;
    this.models = [];
    this.listeners = [];
    this._fetchModels();
  }

  setMode(newMode) {
    if (newMode !== 'local' && newMode !== 'corporate') {
      throw new Error('Invalid mode');
    }
    this.mode = newMode;
    this.model = null; // Сброс выбранной модели при смене режима
    this._fetchModels();
    this._notifyListeners();
  }

  async _fetchModels() {
    try {
      const resp = await fetch(`/api/models?mode=${this.mode}`);
      if (!resp.ok) {
        throw new Error('Failed to fetch models');
      }
      this.models = await resp.json();
      this.model = this.models.length > 0 ? this.models[0] : null;
      this._notifyListeners();
    } catch (e) {
      this.models = [];
      this.model = null;
      this._notifyListeners();
      console.error(`Error loading models for ${this.mode}:`, e);
    }
  }

  setModel(modelName) {
    if (!this.models.includes(modelName)) {
      throw new Error('Model not found in current mode');
    }
    this.model = modelName;
    this._notifyListeners();
  }

  onChange(listener) {
    this.listeners.push(listener);
  }

  _notifyListeners() {
    for (const cb of this.listeners) {
      cb(this.mode, this.model, this.models);
    }
  }

  // getCurrentSettings() {
  //   return {
  //     mode: this.mode,
  //     model: this.model,
  //     models: this.models,
  //   };
  // }
}

const settingsManager = new SettingsManager();
export default settingsManager;

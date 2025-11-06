// app/static/js/settings-manager.js

class SettingsManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.settings = {
            mode: 'local',
            model: null,
            temperature: 0.7,
            max_tokens: 2048
        };
        this.availableModels = [];
        console.log('✓ SettingsManager initialized');
    }

    async loadAvailableModels() {
        console.log('📋 Loading models...');
        try {
            const mode = this.settings.mode || 'local';
            const response = await this.apiService.get(`/models/list?mode=${mode}`);
            console.log('✓ Models response:', response);

            if (response.models && response.models.length > 0) {
                this.availableModels = response.models;
                this.updateModelSelector();
                console.log(`✅ Loaded ${response.models.length} models`);
            } else {
                console.warn('⚠️ No models found:', response.error || 'Unknown error');
                this.availableModels = [];
                this.updateModelSelector();
            }
        } catch (error) {
            console.error('❌ Failed to load models:', error);
            this.availableModels = [];
            this.updateModelSelector();
        }
    }

    updateModelSelector() {
        const selector = document.getElementById('model-selector');
        if (!selector) return;

        if (this.availableModels.length === 0) {
            selector.innerHTML = '<option value="">Модели не найдены</option>';
        } else {
            selector.innerHTML = this.availableModels.map(model =>
                `<option value="${model.name}">${model.name}</option>`
            ).join('');
        }
    }

    getSettings() {
        return this.settings;
    }

    setMode(mode) {
        this.settings.mode = mode;
        this.loadAvailableModels();
    }

    setModel(model) {
        this.settings.model = model;
    }
}

export { SettingsManager };

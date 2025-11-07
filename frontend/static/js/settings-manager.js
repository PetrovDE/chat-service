// app/static/js/settings-manager.js

class SettingsManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.settings = {
            mode: 'local',
            model: 'llama3.1:8b',
            temperature: 0.7,
            max_tokens: 2048
        };
        console.log('✓ SettingsManager initialized');
    }

    async loadAvailableModels() {
        console.log('📋 Loading models...');
        try {
            const mode = this.settings.mode || 'ollama';
            const response = await this.apiService.get(`/models/list?mode=${mode}`);
            console.log('✓ Models response:', response);

            const modelSelector = document.getElementById('model-selector');
            if (modelSelector && response.models) {
                modelSelector.innerHTML = response.models.map(model =>
                    `<option value="${model}">${model}</option>`
                ).join('');
                console.log('✅ Loaded', response.models.length, 'models');
            }
        } catch (error) {
            console.error('❌ Load models error:', error);
        }
    }

    setMode(mode) {
        this.settings.mode = mode;
        console.log('🔧 Mode set to:', mode);
        this.loadAvailableModels();
    }

    setModel(model) {
        this.settings.model = model;
        console.log('🤖 Model set to:', model);
    }

    setTemperature(temperature) {
        this.settings.temperature = parseFloat(temperature);
        console.log('🌡️ Temperature set to:', temperature);
    }

    setMaxTokens(tokens) {
        this.settings.max_tokens = parseInt(tokens);
        console.log('📊 Max tokens set to:', tokens);
    }

    getSettings() {
        return { ...this.settings };
    }

    applySettings() {
        // Apply UI settings
        const tempSlider = document.getElementById('temperatureSlider');
        const tempValue = document.getElementById('temperatureValue');
        const maxTokensInput = document.getElementById('maxTokensInput');

        if (tempSlider && tempValue) {
            this.settings.temperature = parseFloat(tempSlider.value);
            tempValue.textContent = tempSlider.value;
        }

        if (maxTokensInput) {
            this.settings.max_tokens = parseInt(maxTokensInput.value);
        }

        console.log('✅ Settings applied:', this.settings);
        return this.settings;
    }

    setupUI() {
        const tempSlider = document.getElementById('temperatureSlider');
        const tempValue = document.getElementById('temperatureValue');
        const maxTokensInput = document.getElementById('maxTokensInput');

        if (tempSlider && tempValue) {
            tempSlider.addEventListener('input', (e) => {
                tempValue.textContent = e.target.value;
                this.setTemperature(e.target.value);
            });
        }

        if (maxTokensInput) {
            maxTokensInput.addEventListener('change', (e) => {
                this.setMaxTokens(e.target.value);
            });
        }

        console.log('✓ Settings UI setup complete');
    }
}

export { SettingsManager };

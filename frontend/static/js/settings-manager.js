// frontend/static/js/settings-manager.js
class SettingsManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.settings = {
            mode: 'local',
            model: 'llama3.1:8b',
            temperature: 0.7,
            max_tokens: 2048,
            prompt_max_chars: 50000
        };
        this.modelCapabilities = {};
        console.log('SettingsManager initialized');
    }

    async loadAvailableModels(mode = null) {
        console.log('Loading models...');
        try {
            const selectedMode = mode || this.settings.mode || 'local';
            console.log(`Loading models for mode: ${selectedMode}`);

            const validModes = ['local', 'ollama', 'openai', 'aihub'];
            if (!validModes.includes(selectedMode)) {
                console.warn(`Invalid mode ${selectedMode}, using local`);
                this.settings.mode = 'local';
                return this.loadAvailableModels('local');
            }

            const modelsData = await this.apiService.get(`/models/list?mode=${selectedMode}`);
            console.log('Models response:', modelsData);

            const modelSelector = document.getElementById('model-selector');
            if (!modelSelector || !modelsData) return;

            let modelsList = [];
            if (Array.isArray(modelsData)) {
                modelsList = modelsData;
            } else if (modelsData.models && Array.isArray(modelsData.models)) {
                modelsList = modelsData.models;
            } else if (modelsData.data && Array.isArray(modelsData.data)) {
                modelsList = modelsData.data;
            }

            if (modelsList.length === 0) {
                modelSelector.innerHTML = '<option value="">Нет доступных моделей</option>';
                console.warn('No models available for mode:', selectedMode);
                this.updateModelCapsHint('');
                return;
            }

            this.modelCapabilities = {};
            modelSelector.innerHTML = modelsList.map((model) => {
                const modelValue = typeof model === 'string' ? model : (model.name || model.id || String(model));
                const modelLabel = typeof model === 'string' ? model : (model.name || model.id || String(model));

                if (typeof model === 'object' && model) {
                    this.modelCapabilities[modelValue] = {
                        context_window: Number(model.context_window) || null,
                        max_output_tokens: Number(model.max_output_tokens) || null,
                    };
                }

                return `<option value="${modelValue}">${modelLabel}</option>`;
            }).join('');

            if (this.settings.model) {
                const optionExists = Array.from(modelSelector.options).some((opt) => opt.value === this.settings.model);
                if (optionExists) {
                    modelSelector.value = this.settings.model;
                } else {
                    const firstModel = typeof modelsList[0] === 'string' ? modelsList[0] : (modelsList[0].name || modelsList[0].id);
                    this.settings.model = firstModel;
                    modelSelector.value = firstModel;
                    console.log('Current model not found, selected first:', firstModel);
                }
            }

            this.applyModelCapabilities(modelSelector.value || this.settings.model);
            console.log('Loaded', modelsList.length, 'models for mode:', selectedMode);
        } catch (error) {
            console.error('Load models error:', error);
            const modelSelector = document.getElementById('model-selector');
            if (modelSelector) {
                modelSelector.innerHTML = '<option value="">Ошибка загрузки моделей</option>';
            }
            this.updateModelCapsHint('');
        }
    }

    setMode(mode) {
        const validModes = ['local', 'ollama', 'openai', 'aihub'];
        if (!validModes.includes(mode)) {
            console.warn(`Invalid mode: ${mode}, keeping current: ${this.settings.mode}`);
            return;
        }
        this.settings.mode = mode;
        console.log('Mode set to:', mode);
    }

    setModel(model) {
        this.settings.model = model;
        console.log('Model set to:', model);
    }

    setTemperature(temperature) {
        this.settings.temperature = parseFloat(temperature);
        console.log('Temperature set to:', temperature);
    }

    setMaxTokens(tokens) {
        this.settings.max_tokens = parseInt(tokens);
        console.log('Max tokens set to:', tokens);
    }

    setPromptMaxChars(chars) {
        this.settings.prompt_max_chars = parseInt(chars);
        console.log('Prompt max chars set to:', chars);
    }

    updateModelCapsHint(modelName) {
        const caps = this.modelCapabilities[modelName] || {};
        const contextText = caps.context_window ? `${caps.context_window} tok` : 'n/a';
        const outputText = caps.max_output_tokens ? `${caps.max_output_tokens} tok` : 'n/a';
        const promptText = this.settings.prompt_max_chars ? `${this.settings.prompt_max_chars} chars` : 'auto';
        const text = `Model limits: context=${contextText}, output=${outputText}, prompt=${promptText}`;

        const hintInline = document.getElementById('modelCapsHint');
        if (hintInline) hintInline.textContent = text;

        const hintSettings = document.getElementById('settingsModelCapsHint');
        if (hintSettings) hintSettings.textContent = text;
    }

    applyModelCapabilities(modelName) {
        const caps = this.modelCapabilities[modelName] || {};
        const maxTokensInput = document.getElementById('maxTokensInput');
        const promptMaxCharsInput = document.getElementById('promptMaxCharsInput');

        if (maxTokensInput && caps.max_output_tokens) {
            const cap = Math.max(128, Number(caps.max_output_tokens));
            maxTokensInput.max = String(cap);
            if (Number(this.settings.max_tokens) > cap) {
                this.settings.max_tokens = cap;
                maxTokensInput.value = String(cap);
            }
        }

        if (promptMaxCharsInput && caps.context_window) {
            const suggested = Math.max(8000, Math.min(200000, Math.floor(Number(caps.context_window) * 3)));
            if (!this.settings.prompt_max_chars || this.settings.prompt_max_chars === 50000) {
                this.settings.prompt_max_chars = suggested;
                promptMaxCharsInput.value = String(suggested);
            }
        }

        this.updateModelCapsHint(modelName);
    }

    getSettings() {
        return { ...this.settings };
    }

    applySettings() {
        const modelSelector = document.getElementById('model-selector');
        const tempSlider = document.getElementById('temperatureSlider');
        const tempValue = document.getElementById('temperatureValue');
        const maxTokensInput = document.getElementById('maxTokensInput');
        const promptMaxCharsInput = document.getElementById('promptMaxCharsInput');

        if (modelSelector && modelSelector.value) {
            this.settings.model = modelSelector.value;
            this.applyModelCapabilities(modelSelector.value);
        }

        if (tempSlider && tempValue) {
            this.settings.temperature = parseFloat(tempSlider.value);
            tempValue.textContent = tempSlider.value;
        }

        if (maxTokensInput) {
            this.settings.max_tokens = parseInt(maxTokensInput.value);
        }

        if (promptMaxCharsInput) {
            this.settings.prompt_max_chars = parseInt(promptMaxCharsInput.value);
        }

        this.updateModelCapsHint(this.settings.model);
        console.log('Settings applied:', this.settings);
        return this.settings;
    }

    setupUI() {
        const modelSelector = document.getElementById('model-selector');
        const tempSlider = document.getElementById('temperatureSlider');
        const tempValue = document.getElementById('temperatureValue');
        const maxTokensInput = document.getElementById('maxTokensInput');
        const promptMaxCharsInput = document.getElementById('promptMaxCharsInput');

        if (modelSelector) {
            modelSelector.addEventListener('change', (e) => {
                this.setModel(e.target.value);
                this.applyModelCapabilities(e.target.value);
            });
        }

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

        if (promptMaxCharsInput) {
            promptMaxCharsInput.addEventListener('change', (e) => {
                this.setPromptMaxChars(e.target.value);
                this.updateModelCapsHint(this.settings.model);
            });
        }

        this.updateModelCapsHint(this.settings.model);
        console.log('Settings UI setup complete');
    }
}

export { SettingsManager };

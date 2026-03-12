// frontend/static/js/settings-manager.js
class SettingsManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.settings = {
            mode: 'local',
            model: 'llama3.1:8b',
            embedding_model: null,
            temperature: 0.7,
            max_tokens: 2048,
            prompt_max_chars: 50000,
            rag_debug: false,
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

            const [chatModelsData, embeddingModelsData] = await Promise.all([
                this.apiService.get(`/models/list?mode=${selectedMode}&capability=chat`),
                this.apiService.get(`/models/list?mode=${selectedMode}&capability=embedding`),
            ]);

            this.renderModelSelector({
                selectorId: 'model-selector',
                modelsData: chatModelsData,
                settingsKey: 'model',
                emptyLabel: 'No chat models',
                collectCapabilities: true,
            });
            this.renderModelSelector({
                selectorId: 'embedding-model-selector',
                modelsData: embeddingModelsData,
                settingsKey: 'embedding_model',
                emptyLabel: 'No embedding models',
                collectCapabilities: false,
            });

            const selectedChatModel = document.getElementById('model-selector')?.value || this.settings.model;
            this.applyModelCapabilities(selectedChatModel);
            console.log('Loaded provider-aware chat/embedding models for mode:', selectedMode);
        } catch (error) {
            console.error('Load models error:', error);
            const modelSelector = document.getElementById('model-selector');
            if (modelSelector) {
                modelSelector.innerHTML = '<option value="">Model load error</option>';
            }
            const embeddingSelector = document.getElementById('embedding-model-selector');
            if (embeddingSelector) {
                embeddingSelector.innerHTML = '<option value="">Model load error</option>';
            }
            this.updateModelCapsHint('');
        }
    }

    extractModelsList(modelsData) {
        if (!modelsData) return [];
        if (Array.isArray(modelsData)) return modelsData;
        if (Array.isArray(modelsData.models)) return modelsData.models;
        if (Array.isArray(modelsData.data)) return modelsData.data;
        return [];
    }

    renderModelSelector({ selectorId, modelsData, settingsKey, emptyLabel, collectCapabilities = false }) {
        const selector = document.getElementById(selectorId);
        if (!selector) return;

        const modelsList = this.extractModelsList(modelsData);
        const defaultModel = typeof modelsData?.default_model === 'string' ? modelsData.default_model : null;

        const options = [];
        const seen = new Set();
        for (const model of modelsList) {
            const modelValue = typeof model === 'string' ? model : (model?.name || model?.id || String(model));
            if (!modelValue || seen.has(modelValue)) continue;
            seen.add(modelValue);
            options.push(model);
        }

        if (defaultModel && !seen.has(defaultModel)) {
            options.unshift({ name: defaultModel, is_default: true });
            seen.add(defaultModel);
        }

        if (options.length === 0) {
            selector.innerHTML = `<option value="">${emptyLabel}</option>`;
            if (settingsKey === 'embedding_model') {
                this.settings.embedding_model = null;
            }
            return;
        }

        if (collectCapabilities) {
            this.modelCapabilities = {};
        }

        selector.innerHTML = options.map((model) => {
            const modelValue = typeof model === 'string' ? model : (model.name || model.id || String(model));
            const modelLabel = typeof model === 'string' ? model : (model.name || model.id || String(model));

            if (collectCapabilities && typeof model === 'object' && model) {
                this.modelCapabilities[modelValue] = {
                    context_window: Number(model.context_window) || null,
                    max_output_tokens: Number(model.max_output_tokens) || null,
                };
            }

            return `<option value="${modelValue}">${modelLabel}</option>`;
        }).join('');

        const currentValue = this.settings[settingsKey];
        const optionExists = currentValue
            ? Array.from(selector.options).some((opt) => opt.value === currentValue)
            : false;

        let selectedValue = null;
        if (optionExists) {
            selectedValue = currentValue;
        } else if (defaultModel && seen.has(defaultModel)) {
            selectedValue = defaultModel;
        } else {
            const first = options[0];
            selectedValue = typeof first === 'string' ? first : (first.name || first.id || null);
        }

        if (selectedValue) {
            selector.value = selectedValue;
            this.settings[settingsKey] = selectedValue;
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

    setEmbeddingModel(model) {
        this.settings.embedding_model = model || null;
        console.log('Embedding model set to:', this.settings.embedding_model);
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
        const ragDebugToggle = document.getElementById('ragDebugToggle');

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
        const embeddingSelector = document.getElementById('embedding-model-selector');
        const tempSlider = document.getElementById('temperatureSlider');
        const tempValue = document.getElementById('temperatureValue');
        const maxTokensInput = document.getElementById('maxTokensInput');
        const promptMaxCharsInput = document.getElementById('promptMaxCharsInput');
        const ragDebugToggle = document.getElementById('ragDebugToggle');

        if (modelSelector && modelSelector.value) {
            this.settings.model = modelSelector.value;
            this.applyModelCapabilities(modelSelector.value);
        }

        if (embeddingSelector && embeddingSelector.value) {
            this.settings.embedding_model = embeddingSelector.value;
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

        if (ragDebugToggle) {
            this.settings.rag_debug = Boolean(ragDebugToggle.checked);
        }

        this.updateModelCapsHint(this.settings.model);
        console.log('Settings applied:', this.settings);
        return this.settings;
    }

    setupUI() {
        const modelSelector = document.getElementById('model-selector');
        const embeddingSelector = document.getElementById('embedding-model-selector');
        const tempSlider = document.getElementById('temperatureSlider');
        const tempValue = document.getElementById('temperatureValue');
        const maxTokensInput = document.getElementById('maxTokensInput');
        const promptMaxCharsInput = document.getElementById('promptMaxCharsInput');
        const ragDebugToggle = document.getElementById('ragDebugToggle');

        if (modelSelector) {
            modelSelector.addEventListener('change', (e) => {
                this.setModel(e.target.value);
                this.applyModelCapabilities(e.target.value);
            });
        }

        if (embeddingSelector) {
            embeddingSelector.addEventListener('change', (e) => {
                this.setEmbeddingModel(e.target.value);
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

        if (ragDebugToggle) {
            ragDebugToggle.checked = Boolean(this.settings.rag_debug);
            ragDebugToggle.addEventListener('change', (e) => {
                this.settings.rag_debug = Boolean(e.target.checked);
            });
        }

        this.updateModelCapsHint(this.settings.model);
        console.log('Settings UI setup complete');
    }
}

export { SettingsManager };

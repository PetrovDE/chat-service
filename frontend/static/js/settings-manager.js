// frontend/static/js/settings-manager.js
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

    async loadAvailableModels(mode = null) {
        console.log('📋 Loading models...');
        try {
            // Используем переданный режим или текущий
            const selectedMode = mode || this.settings.mode || 'local';
            console.log(`🔧 Loading models for mode: ${selectedMode}`);

            // ✅ Валидация режима
            const validModes = ['local', 'ollama', 'openai', 'aihub'];
            if (!validModes.includes(selectedMode)) {
                console.warn(`⚠️ Invalid mode ${selectedMode}, using local`);
                this.settings.mode = 'local';
                return this.loadAvailableModels('local');
            }

            let modelsData;

            // ✅ ИСПРАВЛЕНИЕ: Всегда используем правильный эндпоинт /models/list?mode=...
            console.log(`🔌 Fetching models from: /models/list?mode=${selectedMode}`);
            const response = await this.apiService.get(`/models/list?mode=${selectedMode}`);
            modelsData = response;

            console.log('✓ Models response:', modelsData);

            const modelSelector = document.getElementById('model-selector');
            if (modelSelector && modelsData) {
                // Обрабатываем разные форматы ответа
                let modelsList = [];

                if (Array.isArray(modelsData)) {
                    // Прямой массив моделей
                    modelsList = modelsData;
                } else if (modelsData.models && Array.isArray(modelsData.models)) {
                    // Объект с полем models
                    modelsList = modelsData.models;
                } else if (modelsData.data && Array.isArray(modelsData.data)) {
                    // Объект с полем data (формат OpenAI)
                    modelsList = modelsData.data;
                } else {
                    console.warn('⚠️ Unexpected models response format:', modelsData);
                    modelsList = [];
                }

                if (modelsList.length === 0) {
                    modelSelector.innerHTML = '<option value="">Нет доступных моделей</option>';
                    console.warn('⚠️ No models available for mode:', selectedMode);
                    return;
                }

                // Заполняем селект
                modelSelector.innerHTML = modelsList.map(model => {
                    // Если model - строка, используем как есть
                    // Если model - объект, извлекаем name или id
                    const modelValue = typeof model === 'string' ? model : (model.name || model.id || String(model));
                    const modelLabel = typeof model === 'string' ? model : (model.name || model.id || String(model));
                    return `<option value="${modelValue}">${modelLabel}</option>`;
                }).join('');

                console.log('✅ Loaded', modelsList.length, 'models for mode:', selectedMode);

                // Установить текущую модель если она есть
                if (this.settings.model) {
                    const optionExists = Array.from(modelSelector.options).some(opt => opt.value === this.settings.model);
                    if (optionExists) {
                        modelSelector.value = this.settings.model;
                    } else {
                        // Если текущая модель не найдена, выбираем первую
                        if (modelsList.length > 0) {
                            const firstModel = typeof modelsList[0] === 'string' ? modelsList[0] : (modelsList[0].name || modelsList[0].id);
                            this.settings.model = firstModel;
                            modelSelector.value = firstModel;
                            console.log('⚠️ Current model not found, selected first:', firstModel);
                        }
                    }
                }
            }
        } catch (error) {
            console.error('❌ Load models error:', error);
            const modelSelector = document.getElementById('model-selector');
            if (modelSelector) {
                modelSelector.innerHTML = '<option value="">Ошибка загрузки моделей</option>';
            }
        }
    }

    setMode(mode) {
        // ✅ Валидация режима
        const validModes = ['local', 'ollama', 'openai', 'aihub'];
        if (!validModes.includes(mode)) {
            console.warn(`⚠️ Invalid mode: ${mode}, keeping current: ${this.settings.mode}`);
            return;
        }
        this.settings.mode = mode;
        console.log('🔧 Mode set to:', mode);
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
        const modelSelector = document.getElementById('model-selector');
        const tempSlider = document.getElementById('temperatureSlider');
        const tempValue = document.getElementById('temperatureValue');
        const maxTokensInput = document.getElementById('maxTokensInput');

        // Получение модели из селектора
        if (modelSelector && modelSelector.value) {
            this.settings.model = modelSelector.value;
        }

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
        const modelSelector = document.getElementById('model-selector');
        const tempSlider = document.getElementById('temperatureSlider');
        const tempValue = document.getElementById('temperatureValue');
        const maxTokensInput = document.getElementById('maxTokensInput');

        // Обработчик изменения модели
        if (modelSelector) {
            modelSelector.addEventListener('change', (e) => {
                this.setModel(e.target.value);
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

        console.log('✓ Settings UI setup complete');
    }
}

export { SettingsManager };

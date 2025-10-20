// app/static/js/settings-manager.js
console.log('Loading SettingsManager module...');

export class SettingsManager {
    constructor(apiService, uiController) {
        console.log('SettingsManager constructor called');
        this.apiService = apiService;
        this.uiController = uiController;

        // Default settings
        this.settings = {
            modelSource: 'ollama',
            modelName: 'llama3.1:8b',
            temperature: 0.7,
            maxTokens: 2000,
            topP: 0.9,
            topK: 40,
            // Corporate API settings
            corporateApiUrl: '',
            corporateApiKey: ''
        };

        this.availableModels = {
            ollama: [],
            corporate: [] // Вместо openai
        };

        this.loadSettings();
    }

    loadSettings() {
        const stored = localStorage.getItem('chat_settings');
        if (stored) {
            try {
                const parsed = JSON.parse(stored);
                this.settings = { ...this.settings, ...parsed };
                console.log('Settings loaded:', this.settings);
            } catch (e) {
                console.error('Error loading settings:', e);
            }
        }
    }

    saveSettings() {
        try {
            localStorage.setItem('chat_settings', JSON.stringify(this.settings));
            console.log('Settings saved:', this.settings);
        } catch (e) {
            console.error('Error saving settings:', e);
        }
    }

    getSettings() {
        return { ...this.settings };
    }

    updateSetting(key, value) {
        this.settings[key] = value;
        this.saveSettings();
    }

    async loadAvailableModels() {
        try {
            const data = await this.apiService.getModels();

            this.availableModels.ollama = data.available_models?.ollama || [];

            // Update current model if available
            if (data.current_model) {
                this.settings.modelSource = data.current_model.source;
                this.settings.modelName = data.current_model.model;
                this.saveSettings();
            }

            console.log('Available models loaded:', this.availableModels);
            return this.availableModels;

        } catch (error) {
            console.error('Error loading models:', error);
            return this.availableModels;
        }
    }

    async switchModel(modelSource, modelName) {
        try {
            await this.apiService.switchModel(modelSource, modelName);

            this.settings.modelSource = modelSource;
            this.settings.modelName = modelName;
            this.saveSettings();

            this.uiController.showSuccess(`Модель переключена: ${modelName}`);
            return true;

        } catch (error) {
            console.error('Error switching model:', error);
            this.uiController.showError('Ошибка переключения модели: ' + error.message);
            return false;
        }
    }

    getAvailableModels() {
        return this.availableModels;
    }

    async openSettingsPanel() {
        // Load latest models
        await this.loadAvailableModels();

        // Create settings panel
        this.renderSettingsPanel();
    }

    renderSettingsPanel() {
        // Remove existing panel
        const existing = document.getElementById('settingsPanel');
        if (existing) {
            existing.remove();
        }

        const panel = document.createElement('div');
        panel.id = 'settingsPanel';
        panel.className = 'settings-panel';
        panel.innerHTML = `
            <div class="settings-overlay" onclick="closeSettingsPanel()"></div>
            <div class="settings-content">
                <div class="settings-header">
                    <h2>⚙️ Настройки</h2>
                    <button class="close-button" onclick="closeSettingsPanel()">×</button>
                </div>
                
                <div class="settings-body">
                    <!-- Model Selection -->
                    <div class="settings-section">
                        <h3>🤖 Модель</h3>
                        
                        <div class="form-group">
                            <label>Источник модели</label>
                            <select id="modelSource" class="settings-input">
                                <option value="ollama" ${this.settings.modelSource === 'ollama' ? 'selected' : ''}>Ollama (Локально)</option>
                                <option value="corporate" ${this.settings.modelSource === 'corporate' ? 'selected' : ''}>Корпоративная API</option>
                            </select>
                        </div>
                        
                        <div id="ollamaModelSection" style="display: ${this.settings.modelSource === 'ollama' ? 'block' : 'none'};">
                            <div class="form-group">
                                <label>Модель Ollama</label>
                                <select id="modelName" class="settings-input">
                                    ${this.renderOllamaModelOptions()}
                                </select>
                            </div>
                        </div>
                        
                        <div id="corporateApiSection" style="display: ${this.settings.modelSource === 'corporate' ? 'block' : 'none'};">
                            <div class="form-group">
                                <label>URL корпоративной API</label>
                                <input type="text" id="corporateApiUrl" class="settings-input" 
                                       value="${this.settings.corporateApiUrl || ''}"
                                       placeholder="https://your-api.company.com/v1">
                                <small>Endpoint для API совместимой с OpenAI</small>
                            </div>
                            
                            <div class="form-group">
                                <label>Название модели</label>
                                <input type="text" id="corporateModelName" class="settings-input" 
                                       value="${this.settings.modelSource === 'corporate' ? this.settings.modelName : ''}"
                                       placeholder="gpt-4, claude-3, llama-3, etc.">
                                <small>Имя модели для использования</small>
                            </div>
                            
                            <div class="form-group">
                                <label>API ключ (KeyCloak token)</label>
                                <input type="password" id="corporateApiKey" class="settings-input" 
                                       value="${this.settings.corporateApiKey || ''}"
                                       placeholder="Bearer token или API key">
                                <small>Токен авторизации из KeyCloak</small>
                            </div>
                        </div>
                    </div>
                    
                    <!-- Generation Parameters -->
                    <div class="settings-section">
                        <h3>🎛️ Параметры генерации</h3>
                        
                        <div class="form-group">
                            <label>
                                Температура: <span id="tempValue">${this.settings.temperature}</span>
                            </label>
                            <input type="range" id="temperature" class="settings-slider" 
                                   min="0" max="2" step="0.1" value="${this.settings.temperature}">
                            <small>Контролирует случайность. 0 = предсказуемо, 2 = креативно</small>
                        </div>
                        
                        <div class="form-group">
                            <label>
                                Максимум токенов: <span id="tokensValue">${this.settings.maxTokens}</span>
                            </label>
                            <input type="range" id="maxTokens" class="settings-slider" 
                                   min="100" max="8000" step="100" value="${this.settings.maxTokens}">
                            <small>Максимальная длина ответа</small>
                        </div>
                        
                        <div class="form-group">
                            <label>
                                Top P: <span id="topPValue">${this.settings.topP}</span>
                            </label>
                            <input type="range" id="topP" class="settings-slider" 
                                   min="0" max="1" step="0.05" value="${this.settings.topP}">
                            <small>Nucleus sampling. Рекомендуется 0.9</small>
                        </div>
                    </div>
                </div>
                
                <div class="settings-footer">
                    <button class="settings-btn secondary" onclick="resetSettings()">Сбросить</button>
                    <button class="settings-btn primary" onclick="saveSettings()">Сохранить</button>
                </div>
            </div>
        `;

        document.body.appendChild(panel);

        // Add event listeners
        this.attachSettingsListeners();

        // Show panel
        setTimeout(() => panel.classList.add('show'), 10);
    }

    renderOllamaModelOptions() {
        const models = this.availableModels.ollama || [];

        if (models.length === 0) {
            return `<option value="${this.settings.modelName}">${this.settings.modelName}</option>`;
        }

        return models.map(model =>
            `<option value="${model}" ${model === this.settings.modelName ? 'selected' : ''}>${model}</option>`
        ).join('');
    }

    attachSettingsListeners() {
        // Model source change
        const modelSource = document.getElementById('modelSource');
        if (modelSource) {
            modelSource.addEventListener('change', () => {
                const source = modelSource.value;
                this.settings.modelSource = source;

                // Toggle sections
                const ollamaSection = document.getElementById('ollamaModelSection');
                const corporateSection = document.getElementById('corporateApiSection');

                if (source === 'ollama') {
                    ollamaSection.style.display = 'block';
                    corporateSection.style.display = 'none';
                } else {
                    ollamaSection.style.display = 'none';
                    corporateSection.style.display = 'block';
                }
            });
        }

        // Sliders
        const temperature = document.getElementById('temperature');
        const tempValue = document.getElementById('tempValue');
        if (temperature && tempValue) {
            temperature.addEventListener('input', () => {
                tempValue.textContent = temperature.value;
            });
        }

        const maxTokens = document.getElementById('maxTokens');
        const tokensValue = document.getElementById('tokensValue');
        if (maxTokens && tokensValue) {
            maxTokens.addEventListener('input', () => {
                tokensValue.textContent = maxTokens.value;
            });
        }

        const topP = document.getElementById('topP');
        const topPValue = document.getElementById('topPValue');
        if (topP && topPValue) {
            topP.addEventListener('input', () => {
                topPValue.textContent = topP.value;
            });
        }
    }

    async applySettings() {
        try {
            // Get values from inputs
            const modelSource = document.getElementById('modelSource')?.value;
            const temperature = parseFloat(document.getElementById('temperature')?.value || 0.7);
            const maxTokens = parseInt(document.getElementById('maxTokens')?.value || 2000);
            const topP = parseFloat(document.getElementById('topP')?.value || 0.9);

            let modelName;

            if (modelSource === 'ollama') {
                modelName = document.getElementById('modelName')?.value;
            } else if (modelSource === 'corporate') {
                modelName = document.getElementById('corporateModelName')?.value;
                const apiUrl = document.getElementById('corporateApiUrl')?.value;
                const apiKey = document.getElementById('corporateApiKey')?.value;

                this.settings.corporateApiUrl = apiUrl;
                this.settings.corporateApiKey = apiKey;
            }

            // Check if model changed
            if (modelSource !== this.settings.modelSource || modelName !== this.settings.modelName) {
                const success = await this.switchModel(modelSource, modelName);
                if (!success) return false;
            }

            // Update settings
            this.settings = {
                ...this.settings,
                modelSource,
                modelName,
                temperature,
                maxTokens,
                topP
            };

            this.saveSettings();
            this.uiController.showSuccess('Настройки сохранены');

            return true;

        } catch (error) {
            console.error('Error applying settings:', error);
            this.uiController.showError('Ошибка сохранения настроек');
            return false;
        }
    }

    resetToDefaults() {
        this.settings = {
            modelSource: 'ollama',
            modelName: 'llama3.1:8b',
            temperature: 0.7,
            maxTokens: 2000,
            topP: 0.9,
            topK: 40,
            corporateApiUrl: '',
            corporateApiKey: ''
        };
        this.saveSettings();
        this.renderSettingsPanel();
        this.uiController.showSuccess('Настройки сброшены');
    }
}

console.log('SettingsManager class defined successfully');
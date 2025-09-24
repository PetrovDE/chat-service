// UI management and control
export class UIController {
    constructor() {
        this.loadingOverlay = null;
        this.setupLoadingOverlay();
    }

    setupLoadingOverlay() {
        // Create loading overlay if it doesn't exist
        if (!document.getElementById('loadingOverlay')) {
            const overlay = document.createElement('div');
            overlay.id = 'loadingOverlay';
            overlay.style.cssText = `
                position: fixed;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: rgba(0,0,0,0.5);
                display: none;
                align-items: center;
                justify-content: center;
                z-index: 9999;
                color: white;
                font-size: 1.1rem;
            `;
            overlay.innerHTML = '<div>Loading...</div>';
            document.body.appendChild(overlay);
            this.loadingOverlay = overlay;
        }
    }

    // Settings panel management
    toggleSettings() {
        const settingsPanel = document.getElementById('settingsPanel');
        const overlay = document.getElementById('settingsOverlay');

        if (settingsPanel && overlay) {
            settingsPanel.classList.add('show');
            overlay.classList.add('show');
        }
    }

    closeSettings() {
        const settingsPanel = document.getElementById('settingsPanel');
        const overlay = document.getElementById('settingsOverlay');

        if (settingsPanel && overlay) {
            settingsPanel.classList.remove('show');
            overlay.classList.remove('show');
        }
    }

    // Global click handler for modal management
    handleGlobalClick(event) {
        const fileModal = document.getElementById('fileModal');
        const attachButton = document.getElementById('attachButton');

        // Close file modal when clicking outside
        if (fileModal && !fileModal.contains(event.target) &&
            attachButton && !attachButton.contains(event.target)) {
            fileModal.classList.remove('show');
        }
    }

    // Model source management
    async initializeModelSelector() {
        try {
            const response = await fetch('/api/source');
            const data = await response.json();

            this.updateModelSelector(data);
            this.setupModelSourceListeners();

            // Load local models if using local source
            if (data.active_source === 'local') {
                await this.loadLocalModels();
            }

        } catch (error) {
            console.error('Failed to initialize model selector:', error);
        }
    }

    updateModelSelector(data) {
        const sourceIndicator = document.getElementById('modelSourceIndicator');
        const modelName = document.getElementById('currentModelName');

        if (sourceIndicator) {
            sourceIndicator.textContent = data.active_source === 'local' ? '💻 Local' : '☁️ API';
            sourceIndicator.className = `model-source-badge ${data.active_source}`;
        }

        if (modelName) {
            modelName.textContent = data.current_model || 'Not selected';
        }
    }

    setupModelSourceListeners() {
        // Source type radio buttons
        document.querySelectorAll('input[name="modelSource"]').forEach(radio => {
            radio.addEventListener('change', async (e) => {
                if (e.target.value === 'api') {
                    this.showApiConfiguration();
                } else {
                    await this.switchToLocal();
                }
            });
        });

        // Local model selector
        const localModelSelect = document.getElementById('localModelSelect');
        if (localModelSelect) {
            localModelSelect.addEventListener('change', async (e) => {
                await this.switchLocalModel(e.target.value);
            });
        }

        // API configuration save button
        const saveApiConfig = document.getElementById('saveApiConfig');
        if (saveApiConfig) {
            saveApiConfig.addEventListener('click', async () => {
                await this.saveApiConfiguration();
            });
        }

        // Load local models button
        const refreshModels = document.getElementById('refreshLocalModels');
        if (refreshModels) {
            refreshModels.addEventListener('click', async () => {
                await this.loadLocalModels();
            });
        }
    }

    async loadLocalModels() {
        try {
            this.showLoading('Loading local models...');

            const response = await fetch('/api/models/local');
            const data = await response.json();

            const select = document.getElementById('localModelSelect');
            if (select && data.models) {
                select.innerHTML = '';

                if (data.models.length === 0) {
                    const option = document.createElement('option');
                    option.value = '';
                    option.textContent = 'No models found';
                    select.appendChild(option);
                } else {
                    data.models.forEach(model => {
                        const option = document.createElement('option');
                        const modelName = typeof model === 'object' ? model.name : model;
                        option.value = modelName;
                        option.textContent = modelName;

                        if (typeof model === 'object' && model.size) {
                            option.textContent += ` (${this.formatFileSize(model.size)})`;
                        }

                        if (modelName === data.current) {
                            option.selected = true;
                        }

                        select.appendChild(option);
                    });
                }
            }

            this.hideLoading();

        } catch (error) {
            this.hideLoading();
            this.showError('Failed to load local models: ' + error.message);
        }
    }

    async switchLocalModel(modelName) {
        if (!modelName) return;

        try {
            this.showLoading('Switching model...');

            const response = await fetch('/api/models/local/switch', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ model: modelName })
            });

            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.detail || 'Failed to switch model');
            }

            const data = await response.json();

            // Update UI
            const currentModelName = document.getElementById('currentModelName');
            if (currentModelName) {
                currentModelName.textContent = modelName;
            }

            this.hideLoading();
            this.showSuccess(`Switched to ${modelName}`);

        } catch (error) {
            this.hideLoading();
            this.showError('Failed to switch model: ' + error.message);
        }
    }

    async switchToLocal() {
        try {
            this.showLoading('Switching to local models...');

            const response = await fetch('/api/source', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ source: 'local' })
            });

            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.detail || 'Failed to switch source');
            }

            const data = await response.json();

            // Update UI
            this.updateModelSelector({
                active_source: 'local',
                current_model: data.current_model
            });

            // Update radio button
            const localRadio = document.querySelector('input[name="modelSource"][value="local"]');
            if (localRadio) {
                localRadio.checked = true;
            }

            // Load local models list
            await this.loadLocalModels();

            this.hideLoading();
            this.showSuccess('Switched to local models');

        } catch (error) {
            this.hideLoading();
            this.showError('Failed to switch: ' + error.message);
        }
    }

    showApiConfiguration() {
        const modal = document.getElementById('apiConfigModal');
        if (modal) {
            modal.classList.add('show');
            this.loadSavedApiConfig();
        }
    }

    hideApiConfiguration() {
        const modal = document.getElementById('apiConfigModal');
        if (modal) {
            modal.classList.remove('show');
        }
    }

    async saveApiConfiguration() {
        try {
            const apiUrl = document.getElementById('apiUrl').value;
            const apiKey = document.getElementById('apiKey').value;
            const modelName = document.getElementById('apiModelName').value;
            const apiType = document.getElementById('apiType').value;

            if (!apiUrl || !apiKey || !modelName) {
                this.showError('Please fill in all required fields');
                return;
            }

            this.showLoading('Configuring API...');

            const response = await fetch('/api/source', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    source: 'api',
                    api_config: {
                        api_url: apiUrl,
                        api_key: apiKey,
                        model_name: modelName,
                        api_type: apiType
                    }
                })
            });

            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.detail || 'API configuration failed');
            }

            const data = await response.json();

            // Update UI
            this.updateModelSelector({
                active_source: 'api',
                current_model: modelName
            });

            // Update radio button
            const apiRadio = document.querySelector('input[name="modelSource"][value="api"]');
            if (apiRadio) {
                apiRadio.checked = true;
            }

            // Hide modal
            this.hideApiConfiguration();

            // Save to localStorage for convenience (excluding API key for security)
            localStorage.setItem('apiConfig', JSON.stringify({
                api_url: apiUrl,
                model_name: modelName,
                api_type: apiType
            }));

            this.hideLoading();
            this.showSuccess('API configured successfully');

        } catch (error) {
            this.hideLoading();
            this.showError('API configuration failed: ' + error.message);
        }
    }

    loadSavedApiConfig() {
        const saved = localStorage.getItem('apiConfig');
        if (saved) {
            try {
                const config = JSON.parse(saved);
                const apiUrl = document.getElementById('apiUrl');
                const apiModelName = document.getElementById('apiModelName');
                const apiType = document.getElementById('apiType');

                if (apiUrl) apiUrl.value = config.api_url || '';
                if (apiModelName) apiModelName.value = config.model_name || '';
                if (apiType) apiType.value = config.api_type || 'openai';
            } catch (e) {
                console.error('Failed to load saved API config:', e);
            }
        }
    }

    // Loading states
    showLoading(message = 'Loading...') {
        if (this.loadingOverlay) {
            this.loadingOverlay.querySelector('div').textContent = message;
            this.loadingOverlay.style.display = 'flex';
        }
    }

    hideLoading() {
        if (this.loadingOverlay) {
            this.loadingOverlay.style.display = 'none';
        }
    }

    // Message display functions
    showError(message) {
        this.showNotification(message, 'error', 5000);
    }

    showSuccess(message) {
        this.showNotification(message, 'success', 3000);
    }

    // Health status indicator
    updateHealthStatus(health) {
        const indicator = document.getElementById('healthIndicator');
        const status = document.getElementById('healthStatus');

        if (indicator && status) {
            const healthStatus = health.status || 'unknown';

            // Update indicator color
            indicator.className = 'status-indicator';
            if (healthStatus === 'healthy') {
                indicator.classList.add('healthy');
            } else if (healthStatus === 'degraded') {
                indicator.classList.add('degraded');
            }

            // Update status text
            if (health.active_source === 'api') {
                status.textContent = `API: ${health.api_status || 'unknown'}`;
            } else {
                status.textContent = `Ollama: ${health.local_status || health.ollama_status || 'unknown'}`;
            }
        }
    }

    updateHealthError() {
        const indicator = document.getElementById('healthIndicator');
        const status = document.getElementById('healthStatus');

        if (indicator && status) {
            indicator.className = 'status-indicator';
            status.textContent = 'Connection Error';
        }
    }

    // Notification system
    showNotification(message, type = 'info', duration = 3000) {
        const notification = document.createElement('div');
        notification.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 1rem 1.5rem;
            border-radius: 8px;
            color: white;
            font-weight: 500;
            z-index: 10000;
            animation: slideInFromRight 0.3s ease-out;
        `;

        // Set color based on type
        switch (type) {
            case 'success':
                notification.style.backgroundColor = '#10b981';
                break;
            case 'error':
                notification.style.backgroundColor = '#ef4444';
                break;
            case 'warning':
                notification.style.backgroundColor = '#f59e0b';
                break;
            default:
                notification.style.backgroundColor = '#3b82f6';
        }

        notification.textContent = message;

        // Add animation keyframes if they don't exist
        if (!document.getElementById('notification-animations')) {
            const style = document.createElement('style');
            style.id = 'notification-animations';
            style.textContent = `
                @keyframes slideInFromRight {
                    from { transform: translateX(100%); opacity: 0; }
                    to { transform: translateX(0); opacity: 1; }
                }
                @keyframes slideOutToRight {
                    from { transform: translateX(0); opacity: 1; }
                    to { transform: translateX(100%); opacity: 0; }
                }
            `;
            document.head.appendChild(style);
        }

        document.body.appendChild(notification);

        // Auto-remove notification
        setTimeout(() => {
            notification.style.animation = 'slideOutToRight 0.3s ease-out';
            setTimeout(() => {
                if (notification.parentNode) {
                    notification.remove();
                }
            }, 300);
        }, duration);
    }

    // Utility methods
    formatFileSize(bytes) {
        if (!bytes) return '';
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
    }

    scrollChatToBottom() {
        const chatMessages = document.getElementById('chatMessages');
        if (chatMessages) {
            chatMessages.scrollTop = chatMessages.scrollHeight;
        }
    }

    clearMessageInput() {
        const messageInput = document.getElementById('messageInput');
        if (messageInput) {
            messageInput.value = '';
            messageInput.style.height = 'auto';
        }
    }

    focusMessageInput() {
        const messageInput = document.getElementById('messageInput');
        if (messageInput) {
            messageInput.focus();
        }
    }
}
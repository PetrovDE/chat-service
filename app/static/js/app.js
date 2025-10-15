// Main application entry point
import { ChatManager } from './chat-manager.js';
import { FileManager } from './file-manager.js';
import { UIController } from './ui-controller.js';
import { ApiService } from './api-service.js';
import { AuthManager } from './auth-manager.js';

class App {
    constructor() {
        this.chatManager = null;
        this.fileManager = null;
        this.uiController = null;
        this.apiService = null;
        this.authManager = null;
    }

    async init() {
        console.log('🚀 Initializing Llama Chat App...');

        try {
            // Initialize API service
            this.apiService = new ApiService();

            // Initialize managers with dependencies
            this.uiController = new UIController();
            this.authManager = new AuthManager(this.apiService, this.uiController);
            this.chatManager = new ChatManager(this.apiService, this.uiController);
            this.fileManager = new FileManager(this.apiService, this.uiController, this.chatManager);

            // Connect API service to UI controller for health updates
            this.apiService.setUIController(this.uiController);

            // Setup event listeners
            this.setupEventListeners();

            // Initialize model selector
            await this.uiController.initializeModelSelector();

            // Check authentication and update UI
            await this.initializeAuth();

            // Initial health check
            await this.apiService.checkHealth();

            // Start health monitoring
            this.startHealthMonitoring();

            console.log('✅ App initialized successfully');

        } catch (error) {
            console.error('❌ Failed to initialize app:', error);
            this.uiController.showError('Failed to initialize application');
        }
    }

    async initializeAuth() {  // ← Добавить весь метод
        try {
            if (this.authManager.isAuthenticated()) {
                // Try to get current user info
                const user = await this.authManager.getCurrentUserInfo();

                if (user) {
                    console.log('User authenticated:', user.username);
                    this.authManager.updateUIAfterAuth();
                } else {
                    console.log('Token invalid, clearing');
                    this.authManager.clearToken();
                }
            } else {
                console.log('User not authenticated');
            }
        } catch (error) {
            console.error('Error initializing auth:', error);
        }
    }

    setupEventListeners() {
        // Message input handling
        const messageInput = document.getElementById('messageInput');

        // Auto-resize textarea
        messageInput.addEventListener('input', (e) => {
            e.target.style.height = 'auto';
            e.target.style.height = Math.min(e.target.scrollHeight, 120) + 'px';
        });

        // Send message on Enter (Shift+Enter for new line)
        messageInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                this.chatManager.sendMessage();
            }
        });

        // Global click handler for modal management
        document.addEventListener('click', (e) => {
            this.uiController.handleGlobalClick(e);
        });

        // Settings form handlers
        this.setupSettingsHandlers();

        // Model settings handlers
        this.setupModelSettingsHandlers();
    }

    setupSettingsHandlers() {
        // Temperature control
        const tempInput = document.getElementById('temperatureInput');
        const tempValue = document.getElementById('temperatureValue');
        if (tempInput && tempValue) {
            tempInput.addEventListener('input', () => {
                tempValue.textContent = tempInput.value;
            });
        }

        // Max tokens control
        const tokensInput = document.getElementById('maxTokensInput');
        const tokensValue = document.getElementById('maxTokensValue');
        if (tokensInput && tokensValue) {
            tokensInput.addEventListener('input', () => {
                tokensValue.textContent = tokensInput.value;
            });
        }

        // Streaming toggle
        const streamingToggle = document.getElementById('streamingEnabled');
        if (streamingToggle) {
            streamingToggle.addEventListener('change', () => {
                this.chatManager.setStreamingEnabled(streamingToggle.checked);
            });
        }
    }

    setupModelSettingsHandlers() {
        // Source type radio buttons
        document.querySelectorAll('input[name="modelSource"]').forEach(radio => {
            radio.addEventListener('change', (e) => {
                const localSection = document.getElementById('localModelSection');
                const apiSection = document.getElementById('apiConfigSection');

                if (e.target.value === 'local') {
                    localSection.style.display = 'block';
                    apiSection.style.display = 'none';
                } else {
                    localSection.style.display = 'none';
                    apiSection.style.display = 'block';
                }
            });
        });
    }

    startHealthMonitoring() {
        // Check health every 30 seconds
        setInterval(() => {
            this.apiService.checkHealth();
        }, 30000);
    }
}

// Global functions for HTML onclick handlers
window.sendMessage = () => app.chatManager.sendMessage();
window.toggleFileModal = () => app.fileManager.toggleModal();
window.handleFileUpload = (event) => app.fileManager.handleFileUpload(event);
window.analyzeFile = () => app.fileManager.analyzeFile();
window.toggleSettings = () => app.uiController.toggleSettings();
window.closeSettings = () => app.uiController.closeSettings();
window.clearChat = () => app.chatManager.clearChat();

// Model settings functions
window.toggleModelSettings = () => {
    const panel = document.getElementById('modelSettingsPanel');
    panel.classList.toggle('show');

    // Load models if opening
    if (panel.classList.contains('show')) {
        app.uiController.loadLocalModels();
    }
};

window.closeModelSettings = () => {
    document.getElementById('modelSettingsPanel').classList.remove('show');
};

window.showApiConfig = () => {
    document.getElementById('apiConfigModal').classList.add('show');
    app.uiController.loadSavedApiConfig();
};

window.closeApiConfig = () => {
    document.getElementById('apiConfigModal').classList.remove('show');
};

// Initialize app when DOM is ready
const app = new App();
document.addEventListener('DOMContentLoaded', () => {
    app.init();
});

// Make app globally available for auth functions
window.app = app;
window.authManager = null;  // Will be set after init

// Export for module access
export default app;
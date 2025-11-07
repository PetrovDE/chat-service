// app/static/js/app.js
import { ApiService } from './api-service.js';
import { AuthManager } from './auth-manager.js';
import { ChatManager } from './chat-manager.js';
import { ConversationsManager } from './conversations-manager.js';
import { UIController } from './ui-controller.js';
import { FileManager } from './file-manager.js';
import { SettingsManager } from './settings-manager.js';
import './settings-ui.js';

console.log('✓ All modules imported successfully');

class App {
    constructor() {
        this.apiService = null;
        this.authManager = null;
        this.chatManager = null;
        this.conversationsManager = null;
        this.uiController = null;
        this.fileManager = null;
        this.settingsManager = null;
        this.initialized = false;
    }

    async initialize() {
        try {
            console.log('🚀 Initializing Llama Chat Application...');

            this.uiController = new UIController();
            console.log('✓ UI Controller initialized');

            this.apiService = new ApiService();
            console.log('✓ API Service initialized');

            this.authManager = new AuthManager(this.apiService, this.uiController);
            console.log('✓ Auth Manager initialized');

            this.settingsManager = new SettingsManager(this.apiService, this.uiController);
            console.log('✓ Settings Manager initialized');
            this.settingsManager.setupUI();

            this.chatManager = new ChatManager(this.apiService, this.uiController);
            console.log('✓ Chat Manager initialized');

            this.conversationsManager = new ConversationsManager(
                this.apiService,
                this.uiController,
                this.chatManager
            );
            console.log('✓ Conversations Manager initialized');

            this.fileManager = new FileManager(this.apiService, this.uiController, this.chatManager);
            console.log('✓ File Manager initialized');

            setTimeout(() => {
                this.fileManager.initializeFileInput();
            }, 100);

            this.setupEventListeners();
            console.log('✓ Event listeners setup');

            await this.authManager.checkAuthStatus();
            console.log('✓ Auth status checked');
            this.authManager.setupForms();

            if (this.authManager.isAuthenticated()) {
                try {
                    await this.conversationsManager.loadConversations();
                    console.log('✓ Conversations loaded');
                } catch (error) {
                    console.warn('⚠️ Could not load conversations:', error.message);
                    // Ignore error if endpoint doesn't exist
                }
            }

            await this.settingsManager.loadAvailableModels();
            console.log('✓ Models loaded');

            await this.checkSystemHealth();
            console.log('✓ System health checked');

            this.initialized = true;
            console.log('✅ Application initialized successfully!');
        } catch (error) {
            console.error('❌ Failed to initialize application:', error);

            const errorContainer = document.getElementById('chatMessages');
            if (errorContainer) {
                errorContainer.innerHTML = `
                    <div style="padding: 2rem; text-align: center; color: #ef4444;">
                        <h2>⚠️ Ошибка инициализации</h2>
                        <p>${error.message || 'Не удалось загрузить приложение'}</p>
                        <button onclick="location.reload()" style="margin-top: 1rem; padding: 0.5rem 1rem; background: #007aff; color: white; border: none; border-radius: 8px; cursor: pointer;">
                            Перезагрузить
                        </button>
                    </div>
                `;
            }
            throw error;
        }
    }

    setupEventListeners() {
        const sendButton = document.getElementById('sendMessage');
        if (sendButton) {
            sendButton.addEventListener('click', () => this.handleSendMessage());
        }

        const stopButton = document.getElementById('stopGeneration');
        if (stopButton) {
            stopButton.addEventListener('click', () => this.handleStopGeneration());
        }

        const messageInput = document.getElementById('messageInput');
        if (messageInput) {
            messageInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    this.handleSendMessage();
                }
            });

            messageInput.addEventListener('input', (e) => {
                e.target.style.height = 'auto';
                e.target.style.height = e.target.scrollHeight + 'px';
            });
        }

        const modeSelector = document.getElementById('mode-selector');
        if (modeSelector) {
            modeSelector.addEventListener('change', (e) => {
                this.settingsManager.setMode(e.target.value);
            });
        }

        const modelSelector = document.getElementById('model-selector');
        if (modelSelector) {
            modelSelector.addEventListener('change', (e) => {
                this.settingsManager.setModel(e.target.value);
            });
        }

        // Health check every 30 seconds
        setInterval(() => this.checkSystemHealth(), 30000);
    }

    async handleSendMessage() {
        const messageInput = document.getElementById('messageInput');
        if (!messageInput) return;

        const message = messageInput.value.trim();
        if (!message) return;

        console.log('📤 Sending message:', message);

        const conversationId = this.chatManager.getCurrentConversation();
        const settings = this.settingsManager.getSettings();

        await this.chatManager.sendMessage(message, conversationId, settings);

        messageInput.value = '';
        messageInput.style.height = 'auto';
    }

    handleStopGeneration() {
        if (this.chatManager) {
            this.chatManager.stopGeneration();
        }
    }

    async checkSystemHealth() {
        try {
            // ИСПРАВЛЕНО: используем специальный метод checkHealth()
            const health = await this.apiService.checkHealth();
            console.log('✓ System health:', health);

            const statusIndicator = document.getElementById('healthIndicator');
            const statusText = document.getElementById('healthStatus');

            if (statusIndicator && statusText) {
                if (health.status === 'healthy') {
                    statusIndicator.style.background = '#10b981';
                    statusText.textContent = 'Работает';
                } else {
                    statusIndicator.style.background = '#ef4444';
                    statusText.textContent = 'Недоступно';
                }
            }
        } catch (error) {
            console.error('❌ Health check failed:', error);
            const statusIndicator = document.getElementById('healthIndicator');
            const statusText = document.getElementById('healthStatus');
            if (statusIndicator && statusText) {
                statusIndicator.style.background = '#ef4444';
                statusText.textContent = 'Ошибка';
            }
        }
    }
}

document.addEventListener('DOMContentLoaded', async () => {
    console.log('DOM loaded, initializing app...');
    try {
        window.app = new App();
        await window.app.initialize();
    } catch (error) {
        console.error('App initialization failed:', error);
    }
});

export default App;

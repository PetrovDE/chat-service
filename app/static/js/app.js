// app/static/js/app.js
import { ApiService } from './api-service.js';
import { AuthManager } from './auth-manager.js';
import { ChatManager } from './chat-manager.js';
import { ConversationsManager } from './conversations-manager.js';
import { UIController } from './ui-controller.js';
import { FileManager } from './file-manager.js';
import { SettingsManager } from './settings-manager.js';

console.log('✓ All modules imported successfully');
console.log('  ApiService:', typeof ApiService);
console.log('  AuthManager:', typeof AuthManager);
console.log('  ChatManager:', typeof ChatManager);
console.log('  ConversationsManager:', typeof ConversationsManager);
console.log('  UIController:', typeof UIController);
console.log('  FileManager:', typeof FileManager);
console.log('  SettingsManager:', typeof SettingsManager);

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

            // 1. Initialize UI Controller (first, as others depend on it)
            this.uiController = new UIController();
            console.log('✓ UI Controller initialized');

            // 2. Initialize API Service
            this.apiService = new ApiService();
            console.log('✓ API Service initialized');

            // 3. Initialize Auth Manager
            this.authManager = new AuthManager(this.apiService, this.uiController);
            console.log('✓ Auth Manager initialized');

            // 4. Initialize Settings Manager
            console.log('Attempting to initialize Settings Manager...');
            console.log('  SettingsManager constructor:', SettingsManager);
            this.settingsManager = new SettingsManager(this.apiService, this.uiController);
            console.log('✓ Settings Manager initialized');
            console.log('  settingsManager instance:', this.settingsManager);

            // 5. Initialize Chat Manager
            console.log('Attempting to initialize Chat Manager...');
            this.chatManager = new ChatManager(this.apiService, this.uiController);
            console.log('✓ Chat Manager initialized');

            // 6. Initialize Conversations Manager
            this.conversationsManager = new ConversationsManager(
                this.apiService,
                this.uiController,
                this.chatManager
            );
            console.log('✓ Conversations Manager initialized');

            // 7. Initialize File Manager
            this.fileManager = new FileManager(this.apiService, this.uiController, this.chatManager);
            console.log('✓ File Manager initialized');
            console.log('  fileManager instance:', this.fileManager);
            console.log('  fileManager.openFileDialog:', typeof this.fileManager.openFileDialog);

            // Initialize file input after a short delay to ensure DOM is ready
            setTimeout(() => {
                this.fileManager.initializeFileInput();
            }, 100);

            // 8. Setup event listeners
            this.setupEventListeners();
            console.log('✓ Event listeners setup');

            // 9. Check authentication status
            await this.authManager.checkAuthStatus();
            console.log('✓ Auth status checked');

            // 10. Load conversations if authenticated
            if (this.authManager.isAuthenticated()) {
                await this.conversationsManager.loadConversations();
                console.log('✓ Conversations loaded');
            }

            // 11. Load available models
            await this.settingsManager.loadAvailableModels();
            console.log('✓ Models loaded');

            // 12. Check system health
            await this.checkSystemHealth();
            console.log('✓ System health checked');

            this.initialized = true;
            console.log('✅ Application initialized successfully!');

        } catch (error) {
            console.error('❌ Failed to initialize application:', error);

            // Show error to user
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
        // Send message button
        const sendButton = document.getElementById('sendMessage');
        if (sendButton) {
            sendButton.addEventListener('click', () => this.handleSendMessage());
        }

        // Stop generation button
        const stopButton = document.getElementById('stopGeneration');
        if (stopButton) {
            stopButton.addEventListener('click', () => this.handleStopGeneration());
        }

        // Message input (Enter to send)
        const messageInput = document.getElementById('messageInput');
        if (messageInput) {
            messageInput.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    this.handleSendMessage();
                }
            });

            // Auto-resize textarea
            messageInput.addEventListener('input', (e) => {
                e.target.style.height = 'auto';
                e.target.style.height = e.target.scrollHeight + 'px';
            });
        }

        // Sidebar toggle
        const sidebarToggle = document.getElementById('sidebarToggle');
        if (sidebarToggle) {
            sidebarToggle.addEventListener('click', () => window.toggleSidebar());
        }

        // Health status check (every 30 seconds)
        setInterval(() => this.checkSystemHealth(), 30000);
    }

    async handleSendMessage() {
        const messageInput = document.getElementById('messageInput');
        if (!messageInput) return;

        const message = messageInput.value.trim();
        if (!message) return;

        const conversationId = this.chatManager.getCurrentConversation();
        const settings = this.settingsManager.getSettings();

        await this.chatManager.sendMessage(message, conversationId, settings);
    }

    handleStopGeneration() {
        if (this.chatManager) {
            this.chatManager.stopGeneration();
        }
    }

    async checkSystemHealth() {
        try {
            const health = await this.apiService.checkHealth();

            const statusIndicator = document.getElementById('healthIndicator');
            const statusText = document.getElementById('healthStatus');

            if (statusIndicator && statusText) {
                if (health.status === 'healthy') {
                    statusIndicator.style.background = '#10b981'; // green
                    statusText.textContent = 'Работает';
                } else if (health.status === 'degraded') {
                    statusIndicator.style.background = '#f59e0b'; // orange
                    statusText.textContent = 'Ограничено';
                } else {
                    statusIndicator.style.background = '#ef4444'; // red
                    statusText.textContent = 'Недоступно';
                }
            }
        } catch (error) {
            console.error('Health check failed:', error);

            const statusIndicator = document.getElementById('healthIndicator');
            const statusText = document.getElementById('healthStatus');

            if (statusIndicator && statusText) {
                statusIndicator.style.background = '#ef4444';
                statusText.textContent = 'Ошибка';
            }
        }
    }
}

// Initialize app when DOM is ready
document.addEventListener('DOMContentLoaded', async () => {
    console.log('DOM loaded, initializing app...');

    try {
        window.app = new App();
        await window.app.initialize();
    } catch (error) {
        console.error('App initialization failed:', error);
    }
});

// Export for use in HTML onclick handlers
export default App;
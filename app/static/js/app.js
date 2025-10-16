// Main application entry point
import { ChatManager } from './chat-manager.js';
import { FileManager } from './file-manager.js';
import { UIController } from './ui-controller.js';
import { ApiService } from './api-service.js';
import { AuthManager } from './auth-manager.js';
import { ConversationsManager } from './conversations-manager.js';

class App {
    constructor() {
        this.chatManager = null;
        this.fileManager = null;
        this.uiController = null;
        this.apiService = null;
        this.authManager = null;
        this.conversationsManager = null;
    }

    async init() {
        console.log('🚀 Initializing Llama Chat App...');

        try {
            // Initialize API service
            this.apiService = new ApiService();

            // Initialize managers with dependencies
            this.uiController = new UIController();
            this.authManager = new AuthManager(this.apiService, this.uiController);
            this.conversationsManager = new ConversationsManager(
                this.apiService,
                this.uiController,
                this.authManager
            );
            this.chatManager = new ChatManager(
                this.apiService,
                this.uiController,
                this.conversationsManager
            );
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

    async initializeAuth() {
        try {
            if (this.authManager.isAuthenticated()) {
                const user = await this.authManager.getCurrentUserInfo();

                if (user) {
                    console.log('User authenticated:', user.username);
                    this.authManager.updateUIAfterAuth();
                    await this.conversationsManager.loadConversations();
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
        // Send message button
        const sendBtn = document.getElementById('sendMessage');
        if (sendBtn) {
            sendBtn.addEventListener('click', () => this.handleSendMessage());
        }

        // Enter key in input
        const messageInput = document.getElementById('messageInput');
        if (messageInput) {
            messageInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    this.handleSendMessage();
                }
            });
        }

        // File upload
        const fileUpload = document.getElementById('fileUpload');
        if (fileUpload) {
            fileUpload.addEventListener('change', (e) => this.handleFileUpload(e));
        }

        // Clear chat
        const clearBtn = document.getElementById('clearChat');
        if (clearBtn) {
            clearBtn.addEventListener('click', () => this.handleClearChat());
        }
    }

    async handleSendMessage() {
        const input = document.getElementById('messageInput');
        const message = input.value.trim();

        if (!message) return;

        // Clear input
        input.value = '';

        // Send to chat manager
        await this.chatManager.sendMessage(message);
    }

    async handleFileUpload(event) {
        const file = event.target.files[0];
        if (!file) return;

        await this.fileManager.uploadFile(file);

        // Reset input
        event.target.value = '';
    }

    handleClearChat() {
        if (confirm('Clear all messages in current chat?')) {
            this.chatManager.clearChat();
            this.conversationsManager.createNewConversation();
        }
    }

    startHealthMonitoring() {
        // Check health every 30 seconds
        setInterval(() => {
            this.apiService.checkHealth();
        }, 30000);
    }
}

// Initialize app when DOM is ready
const app = new App();

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => app.init());
} else {
    app.init();
}

// Make app globally available
window.app = app;

export default app;
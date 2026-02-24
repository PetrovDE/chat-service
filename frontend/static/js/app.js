import { ApiService } from './api-service.js';
import { AuthManager } from './auth-manager.js';
import { ChatManager } from './chat-manager.js';
import { ConversationsManager } from './conversations-manager.js';
import { UIController } from './ui-controller.js';
import { FileManager } from './file-manager.js';
import { SettingsManager } from './settings-manager.js';
import { FilesSidebarManager } from './files-sidebar-manager.js';
import { ThemeManager } from './theme-manager.js';
import './settings-ui.js';

class App {
    constructor() {
        this.apiService = null;
        this.authManager = null;
        this.chatManager = null;
        this.conversationsManager = null;
        this.uiController = null;
        this.fileManager = null;
        this.settingsManager = null;
        this.filesSidebarManager = null;
        this.themeManager = null;
        this.initialized = false;
    }

    async initialize() {
        this.uiController = new UIController();
        this.apiService = new ApiService();
        this.authManager = new AuthManager(this.apiService, this.uiController);
        this.settingsManager = new SettingsManager(this.apiService, this.uiController);
        this.chatManager = new ChatManager(this.apiService, this.uiController);

        this.conversationsManager = new ConversationsManager(
            this.apiService,
            this.uiController,
            this.chatManager
        );

        this.chatManager.setConversationsManager(this.conversationsManager);
        this.fileManager = new FileManager(this.chatManager);
        this.filesSidebarManager = new FilesSidebarManager(this.apiService, this.uiController);
        this.themeManager = new ThemeManager();

        this.themeManager.init();
        this.settingsManager.setupUI();
        this.setupEventListeners();

        await this.authManager.checkAuthStatus();
        this.authManager.setupForms();
        this.conversationsManager.bindSearchInput();

        if (this.authManager.isAuthenticated()) {
            await this.conversationsManager.loadConversations();
            this.filesSidebarManager.initialize();
        } else {
            this.chatManager.renderWelcomeState();
        }

        await this.settingsManager.loadAvailableModels();
        await this.checkSystemHealth();
        this.fileManager.initializeFileInput();

        this.initialized = true;
    }

    setupEventListeners() {
        const syncSidebarState = () => {
            const sidebarToggle = document.getElementById('sidebarToggle');
            const mobileSidebarToggle = document.getElementById('mobileSidebarToggle');
            const isDesktopCollapsed = document.body.classList.contains('sidebar-collapsed');
            const isMobileOpen = !document.body.classList.contains('sidebar-collapsed-mobile');

            if (sidebarToggle) {
                sidebarToggle.textContent = isDesktopCollapsed ? 'Show' : 'Hide';
                sidebarToggle.setAttribute('aria-expanded', String(!isDesktopCollapsed));
            }

            if (mobileSidebarToggle) {
                mobileSidebarToggle.setAttribute('aria-expanded', String(isMobileOpen));
            }
        };

        const sendButton = document.getElementById('sendMessage');
        if (sendButton) sendButton.addEventListener('click', () => this.handleSendMessage());

        const stopButton = document.getElementById('stopGeneration');
        if (stopButton) stopButton.addEventListener('click', () => this.handleStopGeneration());

        const messageInput = document.getElementById('messageInput');
        if (messageInput) {
            messageInput.addEventListener('keydown', (event) => {
                if (event.key === 'Enter' && !event.shiftKey) {
                    event.preventDefault();
                    this.handleSendMessage();
                }
            });

            messageInput.addEventListener('input', (event) => {
                event.target.style.height = 'auto';
                event.target.style.height = `${Math.min(event.target.scrollHeight, 220)}px`;
            });
        }

        const modeSelector = document.getElementById('mode-selector');
        if (modeSelector) {
            modeSelector.addEventListener('change', async (event) => {
                const newMode = event.target.value;
                this.settingsManager.setMode(newMode);
                await this.settingsManager.loadAvailableModels(newMode);
            });
        }

        const modelSelector = document.getElementById('model-selector');
        if (modelSelector) {
            modelSelector.addEventListener('change', (event) => {
                this.settingsManager.setModel(event.target.value);
            });
        }

        const sidebarToggle = document.getElementById('sidebarToggle');
        if (sidebarToggle) {
            sidebarToggle.addEventListener('click', () => {
                document.body.classList.toggle('sidebar-collapsed');
                syncSidebarState();
            });
        }

        const sidebarRestore = document.getElementById('sidebarRestore');
        if (sidebarRestore) {
            sidebarRestore.addEventListener('click', () => {
                document.body.classList.remove('sidebar-collapsed');
                if (sidebarToggle) sidebarToggle.textContent = 'Hide';
            });
        }

        const mobileSidebarToggle = document.getElementById('mobileSidebarToggle');
        if (mobileSidebarToggle) {
            mobileSidebarToggle.addEventListener('click', () => {
                const isMobile = window.matchMedia('(max-width: 980px)').matches;

                if (isMobile) {
                    document.body.classList.toggle('sidebar-collapsed-mobile');
                } else {
                    document.body.classList.toggle('sidebar-collapsed');
                }

                syncSidebarState();
            });
        }

        window.addEventListener('resize', syncSidebarState);
        syncSidebarState();

        setInterval(() => this.checkSystemHealth(), 30000);
    }

    async handleSendMessage() {
        const messageInput = document.getElementById('messageInput');
        if (!messageInput) return;

        const message = messageInput.value.trim();
        if (!message) return;

        const conversationId = this.chatManager.getCurrentConversation();
        const settings = this.settingsManager.getSettings();

        try {
            await this.chatManager.sendMessage(message, conversationId, settings);
            messageInput.value = '';
            messageInput.style.height = 'auto';
        } catch (error) {
            this.uiController.showError(error.message || 'Message send failed');
        }
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
                const healthy = health.status === 'healthy';
                statusIndicator.classList.toggle('is-healthy', healthy);
                statusIndicator.classList.toggle('is-unhealthy', !healthy);
                statusText.textContent = healthy ? 'Online' : 'Degraded';
            }
        } catch (_) {
            const statusIndicator = document.getElementById('healthIndicator');
            const statusText = document.getElementById('healthStatus');

            if (statusIndicator && statusText) {
                statusIndicator.classList.remove('is-healthy');
                statusIndicator.classList.add('is-unhealthy');
                statusText.textContent = 'Offline';
            }
        }
    }

    refreshFilesSidebar() {
        if (this.filesSidebarManager && this.authManager.isAuthenticated()) {
            this.filesSidebarManager.loadFiles(true);
        }
    }
}

document.addEventListener('DOMContentLoaded', async () => {
    try {
        window.app = new App();
        await window.app.initialize();
    } catch (error) {
        const errorContainer = document.getElementById('chatMessages');
        if (errorContainer) {
            errorContainer.innerHTML = `
                <section class="chat-empty-state chat-error-state">
                    <h2>Failed to initialize app</h2>
                    <p>${error.message || 'Unexpected startup error'}</p>
                    <button class="new-chat-btn" onclick="location.reload()" type="button">Reload</button>
                </section>
            `;
        }
    }
});

export default App;

// app/static/js/auth-manager.js
export class AuthManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.currentUser = null;
        this.token = null;

        // Load token from localStorage
        this.loadToken();
    }

    loadToken() {
        const stored = localStorage.getItem('auth_token');
        if (stored) {
            try {
                const data = JSON.parse(stored);
                this.token = data.token;
                this.currentUser = data.user;
                console.log('Loaded token for user:', this.currentUser?.username);
            } catch (e) {
                console.error('Error loading token:', e);
                this.clearToken();
            }
        }
    }

    saveToken(token, user) {
        this.token = token;
        this.currentUser = user;

        localStorage.setItem('auth_token', JSON.stringify({
            token: token,
            user: user
        }));

        console.log('Token saved for user:', user.username);
    }

    clearToken() {
        this.token = null;
        this.currentUser = null;
        localStorage.removeItem('auth_token');
        console.log('Token cleared');
    }

    getToken() {
        return this.token;
    }

    getCurrentUser() {
        return this.currentUser;
    }

    isAuthenticated() {
        return this.token !== null;
    }

    async register(username, email, password, fullName) {
        try {
            const response = await this.apiService.register(username, email, password, fullName);

            this.saveToken(response.access_token, response.user);
            this.updateUIAfterAuth();

            return { success: true, user: response.user };
        } catch (error) {
            console.error('Registration error:', error);
            return { success: false, error: error.message };
        }
    }

    async login(username, password) {
        try {
            const response = await this.apiService.login(username, password);

            this.saveToken(response.access_token, response.user);
            this.updateUIAfterAuth();

            return { success: true, user: response.user };
        } catch (error) {
            console.error('Login error:', error);
            return { success: false, error: error.message };
        }
    }

    async logout() {
        try {
            if (this.isAuthenticated()) {
                await this.apiService.logout();
            }
        } catch (error) {
            console.error('Logout error:', error);
        } finally {
            this.clearToken();
            this.updateUIAfterLogout();
        }
    }

    async checkAuthStatus() {
        if (!this.isAuthenticated()) {
            this.updateUIAfterLogout();
            return false;
        }

        try {
            const user = await this.apiService.getCurrentUser();
            this.currentUser = user;

            if (this.token) {
                this.saveToken(this.token, user);
            }

            this.updateUIAfterAuth();
            return true;

        } catch (error) {
            console.error('Error checking auth status:', error);

            if (error.message.includes('401') || error.message.includes('Unauthorized')) {
                this.clearToken();
                this.updateUIAfterLogout();
            }

            return false;
        }
    }

    updateUIAfterAuth() {
        const authSection = document.getElementById('authSection');

        if (this.currentUser && authSection) {
            authSection.innerHTML = `
                <div class="user-profile-btn" onclick="toggleUserMenu()">
                    <div class="user-avatar">
                        ${this.currentUser.username.charAt(0).toUpperCase()}
                    </div>
                    <span class="user-display-name">${this.currentUser.username}</span>
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <polyline points="6 9 12 15 18 9"></polyline>
                    </svg>
                </div>
                <div class="user-menu-dropdown" id="userMenuDropdown" style="display: none;">
                    <div class="user-menu-header">
                        <div class="user-avatar">
                            ${this.currentUser.username.charAt(0).toUpperCase()}
                        </div>
                        <div class="user-info">
                            <div class="user-name">${this.currentUser.full_name || this.currentUser.username}</div>
                            <div class="user-email">${this.currentUser.email}</div>
                        </div>
                    </div>
                    <div class="user-menu-divider"></div>
                    <button class="user-menu-item" onclick="showProfile()">
                        👤 Профиль
                    </button>
                    <button class="user-menu-item" onclick="showSettings()">
                        ⚙️ Настройки
                    </button>
                    <div class="user-menu-divider"></div>
                    <button class="user-menu-item" onclick="handleLogout()">
                        🚪 Выйти
                    </button>
                </div>
            `;
        }

        // Load conversations if available
        if (window.app && window.app.conversationsManager) {
            window.app.conversationsManager.loadConversations();
        }
    }

    updateUIAfterLogout() {
        const authSection = document.getElementById('authSection');

        if (authSection) {
            authSection.innerHTML = `
                <button class="login-btn" onclick="showLogin()">Войти</button>
            `;
        }

        const conversationsList = document.getElementById('conversationsList');
        if (conversationsList) {
            conversationsList.innerHTML = '<div class="conversations-loading">Войдите для просмотра истории</div>';
        }

        // Clear messages
        if (this.uiController) {
            this.uiController.clearMessages();
        }
    }
}
// app/static/js/auth-manager.js

class AuthManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.authenticated = false;
        this.currentUser = null;
        console.log('✓ AuthManager initialized');
    }

    async checkAuthStatus() {
        console.log('🔐 Checking auth status');
        try {
            const token = localStorage.getItem('auth_token');
            if (token) {
                this.authenticated = true;
                console.log('✓ User authenticated');
            } else {
                this.authenticated = false;
                console.log('⚠️ User not authenticated');
            }
        } catch (error) {
            console.error('❌ Auth check error:', error);
        }
    }

    isAuthenticated() {
        return this.authenticated;
    }

    async login(username, password) {
        console.log('🔑 Logging in...');
        try {
            const response = await this.apiService.post('/auth/login', { username, password });
            if (response.access_token) {
                localStorage.setItem('auth_token', response.access_token);
                this.authenticated = true;
                this.currentUser = response.user;
                console.log('✅ Login successful');
            }
            return response;
        } catch (error) {
            console.error('❌ Login error:', error);
            throw error;
        }
    }

    logout() {
        localStorage.removeItem('auth_token');
        this.authenticated = false;
        this.currentUser = null;
        console.log('👋 Logged out');
    }
}

export { AuthManager };

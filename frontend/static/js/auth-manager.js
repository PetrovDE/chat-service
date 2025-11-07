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

    async register(username, password, email) {
        console.log('📝 Registering new user...');
        try {
            const response = await this.apiService.post('/auth/register', {
                username,
                password,
                email
            });
            console.log('✅ Registration successful');
            return response;
        } catch (error) {
            console.error('❌ Registration error:', error);
            throw error;
        }
    }

    logout() {
        localStorage.removeItem('auth_token');
        this.authenticated = false;
        this.currentUser = null;
        console.log('👋 Logged out');
    }

    setupForms() {
        this.setupLoginForm();
        this.setupRegisterForm();
        this.setupGlobalHelpers();
        console.log('✓ Auth forms setup complete');
    }

    setupLoginForm() {
        const loginForm = document.getElementById('loginForm');
        if (!loginForm) return;

        loginForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const username = document.getElementById('loginUsername').value;
            const password = document.getElementById('loginPassword').value;
            const errorDiv = document.getElementById('loginError');

            try {
                await this.login(username, password);
                this.closeAuthModals();
                location.reload();
            } catch (error) {
                errorDiv.textContent = error.message || 'Ошибка входа';
                errorDiv.style.display = 'block';
            }
        });

        console.log('✓ Login form initialized');
    }

    setupRegisterForm() {
        const registerForm = document.getElementById('registerForm');
        if (!registerForm) return;

        registerForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const username = document.getElementById('registerUsername').value;
            const password = document.getElementById('registerPassword').value;
            const passwordConfirm = document.getElementById('registerPasswordConfirm').value;
            const errorDiv = document.getElementById('registerError');

            if (password !== passwordConfirm) {
                errorDiv.textContent = 'Пароли не совпадают';
                errorDiv.style.display = 'block';
                return;
            }

            if (password.length < 8) {
                errorDiv.textContent = 'Пароль должен быть минимум 8 символов';
                errorDiv.style.display = 'block';
                return;
            }

            if (username.length < 3) {
                errorDiv.textContent = 'Имя пользователя должно быть минимум 3 символа';
                errorDiv.style.display = 'block';
                return;
            }

            try {
                await this.register(username, password, `${username}@example.com`);
                alert('Регистрация успешна! Войдите с вашими данными.');
                this.closeAuthModals();
                this.showLogin();
            } catch (error) {
                errorDiv.textContent = error.message || 'Ошибка регистрации';
                errorDiv.style.display = 'block';
            }
        });

        console.log('✓ Register form initialized');
    }

    closeAuthModals() {
        const loginModal = document.getElementById('loginModal');
        const registerModal = document.getElementById('registerModal');
        const authOverlay = document.getElementById('authOverlay');

        if (loginModal) loginModal.style.display = 'none';
        if (registerModal) registerModal.style.display = 'none';
        if (authOverlay) authOverlay.classList.remove('show');
    }

    showLogin() {
        this.closeAuthModals();
        const loginModal = document.getElementById('loginModal');
        const authOverlay = document.getElementById('authOverlay');

        if (loginModal && authOverlay) {
            authOverlay.classList.add('show');
            loginModal.style.display = 'flex';
            console.log('✓ Login modal opened');
        }
    }

    showRegister() {
        this.closeAuthModals();
        const registerModal = document.getElementById('registerModal');
        const authOverlay = document.getElementById('authOverlay');

        if (registerModal && authOverlay) {
            authOverlay.classList.add('show');
            registerModal.style.display = 'flex';
            console.log('✓ Register modal opened');
        }
    }

    setupGlobalHelpers() {
        // КРИТИЧНО: Глобальные функции для onclick в HTML
        window.showLogin = () => this.showLogin();
        window.showRegister = () => this.showRegister();
        window.switchToRegister = () => this.showRegister();
        window.switchToLogin = () => this.showLogin();
        window.closeAuthModals = () => this.closeAuthModals();

        // Привязка кнопок через addEventListener
        const loginBtn = document.getElementById('loginBtn');
        if (loginBtn) {
            loginBtn.addEventListener('click', (e) => {
                e.preventDefault();
                this.showLogin();
            });
            console.log('✓ Login button bound');
        }

        const settingsBtn = document.getElementById('settingsBtn');
        if (settingsBtn) {
            settingsBtn.addEventListener('click', (e) => {
                e.preventDefault();
                if (window.toggleSettings) {
                    window.toggleSettings();
                }
            });
            console.log('✓ Settings button bound');
        }

        console.log('✓ Global auth helpers initialized');
    }
}

export { AuthManager };

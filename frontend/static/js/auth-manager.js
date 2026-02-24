class AuthManager {
    constructor(apiService, uiController) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.authenticated = false;
        this.currentUser = null;
    }

    async loadCurrentUser() {
        try {
            const user = await this.apiService.get('/auth/me');
            this.currentUser = user;
            this.authenticated = true;
            this.updateLoginButton(true, user.username);
            this.updateProfileUI(user.username, true);
        } catch (_) {
            localStorage.removeItem('auth_token');
            this.authenticated = false;
            this.currentUser = null;
            this.updateLoginButton(false);
            this.updateProfileUI('Guest', false);
        }
    }

    updateProfileUI(username, isAuthenticated) {
        const profileUsername = document.getElementById('profileUsername');
        const logoutBtn = document.getElementById('logoutBtn');
        if (profileUsername) profileUsername.textContent = username;
        if (logoutBtn) logoutBtn.style.display = isAuthenticated ? 'inline-flex' : 'none';
    }

    updateLoginButton(isAuthenticated, username = '') {
        const loginBtn = document.getElementById('loginBtn');
        if (!loginBtn) return;

        if (isAuthenticated && username) {
            loginBtn.textContent = username;
            loginBtn.onclick = (event) => {
                event.preventDefault();
                if (window.toggleSettings) {
                    window.toggleSettings();
                }
            };
        } else {
            loginBtn.textContent = 'Login';
            loginBtn.onclick = (event) => {
                event.preventDefault();
                this.showLogin();
            };
        }
    }

    async checkAuthStatus() {
        const token = localStorage.getItem('auth_token');
        if (!token) {
            this.authenticated = false;
            this.updateLoginButton(false);
            this.updateProfileUI('Guest', false);
            return;
        }

        await this.loadCurrentUser();
    }

    isAuthenticated() {
        return this.authenticated;
    }

    async login(username, password) {
        const response = await this.apiService.post('/auth/login', { username, password });
        if (!response.access_token) {
            throw new Error('Token missing in response');
        }

        localStorage.setItem('auth_token', response.access_token);
        await this.loadCurrentUser();
        return response;
    }

    async register(username, password, email) {
        return this.apiService.post('/auth/register', {
            username,
            password,
            email,
        });
    }

    logout() {
        localStorage.removeItem('auth_token');
        this.authenticated = false;
        this.currentUser = null;
        this.updateLoginButton(false);
        this.updateProfileUI('Guest', false);
    }

    setupForms() {
        this.setupLoginForm();
        this.setupRegisterForm();
        this.setupGlobalHelpers();
    }

    setupLoginForm() {
        const loginForm = document.getElementById('loginForm');
        if (!loginForm) return;

        loginForm.addEventListener('submit', async (event) => {
            event.preventDefault();

            const username = document.getElementById('loginUsername')?.value || '';
            const password = document.getElementById('loginPassword')?.value || '';
            const errorDiv = document.getElementById('loginError');

            try {
                await this.login(username, password);
                this.closeAuthModals();
                location.reload();
            } catch (error) {
                if (errorDiv) {
                    errorDiv.textContent = error.message || 'Invalid credentials';
                    errorDiv.style.display = 'block';
                }
            }
        });
    }

    setupRegisterForm() {
        const registerForm = document.getElementById('registerForm');
        if (!registerForm) return;

        registerForm.addEventListener('submit', async (event) => {
            event.preventDefault();

            const username = document.getElementById('registerUsername')?.value || '';
            const password = document.getElementById('registerPassword')?.value || '';
            const passwordConfirm = document.getElementById('registerPasswordConfirm')?.value || '';
            const errorDiv = document.getElementById('registerError');

            if (password !== passwordConfirm) {
                if (errorDiv) {
                    errorDiv.textContent = 'Passwords do not match';
                    errorDiv.style.display = 'block';
                }
                return;
            }

            try {
                await this.register(username, password, `${username}@example.com`);
                this.closeAuthModals();
                this.showLogin();
                this.uiController.showSuccess('Account created, now login');
            } catch (error) {
                if (errorDiv) {
                    errorDiv.textContent = error.message || 'Registration failed';
                    errorDiv.style.display = 'block';
                }
            }
        });
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
        }
    }

    showRegister() {
        this.closeAuthModals();
        const registerModal = document.getElementById('registerModal');
        const authOverlay = document.getElementById('authOverlay');

        if (registerModal && authOverlay) {
            authOverlay.classList.add('show');
            registerModal.style.display = 'flex';
        }
    }

    setupGlobalHelpers() {
        window.showLogin = () => this.showLogin();
        window.showRegister = () => this.showRegister();
        window.switchToRegister = () => this.showRegister();
        window.switchToLogin = () => this.showLogin();
        window.closeAuthModals = () => this.closeAuthModals();

        const settingsBtn = document.getElementById('settingsBtn');
        if (settingsBtn) {
            settingsBtn.addEventListener('click', (event) => {
                event.preventDefault();
                if (window.toggleSettings) window.toggleSettings();
            });
        }
    }
}

export { AuthManager };

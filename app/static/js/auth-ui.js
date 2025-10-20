// app/static/js/auth-ui.js
// Auth UI functions (called from HTML onclick handlers)

window.showLogin = function() {
    const overlay = document.getElementById('authOverlay');
    const loginModal = document.getElementById('loginModal');
    const registerModal = document.getElementById('registerModal');

    overlay.classList.add('show');
    loginModal.classList.add('show');
    registerModal.classList.remove('show');

    document.getElementById('loginForm').reset();
    document.getElementById('loginError').style.display = 'none';
};

window.showRegister = function() {
    const overlay = document.getElementById('authOverlay');
    const loginModal = document.getElementById('loginModal');
    const registerModal = document.getElementById('registerModal');

    overlay.classList.add('show');
    registerModal.classList.add('show');
    loginModal.classList.remove('show');

    document.getElementById('registerForm').reset();
    document.getElementById('registerError').style.display = 'none';
};

window.switchToLogin = function() {
    window.showLogin();
};

window.switchToRegister = function() {
    window.showRegister();
};

window.closeAuthModals = function() {
    const overlay = document.getElementById('authOverlay');
    const loginModal = document.getElementById('loginModal');
    const registerModal = document.getElementById('registerModal');

    overlay.classList.remove('show');
    loginModal.classList.remove('show');
    registerModal.classList.remove('show');
};

window.handleLogin = async function(event) {
    event.preventDefault();

    const username = document.getElementById('loginUsername').value;
    const password = document.getElementById('loginPassword').value;
    const errorDiv = document.getElementById('loginError');
    const submitBtn = event.target.querySelector('button[type="submit"]');

    errorDiv.style.display = 'none';

    if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.textContent = 'Вход...';
    }

    try {
        if (!window.app || !window.app.authManager) {
            throw new Error('Application not initialized');
        }

        const result = await window.app.authManager.login(username, password);

        if (result.success) {
            window.closeAuthModals();

            if (window.app.uiController) {
                window.app.uiController.showSuccess(`Добро пожаловать, ${result.user.username}!`);
            }

            // Load conversations
            if (window.app.conversationsManager) {
                await window.app.conversationsManager.loadConversations();
            }
        } else {
            errorDiv.textContent = result.error || 'Ошибка входа. Проверьте данные.';
            errorDiv.style.display = 'block';
        }
    } catch (error) {
        console.error('Login error:', error);
        errorDiv.textContent = 'Ошибка подключения к серверу';
        errorDiv.style.display = 'block';
    } finally {
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.textContent = 'Войти';
        }
    }
};

window.handleRegister = async function(event) {
    event.preventDefault();

    const username = document.getElementById('registerUsername').value;
    const email = document.getElementById('registerEmail').value;
    const fullName = document.getElementById('registerFullName').value;
    const password = document.getElementById('registerPassword').value;
    const errorDiv = document.getElementById('registerError');
    const submitBtn = event.target.querySelector('button[type="submit"]');

    errorDiv.style.display = 'none';

    if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.textContent = 'Регистрация...';
    }

    try {
        if (!window.app || !window.app.authManager) {
            throw new Error('Application not initialized');
        }

        const result = await window.app.authManager.register(username, email, password, fullName);

        if (result.success) {
            window.closeAuthModals();

            if (window.app.uiController) {
                window.app.uiController.showSuccess(`Добро пожаловать, ${result.user.username}!`);
            }

            // Load conversations
            if (window.app.conversationsManager) {
                await window.app.conversationsManager.loadConversations();
            }
        } else {
            errorDiv.textContent = result.error || 'Ошибка регистрации';
            errorDiv.style.display = 'block';
        }
    } catch (error) {
        console.error('Registration error:', error);
        errorDiv.textContent = 'Ошибка подключения к серверу';
        errorDiv.style.display = 'block';
    } finally {
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.textContent = 'Зарегистрироваться';
        }
    }
};

window.handleLogout = async function() {
    if (confirm('Вы уверены, что хотите выйти?')) {
        const dropdown = document.getElementById('userMenuDropdown');
        if (dropdown) {
            dropdown.style.display = 'none';
        }

        if (window.app && window.app.authManager) {
            await window.app.authManager.logout();

            if (window.app.uiController) {
                window.app.uiController.showSuccess('Вы вышли из системы');
            }
        }
    }
};

window.toggleUserMenu = function() {
    const dropdown = document.getElementById('userMenuDropdown');
    if (dropdown) {
        dropdown.style.display = dropdown.style.display === 'none' ? 'block' : 'none';
    }
};

window.showProfile = function() {
    const dropdown = document.getElementById('userMenuDropdown');
    if (dropdown) {
        dropdown.style.display = 'none';
    }

    if (window.app && window.app.uiController) {
        window.app.uiController.showSuccess('Профиль пользователя - в разработке');
    } else {
        alert('Профиль пользователя - в разработке');
    }
};

window.showSettings = function() {
    const dropdown = document.getElementById('userMenuDropdown');
    if (dropdown) {
        dropdown.style.display = 'none';
    }

    if (window.app && window.app.uiController) {
        window.app.uiController.showSuccess('Настройки - в разработке');
    } else {
        alert('Настройки - в разработке');
    }
};

// Close dropdown when clicking outside
document.addEventListener('click', function(event) {
    const dropdown = document.getElementById('userMenuDropdown');
    const userBtn = document.querySelector('.user-profile-btn');

    if (dropdown && userBtn) {
        if (!dropdown.contains(event.target) && !userBtn.contains(event.target)) {
            dropdown.style.display = 'none';
        }
    }
});

// Close auth modals on overlay click
document.addEventListener('DOMContentLoaded', function() {
    const overlay = document.getElementById('authOverlay');
    if (overlay) {
        overlay.addEventListener('click', window.closeAuthModals);
    }
});
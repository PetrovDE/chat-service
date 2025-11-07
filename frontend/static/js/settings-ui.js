// app/static/js/settings-ui.js

// Глобальные функции для работы с настройками
window.toggleSettings = function() {
    const settingsPanel = document.getElementById('settingsPanel');
    if (settingsPanel) {
        if (settingsPanel.classList.contains('show')) {
            settingsPanel.classList.remove('show');
        } else {
            settingsPanel.classList.add('show');
        }
    }
};

window.closeSettings = function() {
    const settingsPanel = document.getElementById('settingsPanel');
    if (settingsPanel) {
        settingsPanel.classList.remove('show');
    }
};

window.saveSettings = function() {
    if (window.app && window.app.settingsManager) {
        window.app.settingsManager.applySettings();
        window.closeSettings();
        console.log('✅ Settings saved');
    }
};

window.handleLogout = function() {
    if (window.app && window.app.authManager) {
        window.app.authManager.logout();
        window.closeSettings();
        location.reload();
    }
};

// Глобальные функции для разговоров
window.startNewConversation = function() {
    if (window.app && window.app.conversationsManager) {
        window.app.conversationsManager.createNewConversation();
    }
};

window.toggleSidebar = function() {
    document.body.classList.toggle('sidebar-collapsed');
};

window.clearAttachedFiles = function() {
    if (window.app && window.app.fileManager) {
        window.app.fileManager.clearAttachedFiles();
    }
};

console.log('✓ Settings UI functions loaded');

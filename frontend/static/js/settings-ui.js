window.toggleSettings = function() {
    const settingsPanel = document.getElementById('settingsPanel');
    if (!settingsPanel) return;
    settingsPanel.classList.toggle('show');
};

window.closeSettings = function() {
    const settingsPanel = document.getElementById('settingsPanel');
    if (!settingsPanel) return;
    settingsPanel.classList.remove('show');
};

window.saveSettings = function() {
    if (window.app?.settingsManager) {
        window.app.settingsManager.applySettings();
        window.closeSettings();
    }
};

window.handleLogout = function() {
    if (window.app?.authManager) {
        window.app.authManager.logout();
        window.closeSettings();
        location.reload();
    }
};

window.startNewConversation = function() {
    if (window.app?.conversationsManager) {
        window.app.conversationsManager.createNewConversation();
    }
};

window.toggleSidebar = function() {
    document.body.classList.toggle('sidebar-collapsed');
};

window.clearAttachedFiles = function() {
    if (window.app?.fileManager) {
        window.app.fileManager.clearAttachedFiles();
    }
};

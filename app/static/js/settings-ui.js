// app/static/js/settings-ui.js
// Global functions for settings UI

window.openSettings = function() {
    console.log('openSettings called');
    console.log('window.app:', window.app);
    console.log('window.app.settingsManager:', window.app?.settingsManager);

    if (window.app && window.app.settingsManager) {
        window.app.settingsManager.openSettingsPanel();
    } else {
        console.error('SettingsManager not initialized');
        alert('Настройки недоступны. Проверьте консоль для деталей.');
    }
};

window.closeSettingsPanel = function() {
    const panel = document.getElementById('settingsPanel');
    if (panel) {
        panel.classList.remove('show');
        setTimeout(() => panel.remove(), 300);
    }
};

window.saveSettings = async function() {
    if (window.app && window.app.settingsManager) {
        const success = await window.app.settingsManager.applySettings();
        if (success) {
            window.closeSettingsPanel();
        }
    }
};

window.resetSettings = function() {
    if (confirm('Сбросить все настройки к значениям по умолчанию?')) {
        if (window.app && window.app.settingsManager) {
            window.app.settingsManager.resetToDefaults();
        }
    }
};

window.openFileUpload = function() {
    console.log('openFileUpload called');
    console.log('window.app:', window.app);
    console.log('window.app.fileManager:', window.app?.fileManager);

    if (window.app && window.app.fileManager) {
        console.log('Calling openFileDialog...');
        window.app.fileManager.openFileDialog();
    } else {
        console.error('FileManager not initialized');
        alert('Загрузка файлов недоступна. Проверьте консоль для деталей.');
    }
};

window.stopGeneration = function() {
    if (window.app && window.app.chatManager) {
        window.app.chatManager.stopGeneration();
    }
};
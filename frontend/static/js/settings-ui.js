// app/static/js/settings-ui.js

// Global settings UI functions
window.toggleSettings = function() {
    const settingsPanel = document.getElementById('settingsPanel');
    if (settingsPanel) {
        settingsPanel.classList.toggle('show');
        console.log('⚙️ Settings toggled');
    }
};

window.closeSettings = function() {
    const settingsPanel = document.getElementById('settingsPanel');
    if (settingsPanel) {
        settingsPanel.classList.remove('show');
        console.log('✓ Settings closed');
    }
};

window.saveSettings = function() {
    if (window.app && window.app.settingsManager) {
        window.app.settingsManager.applySettings();
        window.closeSettings();
        console.log('✅ Settings saved');
    } else {
        console.error('❌ SettingsManager not found');
    }
};

console.log('✓ Settings UI functions loaded');

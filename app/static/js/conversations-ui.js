// Global functions for conversations UI

window.toggleSidebar = function() {
    document.body.classList.toggle('sidebar-collapsed');

    const toggle = document.getElementById('sidebarToggle');
    if (toggle) {
        toggle.style.display = document.body.classList.contains('sidebar-collapsed') ? 'block' : 'none';
    }
};

window.startNewConversation = function() {
    if (window.app && window.app.conversationsManager) {
        window.app.conversationsManager.createNewConversation();
    }
};

window.loadConversations = function() {
    if (window.app && window.app.conversationsManager) {
        window.app.conversationsManager.loadConversations();
    }
};

window.renameConversation = function(conversationId) {
    if (window.app && window.app.conversationsManager) {
        window.app.conversationsManager.renameConversation(conversationId);
    }
};

window.deleteConversation = function(conversationId) {
    if (window.app && window.app.conversationsManager) {
        window.app.conversationsManager.deleteConversation(conversationId);
    }
};
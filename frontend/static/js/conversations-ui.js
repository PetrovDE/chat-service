// app/static/js/conversations-ui.js

window.toggleSidebar = function() {
    document.body.classList.toggle('sidebar-collapsed');
};

window.startNewConversation = function() {
    if (window.app && window.app.conversationsManager) {
        window.app.conversationsManager.createNewConversation();
    }
};

window.loadConversation = function(conversationId) {
    console.log('📂 Loading conversation:', conversationId);
    if (window.app && window.app.chatManager) {
        window.app.chatManager.setCurrentConversation(conversationId);
    }
};

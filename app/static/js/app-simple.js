// app/static/js/app-simple.js
// Simplified standalone version with streaming support

console.log('🦙 Loading Llama Chat...');

// Global state
const AppState = {
    token: localStorage.getItem('token'),
    currentUser: null,
    currentConversation: null,
    isGenerating: false
};

// API calls
async function apiCall(endpoint, options = {}) {
    const url = window.location.origin + endpoint;
    const headers = {
        'Content-Type': 'application/json',
        ...(AppState.token && { 'Authorization': `Bearer ${AppState.token}` })
    };

    try {
        const response = await fetch(url, { ...options, headers });
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.detail || data.error || 'Request failed');
        }

        return data;
    } catch (error) {
        console.error('API Error:', error);
        throw error;
    }
}

// UI Functions
function addMessage(role, content, isStreaming = false) {
    const container = document.getElementById('chatMessages');
    const msgId = 'msg-' + Date.now();
    const msgDiv = document.createElement('div');
    msgDiv.id = msgId;
    msgDiv.className = `message ${role}`;
    msgDiv.innerHTML = `
        <div class="message-bubble">${content}${isStreaming ? '▋' : ''}</div>
        <div class="message-time">${new Date().toLocaleTimeString('ru-RU', {hour: '2-digit', minute: '2-digit'})}</div>
    `;
    container.appendChild(msgDiv);
    container.scrollTop = container.scrollHeight;
    return msgId;
}

function updateMessage(msgId, content, isStreaming = true) {
    const msgDiv = document.getElementById(msgId);
    if (msgDiv) {
        const bubble = msgDiv.querySelector('.message-bubble');
        if (bubble) {
            bubble.textContent = content + (isStreaming ? '▋' : '');
        }
        const container = document.getElementById('chatMessages');
        container.scrollTop = container.scrollHeight;
    }
}

function clearMessages() {
    document.getElementById('chatMessages').innerHTML = `
        <div class="message assistant">
            <div class="message-bubble">Привет! Чем могу помочь?</div>
            <div class="message-time">Сейчас</div>
        </div>
    `;
}

// Send message with streaming
async function sendMessage() {
    const input = document.getElementById('messageInput');
    const button = document.getElementById('sendMessage');
    const message = input.value.trim();

    if (!message || AppState.isGenerating) return;

    AppState.isGenerating = true;
    button.disabled = true;
    button.textContent = 'Генерация...';

    // Add user message
    addMessage('user', message);
    input.value = '';
    input.style.height = 'auto';

    // Create assistant message placeholder
    const assistantMsgId = addMessage('assistant', '', true);
    let fullResponse = '';

    try {
        // Use streaming endpoint
        const response = await fetch('/chat/stream', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                ...(AppState.token && { 'Authorization': `Bearer ${AppState.token}` })
            },
            body: JSON.stringify({
                message,
                conversation_id: AppState.currentConversation
            })
        });

        if (!response.ok) {
            throw new Error('Streaming request failed');
        }

        // Read stream
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
            const { done, value } = await reader.read();

            if (done) break;

            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split('\n\n');
            buffer = lines.pop() || '';

            for (const line of lines) {
                if (line.startsWith('data: ')) {
                    try {
                        const data = JSON.parse(line.substring(6));

                        if (data.type === 'start') {
                            // Save conversation ID
                            if (data.conversation_id) {
                                AppState.currentConversation = data.conversation_id;
                            }
                        } else if (data.type === 'chunk') {
                            // Add chunk to response
                            fullResponse += data.content;
                            updateMessage(assistantMsgId, fullResponse, true);
                        } else if (data.type === 'done') {
                            // Finalize message
                            updateMessage(assistantMsgId, fullResponse, false);
                            console.log(`✅ Generation completed in ${data.generation_time}s`);

                            // Reload conversations
                            loadConversations();
                        } else if (data.type === 'error') {
                            throw new Error(data.message);
                        }
                    } catch (parseError) {
                        console.error('Parse error:', parseError);
                    }
                }
            }
        }

    } catch (error) {
        console.error('Streaming error:', error);
        updateMessage(assistantMsgId, '❌ Ошибка: ' + error.message, false);
    } finally {
        AppState.isGenerating = false;
        button.disabled = false;
        button.textContent = 'Отправить';
    }
}

// Load conversations
async function loadConversations() {
    const list = document.getElementById('conversationsList');

    try {
        const conversations = await apiCall('/conversations');

        if (conversations.length === 0) {
            list.innerHTML = '<div class="empty-conversations">Нет бесед</div>';
            return;
        }

        list.innerHTML = conversations.map(conv => `
            <div class="conversation-item ${conv.id === AppState.currentConversation ? 'active' : ''}" 
                 onclick="loadConversation('${conv.id}')">
                <div class="conversation-title">
                    <span class="conv-title-text">${conv.title}</span>
                    <div class="conversation-actions">
                        <button class="icon-btn" onclick="event.stopPropagation(); deleteConversation('${conv.id}')">🗑️</button>
                    </div>
                </div>
                <div class="conversation-meta">
                    <span>${conv.message_count} сообщений</span>
                </div>
            </div>
        `).join('');

    } catch (error) {
        console.error('Load conversations error:', error);
        list.innerHTML = '<div class="conversations-loading">Ошибка загрузки</div>';
    }
}

// Load single conversation
async function loadConversation(id) {
    try {
        const data = await apiCall(`/conversations/${id}`);
        AppState.currentConversation = id;

        clearMessages();
        data.messages.forEach(msg => {
            addMessage(msg.role, msg.content);
        });

        loadConversations();
    } catch (error) {
        console.error('Load conversation error:', error);
    }
}

// Delete conversation
async function deleteConversation(id) {
    if (!confirm('Удалить эту беседу?')) return;

    try {
        await apiCall(`/conversations/${id}`, { method: 'DELETE' });

        if (AppState.currentConversation === id) {
            AppState.currentConversation = null;
            clearMessages();
        }

        loadConversations();
    } catch (error) {
        console.error('Delete error:', error);
        alert('Ошибка удаления');
    }
}

// Start new conversation
function startNewConversation() {
    AppState.currentConversation = null;
    clearMessages();
}

// Auth functions
async function handleLogin(event) {
    event.preventDefault();
    const username = document.getElementById('loginUsername').value;
    const password = document.getElementById('loginPassword').value;
    const errorDiv = document.getElementById('loginError');

    try {
        const data = await apiCall('/auth/login', {
            method: 'POST',
            body: JSON.stringify({ username, password })
        });

        AppState.token = data.access_token;
        localStorage.setItem('token', data.access_token);
        AppState.currentUser = data.user;

        closeAuthModals();
        updateAuthUI();
        loadConversations();

    } catch (error) {
        errorDiv.textContent = error.message;
        errorDiv.style.display = 'block';
    }
}

async function handleRegister(event) {
    event.preventDefault();
    const username = document.getElementById('registerUsername').value;
    const email = document.getElementById('registerEmail').value;
    const password = document.getElementById('registerPassword').value;
    const fullName = document.getElementById('registerFullName').value;
    const errorDiv = document.getElementById('registerError');

    try {
        const data = await apiCall('/auth/register', {
            method: 'POST',
            body: JSON.stringify({ username, email, password, full_name: fullName })
        });

        AppState.token = data.access_token;
        localStorage.setItem('token', data.access_token);
        AppState.currentUser = data.user;

        closeAuthModals();
        updateAuthUI();
        loadConversations();

    } catch (error) {
        errorDiv.textContent = error.message;
        errorDiv.style.display = 'block';
    }
}

function showLogin() {
    document.getElementById('authOverlay').classList.add('show');
    document.getElementById('loginModal').classList.add('show');
    document.getElementById('registerModal').classList.remove('show');
}

function switchToRegister() {
    document.getElementById('loginModal').classList.remove('show');
    document.getElementById('registerModal').classList.add('show');
}

function switchToLogin() {
    document.getElementById('registerModal').classList.remove('show');
    document.getElementById('loginModal').classList.add('show');
}

function closeAuthModals() {
    document.getElementById('authOverlay').classList.remove('show');
    document.getElementById('loginModal').classList.remove('show');
    document.getElementById('registerModal').classList.remove('show');
}

function updateAuthUI() {
    const authSection = document.getElementById('authSection');
    if (AppState.token && AppState.currentUser) {
        authSection.innerHTML = `
            <span style="margin-right: 1rem;">👋 ${AppState.currentUser.username}</span>
            <button class="login-btn" onclick="logout()">Выйти</button>
        `;
    } else {
        authSection.innerHTML = `
            <button class="login-btn" onclick="showLogin()">Войти</button>
        `;
    }
}

function logout() {
    AppState.token = null;
    AppState.currentUser = null;
    AppState.currentConversation = null;
    localStorage.removeItem('token');
    updateAuthUI();
    clearMessages();
    document.getElementById('conversationsList').innerHTML = '<div class="conversations-loading">Войдите для просмотра</div>';
}

// Sidebar toggle
function toggleSidebar() {
    document.body.classList.toggle('sidebar-collapsed');
    const toggle = document.getElementById('sidebarToggle');
    toggle.style.display = document.body.classList.contains('sidebar-collapsed') ? 'block' : 'none';
}

// Health check
async function checkHealth() {
    try {
        const health = await apiCall('/health');
        const indicator = document.getElementById('healthIndicator');
        const status = document.getElementById('healthStatus');

        if (health.status === 'healthy') {
            indicator.style.background = '#10b981';
            status.textContent = 'Работает';
        } else if (health.status === 'degraded') {
            indicator.style.background = '#f59e0b';
            status.textContent = 'Ограничено';
        } else {
            indicator.style.background = '#ef4444';
            status.textContent = 'Недоступно';
        }
    } catch (error) {
        console.error('Health check failed:', error);
    }
}

// Initialize
document.addEventListener('DOMContentLoaded', async () => {
    console.log('✅ DOM loaded');

    // Setup send button
    const sendButton = document.getElementById('sendMessage');
    const messageInput = document.getElementById('messageInput');

    if (sendButton) {
        sendButton.addEventListener('click', sendMessage);
    }

    if (messageInput) {
        messageInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                sendMessage();
            }
        });

        messageInput.addEventListener('input', (e) => {
            e.target.style.height = 'auto';
            e.target.style.height = e.target.scrollHeight + 'px';
        });
    }

    // Check auth
    if (AppState.token) {
        try {
            const user = await apiCall('/auth/me');
            AppState.currentUser = user;
            updateAuthUI();
            loadConversations();
        } catch (error) {
            console.error('Auth check failed:', error);
            logout();
        }
    } else {
        updateAuthUI();
    }

    // Health check
    checkHealth();
    setInterval(checkHealth, 30000);

    console.log('✅ Application initialized with streaming support!');
});
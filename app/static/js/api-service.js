// app/static/js/api-service.js
export class ApiService {
    constructor() {
        this.baseURL = window.location.origin;
    }

    async request(endpoint, options = {}) {
        const url = `${this.baseURL}${endpoint}`;

        const defaultOptions = {
            headers: {
                'Content-Type': 'application/json',
            },
        };

        // Add auth token if exists
        const token = localStorage.getItem('token');
        if (token) {
            defaultOptions.headers['Authorization'] = `Bearer ${token}`;
        }

        const config = { ...defaultOptions, ...options };

        try {
            const response = await fetch(url, config);

            // Handle non-JSON responses
            const contentType = response.headers.get('content-type');
            if (!contentType || !contentType.includes('application/json')) {
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                return await response.text();
            }

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.detail || data.error || `HTTP error! status: ${response.status}`);
            }

            return data;
        } catch (error) {
            console.error('API request failed:', error);
            throw error;
        }
    }

    // Auth endpoints
    async register(username, email, password, fullName) {
        return await this.request('/auth/register', {
            method: 'POST',
            body: JSON.stringify({
                username,
                email,
                password,
                full_name: fullName
            })
        });
    }

    async login(username, password) {
        return await this.request('/auth/login', {
            method: 'POST',
            body: JSON.stringify({ username, password })
        });
    }

    async getCurrentUser() {
        return await this.request('/auth/me');
    }

    async logout() {
        return await this.request('/auth/logout', { method: 'POST' });
    }

    // Chat endpoints
    async sendMessage(message, conversationId = null) {
        return await this.request('/chat', {
            method: 'POST',
            body: JSON.stringify({
                message,
                conversation_id: conversationId
            })
        });
    }

    // Conversations endpoints
    async getConversations() {
        return await this.request('/conversations');
    }

    async getConversation(id) {
        return await this.request(`/conversations/${id}`);
    }

    async deleteConversation(id) {
        return await this.request(`/conversations/${id}`, { method: 'DELETE' });
    }

    async updateConversation(id, data) {
        return await this.request(`/conversations/${id}`, {
            method: 'PATCH',
            body: JSON.stringify(data)
        });
    }

    // System endpoints
    async checkHealth() {
        return await this.request('/health');
    }

    async getAppInfo() {
        return await this.request('/info');
    }
}
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
        const token = localStorage.getItem('auth_token');
        if (token) {
            try {
                const tokenData = JSON.parse(token);
                defaultOptions.headers['Authorization'] = `Bearer ${tokenData.token}`;
            } catch (e) {
                console.error('Error parsing token:', e);
            }
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

    // HTTP Methods shortcuts
    async get(endpoint) {
        return await this.request(endpoint, { method: 'GET' });
    }

    async post(endpoint, body) {
        return await this.request(endpoint, {
            method: 'POST',
            body: JSON.stringify(body)
        });
    }

    async patch(endpoint, body) {
        return await this.request(endpoint, {
            method: 'PATCH',
            body: JSON.stringify(body)
        });
    }

    async delete(endpoint) {
        return await this.request(endpoint, { method: 'DELETE' });
    }

    // Auth endpoints
    async register(username, email, password, fullName) {
        return await this.post('/auth/register', {
            username,
            email,
            password,
            full_name: fullName
        });
    }

    async login(username, password) {
        return await this.post('/auth/login', {
            username,
            password
        });
    }

    async getCurrentUser() {
        return await this.get('/auth/me');
    }

    async logout() {
        return await this.post('/auth/logout', {});
    }

    // Chat endpoints
    async sendMessage(message, conversationId = null) {
        return await this.post('/chat', {
            message,
            conversation_id: conversationId
        });
    }

    // Conversations endpoints
    async getConversations() {
        return await this.get('/conversations');
    }

    async getConversation(id) {
        return await this.get(`/conversations/${id}`);
    }

    async deleteConversation(id) {
        return await this.delete(`/conversations/${id}`);
    }

    async updateConversation(id, data) {
        return await this.patch(`/conversations/${id}`, data);
    }

    // Models endpoints
    async getModels() {
        return await this.get('/models');
    }

    async switchModel(modelSource, modelName) {
        return await this.post('/models/switch', {
            model_source: modelSource,
            model_name: modelName
        });
    }

    async getCurrentModel() {
        return await this.get('/models/current');
    }

    // Files endpoints
    async uploadFile(file) {
        const formData = new FormData();
        formData.append('file', file);

        // Get token
        const token = localStorage.getItem('auth_token');
        let authHeader = {};
        if (token) {
            try {
                const tokenData = JSON.parse(token);
                authHeader = { 'Authorization': `Bearer ${tokenData.token}` };
            } catch (e) {
                console.error('Error parsing token:', e);
            }
        }

        const response = await fetch(`${this.baseURL}/files/upload`, {
            method: 'POST',
            headers: authHeader,
            body: formData
        });

        if (!response.ok) {
            const data = await response.json();
            throw new Error(data.detail || data.error || 'File upload failed');
        }

        return await response.json();
    }

    async getFileInfo(fileId) {
        return await this.get(`/files/${fileId}`);
    }

    async deleteFile(fileId) {
        return await this.delete(`/files/${fileId}`);
    }

    // Stats endpoints
    async getUsageStats() {
        return await this.get('/stats/usage');
    }

    async getUserStats() {
        return await this.get('/stats/user');
    }

    // System endpoints
    async checkHealth() {
        return await this.get('/health');
    }

    async getAppInfo() {
        return await this.get('/info');
    }
}
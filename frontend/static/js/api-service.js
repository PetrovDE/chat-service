// frontend/static/js/api-service.js
class ApiService {
    constructor() {
        this.baseURL = '/api/v1';
    }

    async request(method, endpoint, data = null) {
        const url = `${this.baseURL}${endpoint}`;
        console.log(`📡 ${method} ${url}`, data || '');
        try {
            const headers = { 'Content-Type': 'application/json' };
            const token = localStorage.getItem('auth_token');
            if (token) {
                headers['Authorization'] = `Bearer ${token}`;
            }

            const options = {
                method,
                headers: headers
            };

            if (data) options.body = JSON.stringify(data);

            const response = await fetch(url, options);
            console.log(`✓ ${method} ${url} → ${response.status}`);

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            const result = await response.json();
            return result;
        } catch (error) {
            console.error(`❌ ${method} ${url} → ERROR:`, error);
            throw error;
        }
    }

    async get(endpoint) { return this.request('GET', endpoint); }
    async post(endpoint, data) { return this.request('POST', endpoint, data); }
    async put(endpoint, data) { return this.request('PUT', endpoint, data); }
    async delete(endpoint) { return this.request('DELETE', endpoint); }

    // Health check - СПЕЦИАЛЬНЫЙ метод БЕЗ baseURL
    async checkHealth() {
        console.log('📡 GET /health');
        try {
            const response = await fetch('/health');
            console.log(`✓ GET /health → ${response.status}`);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }
            return await response.json();
        } catch (error) {
            console.error('❌ GET /health → ERROR:', error);
            throw error;
        }
    }

    // Files API - ОБНОВЛЕНО: добавлена поддержка conversation_id параметра
    async getProcessedFiles(conversationId = null) {
        let endpoint = '/files/processed';
        if (conversationId) {
            endpoint += `?conversation_id=${conversationId}`;
        }
        return this.get(endpoint);
    }

    async deleteFile(fileId) {
        return this.delete(`/files/${fileId}`);
    }
}

export { ApiService };

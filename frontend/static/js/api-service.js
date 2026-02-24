class HttpError extends Error {
    constructor(message, status, payload = null) {
        super(message);
        this.name = 'HttpError';
        this.status = status;
        this.payload = payload;
    }
}

class ApiService {
    constructor() {
        this.baseURL = '/api/v1';
        this.defaultTimeoutMs = 30000;
    }

    getAuthHeaders(includeJson = true) {
        const headers = {};
        if (includeJson) headers['Content-Type'] = 'application/json';

        const token = localStorage.getItem('auth_token');
        if (token) headers['Authorization'] = `Bearer ${token}`;

        return headers;
    }

    async request(method, endpoint, data = null, options = {}) {
        const url = `${this.baseURL}${endpoint}`;
        const timeoutMs = options.timeoutMs || this.defaultTimeoutMs;
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeoutMs);

        try {
            const response = await fetch(url, {
                method,
                headers: {
                    ...this.getAuthHeaders(true),
                    ...(options.headers || {}),
                },
                body: data ? JSON.stringify(data) : undefined,
                signal: options.signal || controller.signal,
            });

            const contentType = response.headers.get('content-type') || '';
            const responseBody = contentType.includes('application/json')
                ? await response.json()
                : await response.text();

            if (!response.ok) {
                const detail = responseBody?.detail || response.statusText || `HTTP ${response.status}`;
                throw new HttpError(detail, response.status, responseBody);
            }

            return responseBody;
        } catch (error) {
            if (error.name === 'AbortError') {
                throw new Error('Request timeout');
            }
            throw error;
        } finally {
            clearTimeout(timeoutId);
        }
    }

    async get(endpoint, options = {}) {
        return this.request('GET', endpoint, null, options);
    }

    async post(endpoint, data, options = {}) {
        return this.request('POST', endpoint, data, options);
    }

    async put(endpoint, data, options = {}) {
        return this.request('PUT', endpoint, data, options);
    }

    async patch(endpoint, data, options = {}) {
        return this.request('PATCH', endpoint, data, options);
    }

    async delete(endpoint, options = {}) {
        return this.request('DELETE', endpoint, null, options);
    }

    async checkHealth() {
        const response = await fetch('/health');
        if (!response.ok) {
            throw new Error(`Health check failed: ${response.status}`);
        }
        return response.json();
    }

    async streamChat(payload, signal) {
        const response = await fetch(`${this.baseURL}/chat/stream`, {
            method: 'POST',
            headers: this.getAuthHeaders(true),
            body: JSON.stringify(payload),
            signal,
        });

        if (!response.ok) {
            let message = `HTTP ${response.status}`;
            try {
                const errorPayload = await response.json();
                message = errorPayload.detail || message;
            } catch (_) {
                // no-op
            }
            throw new Error(message);
        }

        return response;
    }

    async getConversations() {
        return this.get('/conversations/');
    }

    async getConversationMessages(conversationId) {
        return this.get(`/conversations/${conversationId}/messages`);
    }

    async getProcessedFiles(conversationId = null) {
        const params = conversationId ? `?conversation_id=${conversationId}` : '';
        return this.get(`/files/processed${params}`);
    }

    async deleteFile(fileId) {
        return this.delete(`/files/${fileId}`);
    }
}

export { ApiService, HttpError };

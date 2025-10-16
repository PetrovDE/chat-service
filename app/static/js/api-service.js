// API Service - handles all API calls
export class ApiService {
    constructor() {
        this.baseURL = '';
        this.uiController = null;
    }

    setUIController(uiController) {
        this.uiController = uiController;
    }

    async request(url, options = {}) {
        try {
            const defaultOptions = {
                headers: {
                    'Content-Type': 'application/json',
                },
            };

            // Add authentication token if available
            if (window.app && window.app.authManager && window.app.authManager.getToken()) {
                defaultOptions.headers['Authorization'] = `Bearer ${window.app.authManager.getToken()}`;
            }

            const response = await fetch(url, {
                ...defaultOptions,
                ...options,
                headers: {
                    ...defaultOptions.headers,
                    ...options.headers,
                }
            });

            if (!response.ok) {
                let errorMessage = `HTTP Error: ${response.status}`;
                try {
                    const errorData = await response.json();
                    errorMessage = errorData.detail || errorMessage;
                } catch {
                    errorMessage = response.statusText || errorMessage;
                }
                throw new Error(errorMessage);
            }

            const contentType = response.headers.get('content-type');
            if (contentType && contentType.includes('application/json')) {
                return await response.json();
            } else if (contentType && contentType.includes('text/')) {
                return await response.text();
            } else {
                return response;
            }
        } catch (error) {
            console.error(`API request failed for ${url}:`, error);
            throw error;
        }
    }

    async get(endpoint) {
        const url = `${this.baseURL}${endpoint}`;
        return await this.request(url, { method: 'GET' });
    }

    async post(endpoint, data) {
        const url = `${this.baseURL}${endpoint}`;
        return await this.request(url, {
            method: 'POST',
            body: JSON.stringify(data)
        });
    }

    async put(endpoint, data) {
        const url = `${this.baseURL}${endpoint}`;
        return await this.request(url, {
            method: 'PUT',
            body: JSON.stringify(data)
        });
    }

    async delete(endpoint) {
        const url = `${this.baseURL}${endpoint}`;
        const response = await this.request(url, { method: 'DELETE' });
        return response || { success: true };
    }

    async patch(endpoint, data) {
        const url = `${this.baseURL}${endpoint}`;
        return await this.request(url, {
            method: 'PATCH',
            body: JSON.stringify(data)
        });
    }

    async chat(message, conversationId = null) {
        const endpoint = conversationId ? '/chat/continue' : '/chat';
        const data = {
            message: message,
            temperature: 0.7,
            max_tokens: 2048
        };

        if (conversationId) {
            data.conversation_id = conversationId;
            data.include_history = true;
        }

        return await this.post(endpoint, data);
    }

    async checkHealth() {
        try {
            const health = await this.get('/health');

            if (this.uiController) {
                this.uiController.updateHealthStatus(health.status, health.model_info);
            }

            return health;
        } catch (error) {
            console.error('Health check failed:', error);

            if (this.uiController) {
                this.uiController.updateHealthStatus('error', null);
            }

            return { status: 'error' };
        }
    }
}
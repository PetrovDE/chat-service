// API communication service
export class ApiService {
    constructor() {
        this.baseURL = '';  // Using relative URLs
        this.uiController = null;
    }

    setUIController(uiController) {
        this.uiController = uiController;
    }

    // Health check
    async checkHealth() {
        try {
            const response = await fetch('/health');
            const health = await response.json();

            if (this.uiController) {
                this.uiController.updateHealthStatus(health);
            }

            return health;
        } catch (error) {
            console.error('Health check failed:', error);
            if (this.uiController) {
                this.uiController.updateHealthError();
            }
            throw error;
        }
    }

    // Chat endpoints
    async sendChat(requestData) {
        try {
            const response = await fetch('/chat', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(requestData)
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to get response');
            }

            return await response.json();
        } catch (error) {
            console.error('Chat request failed:', error);
            throw error;
        }
    }

    async streamChat(requestData) {
        try {
            const response = await fetch('/chat/stream', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(requestData)
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Streaming request failed');
            }

            return response;
        } catch (error) {
            console.error('Stream chat request failed:', error);
            throw error;
        }
    }

    // File endpoints
    async uploadFile(file) {
        try {
            const formData = new FormData();
            formData.append('file', file);

            const response = await fetch('/upload', {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Upload failed');
            }

            return await response.json();
        } catch (error) {
            console.error('File upload failed:', error);
            throw error;
        }
    }

    async analyzeFile(analysisData) {
        try {
            const response = await fetch('/analyze-file', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(analysisData)
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Analysis failed');
            }

            return await response.json();
        } catch (error) {
            console.error('File analysis failed:', error);
            throw error;
        }
    }

    // Model information
    async getModels() {
        try {
            const response = await fetch('/models');

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Failed to get models');
            }

            return await response.json();
        } catch (error) {
            console.error('Get models failed:', error);
            throw error;
        }
    }

    // Generic request wrapper with error handling
    async request(url, options = {}) {
        try {
            const defaultOptions = {
                headers: {
                    'Content-Type': 'application/json',
                },
            };

            // Add authentication token if available
            if (window.authManager && window.authManager.getToken()) {
                defaultOptions.headers['Authorization'] = `Bearer ${window.authManager.getToken()}`;
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
                    // If response is not JSON, use status text
                    errorMessage = response.statusText || errorMessage;
                }
                throw new Error(errorMessage);
            }

            // Handle different content types
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

    // Request interceptors for adding auth, logging, etc.
    addRequestInterceptor(interceptor) {
        // For future use - could modify requests before sending
        this.requestInterceptors = this.requestInterceptors || [];
        this.requestInterceptors.push(interceptor);
    }

    addResponseInterceptor(interceptor) {
        // For future use - could modify responses after receiving
        this.responseInterceptors = this.responseInterceptors || [];
        this.responseInterceptors.push(interceptor);
    }

    // Utility methods for common patterns
    async get(endpoint) {
        return this.request(endpoint, { method: 'GET' });
    }

    async post(endpoint, data) {
        return this.request(endpoint, {
            method: 'POST',
            body: JSON.stringify(data)
        });
    }

    async put(endpoint, data) {
        return this.request(endpoint, {
            method: 'PUT',
            body: JSON.stringify(data)
        });
    }

    async delete(endpoint) {
        return this.request(endpoint, { method: 'DELETE' });
    }

    // Connection testing
    async testConnection() {
        try {
            await this.checkHealth();
            return { status: 'connected', message: 'Connection successful' };
        } catch (error) {
            return { status: 'error', message: error.message };
        }
    }

    // Performance monitoring
    async measureRequestTime(requestFunction) {
        const startTime = performance.now();
        try {
            const result = await requestFunction();
            const endTime = performance.now();
            const duration = endTime - startTime;
            console.log(`Request completed in ${duration.toFixed(2)}ms`);
            return { result, duration };
        } catch (error) {
            const endTime = performance.now();
            const duration = endTime - startTime;
            console.log(`Request failed after ${duration.toFixed(2)}ms:`, error);
            throw error;
        }
    }
}
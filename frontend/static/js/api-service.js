class HttpError extends Error {
    constructor(message, status, payload = null) {
        super(message);
        this.name = 'HttpError';
        this.status = status;
        this.payload = payload;
    }
}

function extractErrorMessage(responseBody, fallback) {
    if (responseBody && typeof responseBody === 'object') {
        if (typeof responseBody.detail === 'string' && responseBody.detail.trim()) {
            return responseBody.detail.trim();
        }
        if (responseBody.error && typeof responseBody.error.message === 'string' && responseBody.error.message.trim()) {
            return responseBody.error.message.trim();
        }
        if (Array.isArray(responseBody.error?.details) && responseBody.error.details.length > 0) {
            return String(responseBody.error.details[0]?.msg || fallback);
        }
        if (Array.isArray(responseBody.details) && responseBody.details.length > 0) {
            return String(responseBody.details[0]?.msg || fallback);
        }
    }

    if (typeof responseBody === 'string' && responseBody.trim()) {
        return responseBody.trim();
    }

    return fallback;
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
        return this.requestRaw(method, endpoint, {
            body: data ? JSON.stringify(data) : undefined,
            includeJsonHeader: true,
            ...options,
        });
    }

    async requestRaw(method, endpoint, options = {}) {
        const url = `${this.baseURL}${endpoint}`;
        const timeoutMs = options.timeoutMs || this.defaultTimeoutMs;
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
        const includeJsonHeader = options.includeJsonHeader !== false;

        try {
            const response = await fetch(url, {
                method,
                headers: {
                    ...this.getAuthHeaders(includeJsonHeader),
                    ...(options.headers || {}),
                },
                body: options.body,
                signal: options.signal || controller.signal,
            });

            const contentType = response.headers.get('content-type') || '';
            const responseBody = response.status === 204
                ? null
                : contentType.includes('application/json')
                ? await response.json()
                : await response.text();

            if (!response.ok) {
                const detail = extractErrorMessage(responseBody, response.statusText || `HTTP ${response.status}`);
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

    async postForm(endpoint, formData, options = {}) {
        return this.requestRaw('POST', endpoint, {
            ...options,
            body: formData,
            includeJsonHeader: false,
        });
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

    async getConversationMessages(conversationId, { skip = 0, limit = 200 } = {}) {
        const params = `?skip=${encodeURIComponent(skip)}&limit=${encodeURIComponent(limit)}`;
        return this.get(`/conversations/${conversationId}/messages${params}`);
    }

    async deleteConversation(conversationId) {
        return this.delete(`/conversations/${conversationId}`);
    }

    async uploadFile({
        file,
        chatId = null,
        autoProcess = true,
        sourceKind = 'upload',
        embeddingProvider = 'local',
        embeddingModel = null,
        pipelineVersion = null,
        parserVersion = null,
        artifactVersion = null,
        chunkingStrategy = null,
        retrievalProfile = null,
    }) {
        const formData = new FormData();
        formData.append('file', file);
        formData.append('source_kind', sourceKind);
        formData.append('auto_process', String(Boolean(autoProcess)));
        formData.append('embedding_provider', embeddingProvider || 'local');

        if (chatId) formData.append('chat_id', String(chatId));
        if (embeddingModel) formData.append('embedding_model', embeddingModel);
        if (pipelineVersion) formData.append('pipeline_version', pipelineVersion);
        if (parserVersion) formData.append('parser_version', parserVersion);
        if (artifactVersion) formData.append('artifact_version', artifactVersion);
        if (chunkingStrategy) formData.append('chunking_strategy', chunkingStrategy);
        if (retrievalProfile) formData.append('retrieval_profile', retrievalProfile);

        return this.postForm('/files/upload', formData);
    }

    async getFiles({ skip = 0, limit = 200 } = {}) {
        const params = new URLSearchParams({
            skip: String(skip),
            limit: String(limit),
        });
        return this.get(`/files/?${params.toString()}`);
    }

    async getReadyFiles({ chatId = null } = {}) {
        const params = chatId ? `?chat_id=${encodeURIComponent(chatId)}` : '';
        return this.get(`/files/processed${params}`);
    }

    async getFile(fileId) {
        return this.get(`/files/${fileId}`);
    }

    async getFileStatus(fileId) {
        return this.get(`/files/${fileId}/status`);
    }

    async getFileQuota() {
        return this.get('/files/quota');
    }

    async attachFileToChat(fileId, chatId) {
        return this.post(`/files/${fileId}/attach`, { chat_id: chatId });
    }

    async detachFileFromChat(fileId, chatId) {
        return this.post(`/files/${fileId}/detach`, { chat_id: chatId });
    }

    async deleteFile(fileId) {
        return this.delete(`/files/${fileId}`);
    }

    async reprocessFile(fileId, request = {}) {
        const payload = {
            embedding_provider: request.embedding_provider || request.embeddingProvider || 'local',
            embedding_model: request.embedding_model || request.embeddingModel || null,
            pipeline_version: request.pipeline_version || request.pipelineVersion || 'pipeline-v1',
            parser_version: request.parser_version || request.parserVersion || 'parser-v1',
            artifact_version: request.artifact_version || request.artifactVersion || 'artifact-v1',
            chunking_strategy: request.chunking_strategy || request.chunkingStrategy || 'smart',
            retrieval_profile: request.retrieval_profile || request.retrievalProfile || 'default',
        };
        return this.post(`/files/${fileId}/reprocess`, payload);
    }

    async getFileDebugInfo(fileId) {
        return this.get(`/files/${fileId}/debug`);
    }

    async getFileProcessingVersions(fileId) {
        return this.get(`/files/${fileId}/processing`);
    }

    async getFileActiveProcessing(fileId) {
        return this.get(`/files/${fileId}/processing/active`);
    }
}

export { ApiService, HttpError };

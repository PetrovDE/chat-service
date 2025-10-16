// File manager - handles file uploads and processing

export class FileManager {
    constructor(apiService, uiController, chatManager) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.chatManager = chatManager;
    }

    async uploadFile(file) {
        try {
            this.uiController.showLoading('Uploading file...');

            const formData = new FormData();
            formData.append('file', file);

            const response = await fetch('/files/upload', {
                method: 'POST',
                body: formData,
                headers: {
                    'Authorization': `Bearer ${window.app.authManager.getToken()}`
                }
            });

            if (!response.ok) {
                throw new Error('File upload failed');
            }

            const result = await response.json();

            this.uiController.hideLoading();
            this.uiController.showSuccess(`File uploaded: ${result.filename}`);

            // Add file info to chat
            this.chatManager.addMessage('system', `File uploaded: ${result.filename}\n\nPreview:\n${result.preview}`);

            return result;

        } catch (error) {
            console.error('File upload error:', error);
            this.uiController.hideLoading();
            this.uiController.showError('Failed to upload file: ' + error.message);
            throw error;
        }
    }
}
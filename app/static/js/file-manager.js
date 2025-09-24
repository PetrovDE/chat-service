// File management functionality
export class FileManager {
    constructor(apiService, uiController, chatManager) {
        this.apiService = apiService;
        this.uiController = uiController;
        this.chatManager = chatManager;
        this.currentFile = null;

        this.setupFileUpload();
        this.setupAnalysisButtons();
    }

    setupFileUpload() {
        const uploadArea = document.getElementById('fileUploadArea');
        const fileModal = document.getElementById('fileModal');

        if (uploadArea) {
            // Drag and drop handlers
            uploadArea.addEventListener('dragover', (e) => {
                e.preventDefault();
                uploadArea.classList.add('dragover');
            });

            uploadArea.addEventListener('dragleave', (e) => {
                e.preventDefault();
                uploadArea.classList.remove('dragover');
            });

            uploadArea.addEventListener('drop', (e) => {
                e.preventDefault();
                uploadArea.classList.remove('dragover');

                const files = e.dataTransfer.files;
                if (files.length > 0) {
                    this.handleFile(files[0]);
                }
            });
        }

        // Prevent modal from closing when clicking inside
        if (fileModal) {
            fileModal.addEventListener('click', (e) => {
                e.stopPropagation();
            });
        }
    }

    setupAnalysisButtons() {
        // Analysis type buttons
        document.querySelectorAll('.analysis-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.analysis-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');

                const customArea = document.getElementById('customPromptArea');
                if (customArea) {
                    if (btn.dataset.type === 'custom') {
                        customArea.style.display = 'block';
                    } else {
                        customArea.style.display = 'none';
                    }
                }
            });
        });
    }

    async handleFileUpload(event) {
        const file = event.target.files[0];
        if (file) {
            await this.handleFile(file);
        }
    }

    async handleFile(file) {
        try {
            this.uiController.showLoading('Uploading file...');

            const data = await this.apiService.uploadFile(file);
            this.currentFile = data;

            // Update file info display
            this.updateFileInfo(data);
            this.showFileAnalysisOptions();

            // Update attach button state
            this.updateAttachButtonState(data.filename);

            this.uiController.hideLoading();
            this.uiController.showSuccess(`File "${data.filename}" uploaded successfully!`);

        } catch (error) {
            this.uiController.hideLoading();
            this.uiController.showError(`Upload error: ${error.message}`);
        }
    }

    updateFileInfo(data) {
        const fileName = document.getElementById('fileName');
        const fileDetails = document.getElementById('fileDetails');
        const filePreview = document.getElementById('filePreview');
        const fileInfo = document.getElementById('fileInfo');

        if (fileName) fileName.textContent = data.filename;

        if (fileDetails) {
            const fileType = data.file_type.toUpperCase().replace('.', '');
            const fileSize = (data.size / 1024).toFixed(1);
            fileDetails.textContent = `Type: ${fileType}, Size: ${fileSize} KB`;
        }

        if (filePreview) filePreview.textContent = data.content_preview;

        if (fileInfo) fileInfo.style.display = 'block';
    }

    showFileAnalysisOptions() {
        const analysisSection = document.getElementById('analysisSection');
        if (analysisSection) {
            analysisSection.style.display = 'block';
        }
    }

    updateAttachButtonState(filename) {
        const attachButton = document.getElementById('attachButton');
        if (attachButton) {
            attachButton.classList.add('has-file');
            attachButton.title = `File attached: ${filename}`;
        }
    }

    async analyzeFile() {
        if (!this.currentFile) {
            this.uiController.showError('No file selected');
            return;
        }

        const activeBtn = document.querySelector('.analysis-btn.active');
        if (!activeBtn) {
            this.uiController.showError('Please select an analysis type');
            return;
        }

        const analysisType = activeBtn.dataset.type;
        const customPrompt = document.getElementById('customPrompt')?.value;

        if (analysisType === 'custom' && !customPrompt?.trim()) {
            this.uiController.showError('Please enter a custom prompt');
            return;
        }

        const analyzeButton = document.getElementById('analyzeButton');
        const originalText = analyzeButton ? analyzeButton.textContent : '';

        try {
            // Update button state
            if (analyzeButton) {
                analyzeButton.textContent = '🔍 Analyzing...';
                analyzeButton.disabled = true;
            }

            const analysisData = {
                content: this.currentFile.full_content,
                analysis_type: analysisType,
                custom_prompt: customPrompt || null,
                filename: this.currentFile.filename  // Include filename
            };

            const result = await this.apiService.analyzeFile(analysisData);

            // Add analysis result to chat
            const analysisMessage = this.formatAnalysisResult(result, this.currentFile.filename);
            this.chatManager.addMessage('assistant', analysisMessage);

            this.uiController.showSuccess('File analysis completed!');

            // Close the file modal after successful analysis
            this.closeModal();

        } catch (error) {
            this.uiController.showError(`Analysis error: ${error.message}`);
        } finally {
            if (analyzeButton) {
                analyzeButton.textContent = originalText;
                analyzeButton.disabled = false;
            }
        }
    }

    formatAnalysisResult(result, filename) {
        return `📄 **File Analysis Results for "${filename}"**\n\n**Analysis Type:** ${result.analysis_type}\n\n**Result:**\n${result.result}`;
    }

    toggleModal() {
        const fileModal = document.getElementById('fileModal');

        if (fileModal) {
            if (fileModal.classList.contains('show')) {
                this.closeModal();
            } else {
                this.openModal();
            }
        }
    }

    openModal() {
        const fileModal = document.getElementById('fileModal');
        const attachButton = document.getElementById('attachButton');

        if (fileModal) {
            fileModal.classList.add('show');
        }

        if (this.currentFile && attachButton) {
            attachButton.classList.add('has-file');
        }
    }

    closeModal() {
        const fileModal = document.getElementById('fileModal');
        if (fileModal) {
            fileModal.classList.remove('show');
        }
    }

    resetFileState() {
        this.currentFile = null;

        const fileInfo = document.getElementById('fileInfo');
        const analysisSection = document.getElementById('analysisSection');
        const fileModal = document.getElementById('fileModal');
        const attachButton = document.getElementById('attachButton');
        const fileInput = document.getElementById('fileInput');
        const customPromptArea = document.getElementById('customPromptArea');
        const customPrompt = document.getElementById('customPrompt');

        if (fileInfo) fileInfo.style.display = 'none';
        if (analysisSection) analysisSection.style.display = 'none';
        if (fileModal) fileModal.classList.remove('show');

        if (attachButton) {
            attachButton.classList.remove('has-file');
            attachButton.title = 'Attach file';
        }

        // Reset file input
        if (fileInput) fileInput.value = '';

        // Reset analysis options
        document.querySelectorAll('.analysis-btn').forEach(btn => {
            btn.classList.remove('active');
        });

        const summaryBtn = document.querySelector('.analysis-btn[data-type="summary"]');
        if (summaryBtn) summaryBtn.classList.add('active');

        if (customPromptArea) customPromptArea.style.display = 'none';
        if (customPrompt) customPrompt.value = '';
    }

    getCurrentFile() {
        return this.currentFile;
    }

    hasFile() {
        return this.currentFile !== null;
    }
}
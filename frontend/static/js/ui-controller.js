class UIController {
    constructor() {
        this.chatHistory = document.getElementById('chatMessages');
        this.toastTimeout = null;
    }

    showError(message) {
        this.showToast(message, 'error');
    }

    showSuccess(message) {
        this.showToast(message, 'success');
    }

    showToast(message, type = 'success') {
        let toast = document.getElementById('toastNotification');
        if (!toast) {
            toast = document.createElement('div');
            toast.id = 'toastNotification';
            toast.className = 'toast-notification';
            toast.setAttribute('role', 'status');
            toast.setAttribute('aria-live', 'polite');
            document.body.appendChild(toast);
        }

        toast.className = `toast-notification ${type}`;
        toast.innerHTML = `
            <div class="toast-content">
                <span class="toast-icon">${type === 'success' ? 'OK' : 'ERR'}</span>
                <span class="toast-message">${message}</span>
            </div>
        `;
        toast.classList.add('show');

        if (this.toastTimeout) clearTimeout(this.toastTimeout);
        this.toastTimeout = setTimeout(() => {
            toast.classList.remove('show');
        }, 3200);
    }

    showLoading(text = 'Loading...') {
        const overlay = document.getElementById('loadingOverlay');
        if (!overlay) return;

        const loadingText = overlay.querySelector('p');
        if (loadingText) loadingText.textContent = text;
        overlay.style.display = 'flex';
    }

    hideLoading() {
        const overlay = document.getElementById('loadingOverlay');
        if (!overlay) return;
        overlay.style.display = 'none';
    }
}

export { UIController };

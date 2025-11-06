// app/static/js/utils.js

export function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

export function formatDate(dateString) {
    const date = new Date(dateString);
    return date.toLocaleString('ru-RU');
}

export function showToast(message, type = 'info') {
    console.log(`🔔 Toast [${type}]:`, message);

    const toast = document.createElement('div');
    toast.className = `toast-notification ${type} show`;
    toast.innerHTML = `
        <div class="toast-content">
            <span class="toast-icon">${type === 'error' ? '❌' : '✅'}</span>
            <span class="toast-message">${message}</span>
        </div>
    `;

    document.body.appendChild(toast);

    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
    }, 3000);
}

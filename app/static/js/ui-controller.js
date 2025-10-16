// UI Controller - handles UI updates and notifications

export class UIController {
    constructor() {
        this.loadingOverlay = null;
        this.notificationContainer = null;
        this.initNotificationContainer();
    }

    initNotificationContainer() {
        // Create notification container if it doesn't exist
        if (!document.getElementById('notificationContainer')) {
            const container = document.createElement('div');
            container.id = 'notificationContainer';
            container.style.cssText = `
                position: fixed;
                top: 20px;
                right: 20px;
                z-index: 10000;
                max-width: 400px;
            `;
            document.body.appendChild(container);
        }
        this.notificationContainer = document.getElementById('notificationContainer');
    }

    showLoading(message = 'Loading...') {
        const loadingDiv = document.getElementById('loadingOverlay');
        const loadingText = document.getElementById('loadingText');

        if (loadingDiv) {
            loadingDiv.style.display = 'flex';
            if (loadingText) {
                loadingText.textContent = message;
            }
        }
    }

    hideLoading() {
        const loadingDiv = document.getElementById('loadingOverlay');
        if (loadingDiv) {
            loadingDiv.style.display = 'none';
        }
    }

    showNotification(message, type = 'info', duration = 3000) {
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.style.cssText = `
            padding: 1rem 1.5rem;
            margin-bottom: 10px;
            border-radius: 8px;
            background: white;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
            display: flex;
            align-items: center;
            gap: 0.75rem;
            animation: slideIn 0.3s ease-out;
            border-left: 4px solid ${this.getTypeColor(type)};
        `;

        const icon = this.getTypeIcon(type);
        notification.innerHTML = `
            <span style="font-size: 1.5rem;">${icon}</span>
            <span style="flex: 1; color: #1f2937;">${message}</span>
            <button onclick="this.parentElement.remove()" style="
                background: none;
                border: none;
                font-size: 1.5rem;
                color: #8e8e93;
                cursor: pointer;
                padding: 0;
                line-height: 1;
            ">×</button>
        `;

        this.notificationContainer.appendChild(notification);

        // Auto remove
        setTimeout(() => {
            notification.style.animation = 'slideOut 0.3s ease-out';
            setTimeout(() => notification.remove(), 300);
        }, duration);
    }

    getTypeColor(type) {
        const colors = {
            success: '#10b981',
            error: '#ef4444',
            warning: '#f59e0b',
            info: '#3b82f6'
        };
        return colors[type] || colors.info;
    }

    getTypeIcon(type) {
        const icons = {
            success: '✓',
            error: '✕',
            warning: '⚠',
            info: 'ℹ'
        };
        return icons[type] || icons.info;
    }

    showSuccess(message) {
        this.showNotification(message, 'success');
    }

    showError(message) {
        this.showNotification(message, 'error', 5000);
    }

    showWarning(message) {
        this.showNotification(message, 'warning');
    }

    showInfo(message) {
        this.showNotification(message, 'info');
    }

    updateHealthStatus(status, modelInfo) {
        const indicator = document.getElementById('healthIndicator');
        const statusText = document.getElementById('healthStatus');

        if (!indicator || !statusText) return;

        const statusMap = {
            healthy: { color: '#10b981', text: 'Connected' },
            warning: { color: '#f59e0b', text: 'Warning' },
            error: { color: '#ef4444', text: 'Error' },
            unknown: { color: '#8e8e93', text: 'Unknown' }
        };

        const statusConfig = statusMap[status] || statusMap.unknown;

        indicator.style.backgroundColor = statusConfig.color;

        if (modelInfo) {
            statusText.textContent = `${statusConfig.text} - ${modelInfo.model_name}`;
        } else {
            statusText.textContent = statusConfig.text;
        }
    }

    async initializeModelSelector() {
        // Model selector initialization would go here
        // For now, just a placeholder
        console.log('Model selector initialized');
    }
}

// Add animation styles
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            transform: translateX(400px);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
    
    @keyframes slideOut {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(400px);
            opacity: 0;
        }
    }
`;
document.head.appendChild(style);
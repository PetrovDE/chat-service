const NAIVE_ISO_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?$/;

export function parseAppTimestamp(value) {
    if (!value && value !== 0) return null;

    if (value instanceof Date) {
        const copied = new Date(value.getTime());
        return Number.isNaN(copied.getTime()) ? null : copied;
    }

    if (typeof value === 'number') {
        const fromMs = new Date(value);
        return Number.isNaN(fromMs.getTime()) ? null : fromMs;
    }

    let raw = String(value).trim();
    if (!raw) return null;

    // Backend stores UTC instants; naive ISO should be interpreted as UTC.
    if (NAIVE_ISO_RE.test(raw)) {
        raw = `${raw}Z`;
    }

    const parsed = new Date(raw);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export function nowTimestampISO() {
    return new Date().toISOString();
}

export function toLocalDateKey(value) {
    const date = parseAppTimestamp(value);
    if (!date) return '';
    const y = date.getFullYear();
    const m = String(date.getMonth() + 1).padStart(2, '0');
    const d = String(date.getDate()).padStart(2, '0');
    return `${y}-${m}-${d}`;
}

export function formatMessageTimeLabel(value) {
    const date = parseAppTimestamp(value);
    if (!date) return '';
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

export function formatMessageDateLabel(value) {
    const date = parseAppTimestamp(value);
    if (!date) return '';
    const now = new Date();
    const isToday = now.toDateString() === date.toDateString();
    return isToday ? 'Today' : date.toLocaleDateString();
}

export function formatRelativeTimestamp(value) {
    const date = parseAppTimestamp(value);
    if (!date) return '';

    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);
    const diffDays = Math.floor(diffMs / 86400000);

    if (diffMins < 1) return 'just now';
    if (diffMins < 60) return `${diffMins} min ago`;
    if (diffHours < 24) return `${diffHours} h ago`;
    if (diffDays < 7) return `${diffDays} d ago`;
    return date.toLocaleDateString();
}

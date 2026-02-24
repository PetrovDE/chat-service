class ThemeManager {
    constructor(storageKey = 'ui_theme') {
        this.storageKey = storageKey;
        this.themes = ['light', 'dark', 'system'];
    }

    getStoredTheme() {
        const value = localStorage.getItem(this.storageKey);
        return this.themes.includes(value) ? value : 'system';
    }

    resolveTheme(theme) {
        if (theme === 'system') {
            const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
            return prefersDark ? 'dark' : 'light';
        }
        return theme;
    }

    applyTheme(theme) {
        const effective = this.resolveTheme(theme);
        document.documentElement.setAttribute('data-theme', effective);
        localStorage.setItem(this.storageKey, theme);
        this.updateToggleLabel(theme, effective);
    }

    updateToggleLabel(theme, effective) {
        const btn = document.getElementById('themeToggle');
        if (!btn) return;

        const labelMap = {
            light: 'Light',
            dark: 'Dark',
            system: `System (${effective})`,
        };

        btn.setAttribute('aria-label', `Theme: ${labelMap[theme]}`);
        btn.textContent = theme === 'dark' ? 'Dark' : theme === 'light' ? 'Light' : 'System';
    }

    cycleTheme() {
        const current = this.getStoredTheme();
        const currentIndex = this.themes.indexOf(current);
        const nextTheme = this.themes[(currentIndex + 1) % this.themes.length];
        this.applyTheme(nextTheme);
    }

    init() {
        const theme = this.getStoredTheme();
        this.applyTheme(theme);

        const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
        mediaQuery.addEventListener('change', () => {
            if (this.getStoredTheme() === 'system') {
                this.applyTheme('system');
            }
        });

        const btn = document.getElementById('themeToggle');
        if (btn) {
            btn.addEventListener('click', () => this.cycleTheme());
        }
    }
}

export { ThemeManager };

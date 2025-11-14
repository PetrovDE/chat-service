// frontend/static/js/formatters.js

export function formatMarkdown(text) {
    if (!window.marked) {
        console.warn('⚠️ marked.js не загружена');
        return escapeHtml(text);
    }

    try {
        // Важно: используем marked.marked для вызова
        const result = marked.marked(text, {
            breaks: true,
            gfm: true,
        });
        return result;
    } catch (error) {
        console.error('❌ Ошибка markdown:', error);
        return escapeHtml(text);
    }
}

export function highlightCode(element) {
    if (!window.hljs) {
        console.warn('⚠️ highlight.js не загружена');
        return;
    }

    try {
        element.querySelectorAll('pre code').forEach(block => {
            hljs.highlightElement(block);
        });
    } catch (error) {
        console.error('❌ Highlight error:', error);
    }
}

export function formatMessage(text) {
    if (!text || text.trim() === '') return '';

    // Парсим markdown
    const html = formatMarkdown(text);

    // Подсвечиваем код
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = html;
    highlightCode(tempDiv);
    addCopyButtons(tempDiv);

    return tempDiv.innerHTML;
}

export function addCopyButtons(container) {
    container.querySelectorAll('pre').forEach(pre => {
        if (pre.querySelector('.copy-btn')) return;

        const code = pre.querySelector('code');
        if (!code) return;

        pre.style.position = 'relative';

        const btn = document.createElement('button');
        btn.className = 'copy-btn';
        btn.textContent = '📋';
        btn.style.cssText = `
            position: absolute;
            top: 8px;
            right: 8px;
            padding: 4px 8px;
            background: #1e1e1e;
            color: #fff;
            border: 1px solid #333;
            border-radius: 3px;
            cursor: pointer;
            font-size: 14px;
            opacity: 0;
            transition: opacity 0.2s;
            z-index: 10;
        `;

        pre.appendChild(btn);

        pre.addEventListener('mouseenter', () => btn.style.opacity = '1');
        pre.addEventListener('mouseleave', () => btn.style.opacity = '0');

        btn.addEventListener('click', () => {
            navigator.clipboard.writeText(code.textContent);
            btn.textContent = '✅';
            setTimeout(() => btn.textContent = '📋', 1500);
        });
    });
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

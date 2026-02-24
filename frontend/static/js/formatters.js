export function formatMarkdown(text) {
    if (!window.marked) {
        return escapeHtml(text);
    }

    try {
        const result = marked.marked(text, {
            breaks: true,
            gfm: true,
        });
        return sanitizeHtml(result);
    } catch (_) {
        return escapeHtml(text);
    }
}

export function highlightCode(element) {
    if (!window.hljs) return;

    try {
        element.querySelectorAll('pre code').forEach((block) => {
            hljs.highlightElement(block);
        });
    } catch (_) {
        // no-op
    }
}

export function formatMessage(text) {
    if (!text || text.trim() === '') return '';

    const html = formatMarkdown(text);
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = html;
    highlightCode(tempDiv);
    addCopyButtons(tempDiv);

    return tempDiv.innerHTML;
}

export function addCopyButtons(container) {
    container.querySelectorAll('pre').forEach((pre) => {
        if (pre.querySelector('.copy-btn')) return;

        const code = pre.querySelector('code');
        if (!code) return;

        pre.style.position = 'relative';

        const btn = document.createElement('button');
        btn.className = 'copy-btn';
        btn.textContent = 'Copy';
        btn.type = 'button';
        btn.style.cssText = [
            'position:absolute',
            'top:8px',
            'right:8px',
            'padding:4px 8px',
            'background:#1e1e1e',
            'color:#fff',
            'border:1px solid #333',
            'border-radius:6px',
            'cursor:pointer',
            'font-size:12px',
            'opacity:0',
            'transition:opacity .2s',
            'z-index:10',
        ].join(';');

        pre.appendChild(btn);

        pre.addEventListener('mouseenter', () => {
            btn.style.opacity = '1';
        });

        pre.addEventListener('mouseleave', () => {
            btn.style.opacity = '0';
        });

        btn.addEventListener('click', async () => {
            try {
                await navigator.clipboard.writeText(code.textContent || '');
                btn.textContent = 'Copied';
            } catch (_) {
                btn.textContent = 'Failed';
            }

            setTimeout(() => {
                btn.textContent = 'Copy';
            }, 1200);
        });
    });
}

function sanitizeHtml(html) {
    const template = document.createElement('template');
    template.innerHTML = html;

    template.content.querySelectorAll('script, iframe, object, embed').forEach((node) => {
        node.remove();
    });

    template.content.querySelectorAll('*').forEach((node) => {
        [...node.attributes].forEach((attr) => {
            const name = attr.name.toLowerCase();
            const value = attr.value.toLowerCase();
            if (name.startsWith('on')) {
                node.removeAttribute(attr.name);
            }
            if ((name === 'href' || name === 'src') && value.startsWith('javascript:')) {
                node.removeAttribute(attr.name);
            }
        });
    });

    return template.innerHTML;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

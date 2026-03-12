from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_settings_prompt_max_input_allows_500000():
    html = _read("frontend/index.html")
    assert 'id="promptMaxCharsInput"' in html
    assert 'max="500000"' in html


def test_settings_manager_clamps_prompt_max_chars_to_supported_range():
    src = _read("frontend/static/js/settings-manager.js")
    assert "PROMPT_MAX_CHARS_MAX = 500000" in src
    assert "clampPromptMaxChars" in src
    assert "this.settings.prompt_max_chars = clampPromptMaxChars" in src

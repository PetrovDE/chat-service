from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_files_sidebar_uses_same_time_helper_as_messages():
    files_sidebar = _read("frontend/static/js/files-sidebar-manager.js")
    assert "from './time-format.js'" in files_sidebar
    assert "formatRelativeTimestamp(file.uploaded_at)" in files_sidebar
    assert "formatDate(dateString)" not in files_sidebar


def test_chat_manager_uses_shared_time_parsing_and_formatting():
    chat_manager = _read("frontend/static/js/chat-manager.js")
    assert "from './time-format.js'" in chat_manager
    assert "formatMessageTimeLabel" in chat_manager
    assert "formatMessageDateLabel" in chat_manager
    assert "parseAppTimestamp" in chat_manager


def test_shared_time_formatter_treats_naive_iso_as_utc():
    time_helper = _read("frontend/static/js/time-format.js")
    assert "NAIVE_ISO_RE" in time_helper
    assert "raw = `${raw}Z`" in time_helper

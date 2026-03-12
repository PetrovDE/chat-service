import pytest
from pydantic import ValidationError

from app.schemas.chat import ChatMessage


def test_chat_schema_accepts_prompt_max_chars_500000():
    payload = ChatMessage(message="hello", prompt_max_chars=500000)
    assert payload.prompt_max_chars == 500000


def test_chat_schema_rejects_prompt_max_chars_above_limit():
    with pytest.raises(ValidationError) as exc:
        ChatMessage(message="hello", prompt_max_chars=500001)
    assert "prompt_max_chars" in str(exc.value)

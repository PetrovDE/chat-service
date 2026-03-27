# app/observability/context.py
from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any, Dict, Optional

request_id_ctx: ContextVar[Optional[str]] = ContextVar("request_id", default=None)
user_id_ctx: ContextVar[Optional[str]] = ContextVar("user_id", default=None)
conversation_id_ctx: ContextVar[Optional[str]] = ContextVar("conversation_id", default=None)
file_id_ctx: ContextVar[Optional[str]] = ContextVar("file_id", default=None)
upload_id_ctx: ContextVar[Optional[str]] = ContextVar("upload_id", default=None)
document_id_ctx: ContextVar[Optional[str]] = ContextVar("document_id", default=None)


def _normalize_context_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def bind_context_values(
    *,
    request_id: Any = None,
    user_id: Any = None,
    conversation_id: Any = None,
    file_id: Any = None,
    upload_id: Any = None,
    document_id: Any = None,
) -> Dict[ContextVar[Optional[str]], Token[Optional[str]]]:
    tokens: Dict[ContextVar[Optional[str]], Token[Optional[str]]] = {}
    fields = (
        (request_id_ctx, request_id),
        (user_id_ctx, user_id),
        (conversation_id_ctx, conversation_id),
        (file_id_ctx, file_id),
        (upload_id_ctx, upload_id),
        (document_id_ctx, document_id),
    )
    for ctx_var, raw_value in fields:
        value = _normalize_context_value(raw_value)
        if value is None:
            continue
        tokens[ctx_var] = ctx_var.set(value)
    return tokens


def reset_context_values(tokens: Dict[ContextVar[Optional[str]], Token[Optional[str]]]) -> None:
    for ctx_var, token in reversed(list(tokens.items())):
        ctx_var.reset(token)

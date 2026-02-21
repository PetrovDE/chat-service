# app/observability/context.py
from __future__ import annotations

from contextvars import ContextVar
from typing import Optional

request_id_ctx: ContextVar[Optional[str]] = ContextVar("request_id", default=None)
user_id_ctx: ContextVar[Optional[str]] = ContextVar("user_id", default=None)
conversation_id_ctx: ContextVar[Optional[str]] = ContextVar("conversation_id", default=None)
file_id_ctx: ContextVar[Optional[str]] = ContextVar("file_id", default=None)

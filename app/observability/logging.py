# app/observability/logging.py
from __future__ import annotations

import logging

from app.observability.context import (
    request_id_ctx,
    user_id_ctx,
    conversation_id_ctx,
    file_id_ctx,
)


class ContextFilter(logging.Filter):
    """Injects request/user/conversation/file ids into every LogRecord."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_ctx.get() or "-"
        record.user_id = user_id_ctx.get() or "-"
        record.conversation_id = conversation_id_ctx.get() or "-"
        record.file_id = file_id_ctx.get() or "-"
        return True

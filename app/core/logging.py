# app/core/logging.py
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

from app.core.config import settings
from app.observability.context import request_id_ctx, user_id_ctx, conversation_id_ctx, file_id_ctx


def setup_logging(
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
) -> None:
    """
    Logging setup with correlation fields:
      rid (request_id), uid (user_id), cid (conversation_id), fid (file_id)

    IMPORTANT:
      We inject these fields using LogRecordFactory so formatting never crashes
      even for uvicorn/internal logs.
    """
    level = (log_level or settings.LOG_LEVEL or "INFO").upper()

    # Ensure log dir exists
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    # Inject correlation fields into EVERY record (the only robust way)
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.request_id = request_id_ctx.get() or getattr(record, "request_id", "-") or "-"
        record.user_id = user_id_ctx.get() or getattr(record, "user_id", "-") or "-"
        record.conversation_id = conversation_id_ctx.get() or getattr(record, "conversation_id", "-") or "-"
        record.file_id = file_id_ctx.get() or getattr(record, "file_id", "-") or "-"
        return record

    logging.setLogRecordFactory(record_factory)

    # Formatter (no emojis, compact, production-friendly)
    log_format = (
        "%(asctime)s %(levelname)s %(name)s "
        "rid=%(request_id)s uid=%(user_id)s cid=%(conversation_id)s fid=%(file_id)s - %(message)s"
    )
    date_format = "%Y-%m-%d %H:%M:%S"

    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format=log_format,
        datefmt=date_format,
        handlers=handlers,
    )

    # Reduce noisy loggers
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)

    logging.getLogger(__name__).info("Logging configured (level=%s)", level)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

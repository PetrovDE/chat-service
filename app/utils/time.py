from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def ensure_utc_datetime(value: Optional[datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def to_utc_iso(value: Optional[datetime]) -> Optional[str]:
    dt = ensure_utc_datetime(value)
    if dt is None:
        return None
    return dt.isoformat()

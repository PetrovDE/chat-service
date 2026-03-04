from __future__ import annotations

from typing import Optional

import httpx

from app.services.llm.exceptions import CircuitOpenError


def classify_aihub_failure(exc: Exception) -> Optional[str]:
    if isinstance(exc, CircuitOpenError):
        return "circuit_open"
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.NetworkError):
        return "network"
    if isinstance(exc, httpx.HTTPStatusError):
        status = int(getattr(exc.response, "status_code", 0) or 0)
        if 500 <= status <= 599:
            return "hub_5xx"
    return None


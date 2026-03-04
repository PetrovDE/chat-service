from __future__ import annotations


class CircuitOpenError(RuntimeError):
    """Raised when the AI HUB circuit breaker is open."""


class AIHubUnavailableError(RuntimeError):
    """Raised when AI HUB is unavailable and fallback is denied."""


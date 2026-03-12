from __future__ import annotations


class CircuitOpenError(RuntimeError):
    """Raised when the AI HUB circuit breaker is open."""


class AIHubUnavailableError(RuntimeError):
    """Raised when AI HUB is unavailable and fallback is denied."""


class LLMProviderError(RuntimeError):
    """Base provider error with retryability metadata."""

    def __init__(self, message: str, *, provider: str, status_code: int | None = None, retryable: bool = False):
        super().__init__(message)
        self.provider = provider
        self.status_code = status_code
        self.retryable = retryable


class ProviderAuthError(LLMProviderError):
    """Authentication/authorization error. Non-retryable."""

    def __init__(self, message: str, *, provider: str, status_code: int | None = None):
        super().__init__(message, provider=provider, status_code=status_code, retryable=False)


class ProviderTransientError(LLMProviderError):
    """Transient provider/network error. Retryable."""

    def __init__(self, message: str, *, provider: str, status_code: int | None = None):
        super().__init__(message, provider=provider, status_code=status_code, retryable=True)


class ProviderConfigError(LLMProviderError):
    """Configuration/request error. Non-retryable."""

    def __init__(self, message: str, *, provider: str, status_code: int | None = None):
        super().__init__(message, provider=provider, status_code=status_code, retryable=False)

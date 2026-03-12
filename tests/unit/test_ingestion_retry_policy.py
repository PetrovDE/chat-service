from types import SimpleNamespace

from app.services.file_pipeline import classify_ingestion_exception
from app.services.llm.exceptions import ProviderAuthError, ProviderTransientError


def test_retry_policy_does_not_retry_on_auth_errors():
    classified = classify_ingestion_exception(
        ProviderAuthError("unauthorized", provider="aihub", status_code=401)
    )
    assert classified["retryable"] is False
    assert classified["fatal"] is True
    assert "401" in classified["code"]


def test_retry_policy_retries_on_transient_provider_errors():
    classified = classify_ingestion_exception(
        ProviderTransientError("timeout", provider="aihub", status_code=429)
    )
    assert classified["retryable"] is True
    assert classified["fatal"] is False


def test_retry_policy_retries_on_transient_http_statuses():
    exc = RuntimeError("remote timeout")
    exc.response = SimpleNamespace(status_code=503)
    classified = classify_ingestion_exception(exc)
    assert classified["retryable"] is True
    assert classified["fatal"] is False


def test_retry_policy_marks_unauthorized_http_status_fatal():
    exc = RuntimeError("unauthorized")
    exc.response = SimpleNamespace(status_code=401)
    classified = classify_ingestion_exception(exc)
    assert classified["retryable"] is False
    assert classified["fatal"] is True

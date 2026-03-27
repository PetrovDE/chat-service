import pytest

from scripts.evals.online import run_online_eval_sync


class _FakeAsyncClient:
    def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
        _ = (args, kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
        _ = (exc_type, exc, tb)
        return False

    async def post(self, path, json):  # noqa: ANN001, ANN201
        assert path == "/api/v1/chat/"
        _ = json
        payload = {
            "response": "Full analytics report with actionable insights and chart interpretation.",
            "execution_route": "complex_analytics",
            "executor_status": "success",
            "artifacts": [{"kind": "histogram", "url": "/uploads/chart.png"}],
            "rag_debug": {
                "complex_analytics": {
                    "metrics": {"rows_total": 10, "columns_total": 3},
                    "response_status": "fallback",
                    "response_error_code": "low_content_quality",
                }
            },
        }
        return _FakeResponse(payload)


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


def test_online_eval_collects_complex_quality_metric(monkeypatch):
    monkeypatch.setattr("scripts.evals.online.httpx.AsyncClient", _FakeAsyncClient)
    monkeypatch.setenv("EVAL_COMPLEX_ANALYTICS_CONVERSATION_ID", "conv-123")

    datasets = {
        "complex_analytics_quality_online": [
            {
                "id": "case-1",
                "online_metric": "complex_analytics_report_quality",
                "online_request": {
                    "message": "Analyze file fully with charts",
                    "conversation_id": "${EVAL_COMPLEX_ANALYTICS_CONVERSATION_ID}",
                    "rag_debug": True,
                },
                "online_expect": {
                    "execution_route": "complex_analytics",
                    "executor_status": "success",
                    "artifacts_min": 1,
                    "response_contains_any": ["analytics", "insight"],
                    "metrics_required_keys": ["rows_total", "columns_total"],
                },
                "max_latency_ms": 5000,
            }
        ]
    }

    report = run_online_eval_sync(
        datasets=datasets,
        base_url="http://localhost:8000",
        timeout_seconds=5.0,
    )
    assert report["executed_cases"] == 1
    assert report["passed_cases"] == 1
    assert report["score"] == 1.0
    assert report["metrics"]["complex_analytics_report_quality"]["score"] == 1.0
    assert report["by_dataset"]["complex_analytics_quality_online"]["score"] == 1.0
    assert report["skipped_cases_count"] == 0


def test_online_eval_raises_on_missing_env_placeholder(monkeypatch):
    monkeypatch.setattr("scripts.evals.online.httpx.AsyncClient", _FakeAsyncClient)
    monkeypatch.delenv("EVAL_COMPLEX_ANALYTICS_CONVERSATION_ID", raising=False)

    datasets = {
        "complex_analytics_quality_online": [
            {
                "id": "case-1",
                "online_metric": "complex_analytics_report_quality",
                "online_request": {
                    "message": "Analyze file fully with charts",
                    "conversation_id": "${EVAL_COMPLEX_ANALYTICS_CONVERSATION_ID}",
                },
                "online_expect": {"execution_route": "complex_analytics"},
            }
        ]
    }

    with pytest.raises(ValueError, match="Missing required environment variable"):
        run_online_eval_sync(
            datasets=datasets,
            base_url="http://localhost:8000",
            timeout_seconds=5.0,
        )


def test_online_eval_payload_path_operators_and_env_gated_skip(monkeypatch):
    monkeypatch.setattr("scripts.evals.online.httpx.AsyncClient", _FakeAsyncClient)
    monkeypatch.delenv("EVAL_OPTIONAL_CASE_ENABLED", raising=False)

    datasets = {
        "rag_retrieval_quality_online": [
            {
                "id": "case-path-checks",
                "online_metric": "retrieval_relevance",
                "online_request": {"message": "check payload operators"},
                "online_expect": {
                    "payload_present": ["rag_debug.complex_analytics.metrics.rows_total"],
                    "payload_equals": {"executor_status": "success"},
                    "payload_in": {"execution_route": ["tabular_sql", "complex_analytics"]},
                    "payload_min": {"rag_debug.complex_analytics.metrics.rows_total": 5},
                    "payload_contains_any": {"response": ["insight", "analysis"]},
                },
            },
            {
                "id": "case-env-gated",
                "online_metric": "retrieval_relevance",
                "enabled_if_env": "EVAL_OPTIONAL_CASE_ENABLED",
                "online_request": {"message": "must be skipped"},
                "online_expect": {"payload_equals": {"executor_status": "success"}},
            },
        ]
    }

    report = run_online_eval_sync(
        datasets=datasets,
        base_url="http://localhost:8000",
        timeout_seconds=5.0,
    )

    assert report["executed_cases"] == 1
    assert report["passed_cases"] == 1
    assert report["metrics"]["retrieval_relevance"]["score"] == 1.0
    assert report["skipped_cases_count"] == 1
    assert report["skipped_cases"][0]["id"] == "case-env-gated"

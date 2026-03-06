from pathlib import Path

from scripts.evals import runner


def test_online_eval_runner_uses_online_datasets_only(monkeypatch):
    captured = {}

    def fake_run_online_eval_sync(*, datasets, base_url, timeout_seconds, auth_bearer_token=None):  # noqa: ANN001
        captured["datasets"] = datasets
        captured["base_url"] = base_url
        captured["timeout_seconds"] = timeout_seconds
        captured["auth_bearer_token"] = auth_bearer_token
        return {
            "executed_cases": 1,
            "passed_cases": 1,
            "score": 1.0,
            "metrics": {"complex_analytics_report_quality": {"passed": 1, "total": 1, "score": 1.0}},
            "cases": [],
            "latency_ms": [100.0],
            "latency_p95_ms": 100.0,
            "latency_p95_ms_by_dataset": {"complex_analytics_quality_online": 100.0},
            "latency_violations": [],
            "by_dataset": {"complex_analytics_quality_online": {"executed_cases": 1, "passed_cases": 1, "score": 1.0}},
        }

    monkeypatch.setattr(runner, "run_online_eval_sync", fake_run_online_eval_sync)

    summary = runner.run_eval_suite(
        mode="online",
        dataset_root=Path("tests/evals/datasets"),
        online_base_url="http://localhost:8000",
        online_timeout_seconds=12.0,
    )

    assert summary["mode"] == "online"
    assert summary["offline_reports"] == {}
    assert summary["metrics"] == {}
    assert "online_report" in summary
    assert set(captured["datasets"].keys()) == set(runner.ONLINE_DATASET_NAMES)

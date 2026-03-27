from pathlib import Path

from scripts.evals.ci_gates import evaluate_ci_gates, load_gate_config
from scripts.evals import runner


def _report(
    dataset: str,
    *,
    passed_cases: int,
    total_cases: int,
    latency_p95_ms: float,
    extra: dict | None = None,
):
    payload = {
        "dataset": dataset,
        "total_cases": total_cases,
        "passed_cases": passed_cases,
        "score": (passed_cases / total_cases) if total_cases else 0.0,
        "latency_p95_ms": latency_p95_ms,
        "latency_violations": [],
        "cases": [],
    }
    if extra:
        payload.update(extra)
    return payload


def test_offline_eval_runner_metrics_and_gates_with_deterministic_reports(monkeypatch):
    monkeypatch.setattr(
        runner,
        "run_tabular_aggregate_eval",
        lambda cases, temp_dir: _report(
            "tabular_aggregate_golden",
            passed_cases=3,
            total_cases=3,
            latency_p95_ms=100.0,
            extra={"numeric_checks_passed": 3, "numeric_checks_total": 3},
        ),
    )
    monkeypatch.setattr(
        runner,
        "run_tabular_profile_eval",
        lambda cases, temp_dir: _report(
            "tabular_profile_golden",
            passed_cases=2,
            total_cases=2,
            latency_p95_ms=100.0,
            extra={"numeric_checks_passed": 2, "numeric_checks_total": 2},
        ),
    )
    monkeypatch.setattr(
        runner,
        "run_narrative_rag_eval",
        lambda cases: _report(
            "narrative_rag_golden",
            passed_cases=2,
            total_cases=2,
            latency_p95_ms=50.0,
            extra={"supported_claims": 5, "total_claims": 5},
        ),
    )
    monkeypatch.setattr(
        runner,
        "run_fallback_route_eval",
        lambda cases: _report(
            "fallback_route_golden",
            passed_cases=5,
            total_cases=5,
            latency_p95_ms=100.0,
            extra={"route_checks_passed": 5, "route_checks_total": 5, "route_latency_p95_ms": {}},
        ),
    )
    monkeypatch.setattr(
        runner,
        "run_complex_analytics_quality_eval",
        lambda cases, temp_dir: _report(
            "complex_analytics_quality_golden",
            passed_cases=1,
            total_cases=1,
            latency_p95_ms=300.0,
            extra={"quality_checks_passed": 1, "quality_checks_total": 1},
        ),
    )
    monkeypatch.setattr(
        runner,
        "run_tabular_langgraph_eval_slice",
        lambda cases, temp_dir: _report(
            "tabular_langgraph_eval_slice_golden",
            passed_cases=5,
            total_cases=5,
            latency_p95_ms=300.0,
            extra={
                "langgraph_passed_cases": 5,
                "legacy_passed_cases": 5,
                "explainability_gain_cases": 5,
            },
        ),
    )

    summary = runner.run_eval_suite(mode="offline", dataset_root=Path("tests/evals/datasets"))
    gates = evaluate_ci_gates(summary, load_gate_config(Path("tests/evals/gates.json")))

    numeric = summary["metrics"]["numeric_exact_match"]
    citation = summary["metrics"]["citation_faithfulness"]
    route = summary["metrics"]["route_correctness"]
    complex_quality = summary["metrics"]["complex_analytics_report_quality"]
    langgraph_correctness = summary["metrics"]["langgraph_eval_correctness"]
    langgraph_delta = summary["metrics"]["langgraph_vs_legacy_correctness_delta"]
    langgraph_explainability = summary["metrics"]["langgraph_explainability_gain"]

    assert numeric["score"] == 1.0
    assert citation["score"] == 1.0
    assert route["score"] == 1.0
    assert complex_quality["score"] == 1.0
    assert langgraph_correctness["score"] == 1.0
    assert langgraph_delta["score"] >= 0.0
    assert langgraph_explainability["score"] == 1.0
    assert gates["passed"] is True
    assert not summary["latency"]["violations"]

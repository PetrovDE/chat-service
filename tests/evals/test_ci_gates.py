from scripts.evals.ci_gates import evaluate_ci_gates


def test_ci_gates_fail_when_metrics_drop_below_thresholds():
    summary = {
        "metrics": {
            "numeric_exact_match": {"score": 0.8},
            "citation_faithfulness": {"score": 0.9},
            "route_correctness": {"score": 0.7},
        },
        "latency": {
            "violations": [{"dataset": "tabular_aggregate_golden"}],
            "p95_ms_by_dataset": {"tabular_aggregate_golden": 3000.0},
        },
    }
    gate_config = {
        "numeric_exact_match_min": 1.0,
        "citation_faithfulness_min": 1.0,
        "route_correctness_min": 1.0,
        "max_latency_violations": 0,
        "p95_latency_ms": {"tabular_aggregate_golden": 2500.0},
    }

    result = evaluate_ci_gates(summary, gate_config)
    assert result["passed"] is False
    assert any(check["name"] == "numeric_exact_match" and check["passed"] is False for check in result["checks"])
    assert any(check["name"] == "citation_faithfulness" and check["passed"] is False for check in result["checks"])
    assert any(check["name"] == "route_correctness" and check["passed"] is False for check in result["checks"])
    assert any(
        check["name"] == "latency_regression_violations" and check["passed"] is False for check in result["checks"]
    )


def test_ci_gates_fail_when_online_complex_quality_below_preprod_threshold():
    summary = {
        "metrics": {
            "numeric_exact_match": {"score": 1.0},
            "citation_faithfulness": {"score": 1.0},
            "route_correctness": {"score": 1.0},
            "complex_analytics_report_quality": {"score": 1.0},
        },
        "latency": {
            "violations": [],
            "p95_ms_by_dataset": {},
        },
        "online_report": {
            "metrics": {
                "complex_analytics_report_quality": {"score": 0.5},
            },
            "latency_violations": [],
            "latency_p95_ms_by_dataset": {"complex_analytics_quality_online": 8000.0},
        },
    }
    gate_config = {
        "numeric_exact_match_min": 1.0,
        "citation_faithfulness_min": 1.0,
        "route_correctness_min": 1.0,
        "complex_analytics_report_quality_min": 1.0,
        "online_complex_analytics_report_quality_min": 1.0,
        "max_latency_violations": 0,
        "online_max_latency_violations": 0,
        "p95_latency_ms": {},
        "online_p95_latency_ms": {"complex_analytics_quality_online": 12000.0},
    }

    result = evaluate_ci_gates(summary, gate_config)
    assert result["passed"] is False
    assert any(
        check["name"] == "online_complex_analytics_report_quality" and check["passed"] is False
        for check in result["checks"]
    )

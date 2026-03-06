from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_GATES: Dict[str, Any] = {
    "numeric_exact_match_min": 1.0,
    "citation_faithfulness_min": 1.0,
    "route_correctness_min": 1.0,
    "complex_analytics_report_quality_min": 1.0,
    "online_complex_analytics_report_quality_min": 0.0,
    "max_latency_violations": 0,
    "online_max_latency_violations": 0,
    "p95_latency_ms": {
        "tabular_aggregate_golden": 2500.0,
        "tabular_profile_golden": 3000.0,
        "fallback_route_golden": 1500.0,
        "complex_analytics_quality_golden": 4000.0,
    },
    "online_p95_latency_ms": {},
}


def load_gate_config(path: Optional[Path]) -> Dict[str, Any]:
    if path is None:
        return dict(DEFAULT_GATES)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Gate config must be a JSON object: {path}")
    merged = dict(DEFAULT_GATES)
    merged.update(payload)
    return merged


def _metric_score(summary: Dict[str, Any], metric_name: str) -> float:
    metrics = summary.get("metrics", {})
    payload = metrics.get(metric_name, {})
    return float(payload.get("score", 0.0) or 0.0)


def _add_check(checks: list, *, name: str, passed: bool, observed: Any, threshold: Any) -> None:
    checks.append(
        {
            "name": name,
            "passed": bool(passed),
            "observed": observed,
            "threshold": threshold,
        }
    )


def evaluate_ci_gates(summary: Dict[str, Any], gate_config: Dict[str, Any]) -> Dict[str, Any]:
    checks = []

    numeric_score = _metric_score(summary, "numeric_exact_match")
    numeric_threshold = float(gate_config.get("numeric_exact_match_min", 1.0))
    _add_check(
        checks,
        name="numeric_exact_match",
        passed=numeric_score >= numeric_threshold,
        observed=round(numeric_score, 6),
        threshold=numeric_threshold,
    )

    citation_score = _metric_score(summary, "citation_faithfulness")
    citation_threshold = float(gate_config.get("citation_faithfulness_min", 1.0))
    _add_check(
        checks,
        name="citation_faithfulness",
        passed=citation_score >= citation_threshold,
        observed=round(citation_score, 6),
        threshold=citation_threshold,
    )

    route_score = _metric_score(summary, "route_correctness")
    route_threshold = float(gate_config.get("route_correctness_min", 1.0))
    _add_check(
        checks,
        name="route_correctness",
        passed=route_score >= route_threshold,
        observed=round(route_score, 6),
        threshold=route_threshold,
    )

    complex_quality_score = _metric_score(summary, "complex_analytics_report_quality")
    complex_quality_threshold = float(gate_config.get("complex_analytics_report_quality_min", 1.0))
    _add_check(
        checks,
        name="complex_analytics_report_quality",
        passed=complex_quality_score >= complex_quality_threshold,
        observed=round(complex_quality_score, 6),
        threshold=complex_quality_threshold,
    )

    latency_payload = summary.get("latency", {})
    violations = latency_payload.get("violations", [])
    max_violations = int(gate_config.get("max_latency_violations", 0))
    _add_check(
        checks,
        name="latency_regression_violations",
        passed=len(violations) <= max_violations,
        observed=len(violations),
        threshold=max_violations,
    )

    p95_thresholds = gate_config.get("p95_latency_ms", {})
    observed_p95 = latency_payload.get("p95_ms_by_dataset", {})
    if isinstance(p95_thresholds, dict):
        for dataset_name, threshold in sorted(p95_thresholds.items()):
            threshold_value = float(threshold)
            observed_value = float(observed_p95.get(dataset_name, 0.0) or 0.0)
            _add_check(
                checks,
                name=f"p95_latency::{dataset_name}",
                passed=observed_value <= threshold_value,
                observed=round(observed_value, 3),
                threshold=threshold_value,
            )

    online_report = summary.get("online_report") if isinstance(summary.get("online_report"), dict) else None
    online_threshold = float(gate_config.get("online_complex_analytics_report_quality_min", 0.0))
    if online_report is not None or online_threshold > 0.0:
        online_metrics = (online_report or {}).get("metrics", {})
        online_complex_quality = (online_metrics.get("complex_analytics_report_quality") or {})
        online_score = float(online_complex_quality.get("score", 0.0) or 0.0)
        _add_check(
            checks,
            name="online_complex_analytics_report_quality",
            passed=online_score >= online_threshold,
            observed=round(online_score, 6),
            threshold=online_threshold,
        )

    online_max_violations = gate_config.get("online_max_latency_violations")
    if online_report is not None and online_max_violations is not None:
        online_violations = (online_report or {}).get("latency_violations")
        violations_count = len(online_violations) if isinstance(online_violations, list) else 0
        max_violations = int(online_max_violations)
        _add_check(
            checks,
            name="online_latency_regression_violations",
            passed=violations_count <= max_violations,
            observed=violations_count,
            threshold=max_violations,
        )

    online_p95_thresholds = gate_config.get("online_p95_latency_ms")
    if online_report is not None and isinstance(online_p95_thresholds, dict):
        observed_online_p95 = (online_report or {}).get("latency_p95_ms_by_dataset")
        observed_online_p95 = observed_online_p95 if isinstance(observed_online_p95, dict) else {}
        for dataset_name, threshold in sorted(online_p95_thresholds.items()):
            threshold_value = float(threshold)
            observed_value = float(observed_online_p95.get(dataset_name, 0.0) or 0.0)
            _add_check(
                checks,
                name=f"online_p95_latency::{dataset_name}",
                passed=observed_value <= threshold_value,
                observed=round(observed_value, 3),
                threshold=threshold_value,
            )

    passed = all(bool(check.get("passed")) for check in checks)
    return {"passed": passed, "checks": checks}

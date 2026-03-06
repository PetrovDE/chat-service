from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any, Dict

from scripts.evals.datasets import load_named_datasets
from scripts.evals.offline import (
    run_complex_analytics_quality_eval,
    run_fallback_route_eval,
    run_narrative_rag_eval,
    run_tabular_aggregate_eval,
    run_tabular_profile_eval,
)
from scripts.evals.online import run_online_eval_sync

OFFLINE_DATASET_NAMES = (
    "tabular_aggregate_golden",
    "tabular_profile_golden",
    "narrative_rag_golden",
    "fallback_route_golden",
    "complex_analytics_quality_golden",
)

ONLINE_DATASET_NAMES = (
    "complex_analytics_quality_online",
)


def _safe_ratio(passed: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return float(passed) / float(total)


def _p95_by_dataset(offline_reports: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    payload = {}
    for dataset_name, report in offline_reports.items():
        payload[dataset_name] = float(report.get("latency_p95_ms", 0.0) or 0.0)
    return payload


def _collect_violations(offline_reports: Dict[str, Dict[str, Any]]) -> list:
    violations = []
    for report in offline_reports.values():
        for item in report.get("latency_violations", []) or []:
            violations.append(item)
    return violations


def _build_metrics(offline_reports: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    aggregate = offline_reports["tabular_aggregate_golden"]
    profile = offline_reports["tabular_profile_golden"]
    narrative = offline_reports["narrative_rag_golden"]
    fallback = offline_reports["fallback_route_golden"]
    complex_quality = offline_reports["complex_analytics_quality_golden"]

    numeric_passed = int(aggregate.get("numeric_checks_passed", 0)) + int(profile.get("numeric_checks_passed", 0))
    numeric_total = int(aggregate.get("numeric_checks_total", 0)) + int(profile.get("numeric_checks_total", 0))

    supported_claims = int(narrative.get("supported_claims", 0))
    total_claims = int(narrative.get("total_claims", 0))

    route_checks_passed = int(fallback.get("route_checks_passed", 0))
    route_checks_total = int(fallback.get("route_checks_total", 0))
    quality_checks_passed = int(complex_quality.get("quality_checks_passed", 0))
    quality_checks_total = int(complex_quality.get("quality_checks_total", 0))

    return {
        "numeric_exact_match": {
            "passed": numeric_passed,
            "total": numeric_total,
            "score": _safe_ratio(numeric_passed, numeric_total),
        },
        "citation_faithfulness": {
            "supported_claims": supported_claims,
            "total_claims": total_claims,
            "score": _safe_ratio(supported_claims, total_claims),
        },
        "route_correctness": {
            "passed": route_checks_passed,
            "total": route_checks_total,
            "score": _safe_ratio(route_checks_passed, route_checks_total),
        },
        "complex_analytics_report_quality": {
            "passed": quality_checks_passed,
            "total": quality_checks_total,
            "score": _safe_ratio(quality_checks_passed, quality_checks_total),
        },
    }


def run_eval_suite(
    *,
    mode: str,
    dataset_root: Path,
    online_base_url: str | None = None,
    online_timeout_seconds: float = 20.0,
    online_auth_bearer_token: str | None = None,
) -> Dict[str, Any]:
    selected_mode = str(mode or "offline").strip().lower()
    if selected_mode not in {"offline", "online", "hybrid"}:
        raise ValueError("mode must be one of: offline, online, hybrid")

    offline_datasets = (
        load_named_datasets(dataset_root=dataset_root, dataset_names=OFFLINE_DATASET_NAMES)
        if selected_mode in {"offline", "hybrid"}
        else {}
    )
    online_datasets = (
        load_named_datasets(dataset_root=dataset_root, dataset_names=ONLINE_DATASET_NAMES)
        if selected_mode in {"online", "hybrid"}
        else {}
    )
    offline_reports: Dict[str, Dict[str, Any]] = {}
    online_report: Dict[str, Any] | None = None

    if selected_mode in {"offline", "hybrid"}:
        with tempfile.TemporaryDirectory(prefix="llama_eval_") as tmp_dir_raw:
            tmp_dir = Path(tmp_dir_raw)
            offline_reports["tabular_aggregate_golden"] = run_tabular_aggregate_eval(
                offline_datasets["tabular_aggregate_golden"],
                temp_dir=tmp_dir,
            )
            offline_reports["tabular_profile_golden"] = run_tabular_profile_eval(
                offline_datasets["tabular_profile_golden"],
                temp_dir=tmp_dir,
            )
            offline_reports["complex_analytics_quality_golden"] = run_complex_analytics_quality_eval(
                offline_datasets["complex_analytics_quality_golden"],
                temp_dir=tmp_dir,
            )
        offline_reports["narrative_rag_golden"] = run_narrative_rag_eval(offline_datasets["narrative_rag_golden"])
        offline_reports["fallback_route_golden"] = run_fallback_route_eval(offline_datasets["fallback_route_golden"])

    if selected_mode in {"online", "hybrid"}:
        if not online_base_url:
            raise ValueError("online_base_url is required for online/hybrid mode")
        online_report = run_online_eval_sync(
            datasets=online_datasets,
            base_url=online_base_url,
            timeout_seconds=float(online_timeout_seconds),
            auth_bearer_token=online_auth_bearer_token,
        )

    metrics = _build_metrics(offline_reports) if offline_reports else {}
    latency = {
        "p95_ms_by_dataset": _p95_by_dataset(offline_reports),
        "violations": _collect_violations(offline_reports),
        "route_p95_ms": offline_reports.get("fallback_route_golden", {}).get("route_latency_p95_ms", {}),
    }

    summary: Dict[str, Any] = {
        "mode": selected_mode,
        "datasets_root": str(dataset_root),
        "offline_reports": offline_reports,
        "metrics": metrics,
        "latency": latency,
    }
    if online_report is not None:
        summary["online_report"] = online_report
    return summary

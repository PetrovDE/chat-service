from __future__ import annotations

import asyncio
import os
import re
from time import perf_counter
from typing import Any, Dict, List, Sequence

import httpx


def _percentile(values: Sequence[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * max(0.0, min(1.0, p))
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def _contains_all(text: str, parts: Sequence[str]) -> bool:
    lowered = str(text or "").lower()
    return all(str(part).lower() in lowered for part in parts)


def _contains_any(text: str, parts: Sequence[str]) -> bool:
    lowered = str(text or "").lower()
    return any(str(part).lower() in lowered for part in parts)


def _safe_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _response_text(response_payload: Dict[str, Any]) -> str:
    return str(response_payload.get("response") or "")


def _complex_debug(response_payload: Dict[str, Any]) -> Dict[str, Any]:
    rag_debug = _safe_dict(response_payload.get("rag_debug"))
    return _safe_dict(rag_debug.get("complex_analytics"))


_ENV_PLACEHOLDER_RE = re.compile(r"^\$\{([A-Z0-9_]+)\}$")
_MISSING = object()


def _resolve_path(payload: Any, path: str) -> Any:
    node = payload
    for token in str(path or "").split("."):
        if not token:
            return _MISSING
        if isinstance(node, dict):
            if token not in node:
                return _MISSING
            node = node[token]
            continue
        if isinstance(node, list) and token.isdigit():
            index = int(token)
            if index < 0 or index >= len(node):
                return _MISSING
            node = node[index]
            continue
        return _MISSING
    return node


def _append_payload_equals_checks(checks: List[Dict[str, Any]], *, payload: Dict[str, Any], expected: Dict[str, Any]) -> None:
    payload_equals = expected.get("payload_equals")
    if not isinstance(payload_equals, dict):
        return
    for raw_path, expected_value in payload_equals.items():
        path = str(raw_path)
        observed_value = _resolve_path(payload, path)
        checks.append(
            {
                "name": f"payload_equals::{path}",
                "passed": (observed_value is not _MISSING) and (observed_value == expected_value),
                "observed": None if observed_value is _MISSING else observed_value,
                "expected": expected_value,
            }
        )


def _append_payload_in_checks(checks: List[Dict[str, Any]], *, payload: Dict[str, Any], expected: Dict[str, Any]) -> None:
    payload_in = expected.get("payload_in")
    if not isinstance(payload_in, dict):
        return
    for raw_path, allowed_values in payload_in.items():
        if not isinstance(allowed_values, list):
            continue
        path = str(raw_path)
        observed_value = _resolve_path(payload, path)
        checks.append(
            {
                "name": f"payload_in::{path}",
                "passed": (observed_value is not _MISSING) and (observed_value in allowed_values),
                "observed": None if observed_value is _MISSING else observed_value,
                "expected": list(allowed_values),
            }
        )


def _append_payload_present_checks(
    checks: List[Dict[str, Any]], *, payload: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    payload_present = expected.get("payload_present")
    if not isinstance(payload_present, list):
        return
    for item in payload_present:
        path = str(item or "")
        observed_value = _resolve_path(payload, path)
        observed_present = observed_value is not _MISSING and observed_value is not None
        checks.append(
            {
                "name": f"payload_present::{path}",
                "passed": observed_present,
                "observed": observed_present,
                "expected": True,
            }
        )


def _append_payload_numeric_checks(
    checks: List[Dict[str, Any]], *, payload: Dict[str, Any], expected: Dict[str, Any], op_name: str, comparator: str
) -> None:
    raw_thresholds = expected.get(op_name)
    if not isinstance(raw_thresholds, dict):
        return
    for raw_path, threshold in raw_thresholds.items():
        path = str(raw_path)
        observed_value = _resolve_path(payload, path)
        passed = False
        observed_numeric: float | None = None
        threshold_value = float(threshold)
        try:
            observed_numeric = float(observed_value)
            if comparator == "min":
                passed = observed_numeric >= threshold_value
            else:
                passed = observed_numeric <= threshold_value
        except (TypeError, ValueError):
            passed = False
        checks.append(
            {
                "name": f"{op_name}::{path}",
                "passed": passed,
                "observed": observed_numeric,
                "expected": threshold_value,
            }
        )


def _append_payload_text_checks(
    checks: List[Dict[str, Any]], *, payload: Dict[str, Any], expected: Dict[str, Any], op_name: str, mode: str
) -> None:
    raw_payload = expected.get(op_name)
    if not isinstance(raw_payload, dict):
        return
    for raw_path, raw_values in raw_payload.items():
        if not isinstance(raw_values, list):
            continue
        path = str(raw_path)
        observed_value = _resolve_path(payload, path)
        observed_text = str(observed_value or "")
        if mode == "all":
            passed = _contains_all(observed_text, [str(item) for item in raw_values])
        else:
            passed = _contains_any(observed_text, [str(item) for item in raw_values])
        checks.append(
            {
                "name": f"{op_name}::{path}",
                "passed": observed_value is not _MISSING and passed,
                "observed": observed_text,
                "expected": list(raw_values),
            }
        )


def _resolve_placeholders(value: Any) -> Any:
    if isinstance(value, str):
        match = _ENV_PLACEHOLDER_RE.match(value.strip())
        if not match:
            return value
        env_name = match.group(1)
        env_value = os.getenv(env_name)
        if not env_value:
            raise ValueError(f"Missing required environment variable for online eval request: {env_name}")
        return env_value
    if isinstance(value, list):
        return [_resolve_placeholders(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _resolve_placeholders(v) for k, v in value.items()}
    return value


def _evaluate_case_response(case: Dict[str, Any], response_payload: Dict[str, Any]) -> Dict[str, Any]:
    expected = case.get("online_expect") or {}
    checks: List[Dict[str, Any]] = []
    complex_debug = _complex_debug(response_payload)

    if "model_route" in expected:
        observed = str(response_payload.get("model_route") or "")
        expected_value = str(expected.get("model_route") or "")
        checks.append({"name": "model_route", "passed": observed == expected_value, "observed": observed, "expected": expected_value})

    if "fallback_reason" in expected:
        observed = str(response_payload.get("fallback_reason") or "")
        expected_value = str(expected.get("fallback_reason") or "")
        checks.append(
            {"name": "fallback_reason", "passed": observed == expected_value, "observed": observed, "expected": expected_value}
        )

    contains = expected.get("response_contains")
    if isinstance(contains, list):
        observed_response = _response_text(response_payload)
        passed = _contains_all(observed_response, [str(item) for item in contains])
        checks.append(
            {
                "name": "response_contains",
                "passed": passed,
                "observed": observed_response,
                "expected": list(contains),
            }
        )

    contains_any = expected.get("response_contains_any")
    if isinstance(contains_any, list):
        observed_response = _response_text(response_payload)
        passed = _contains_any(observed_response, [str(item) for item in contains_any])
        checks.append(
            {
                "name": "response_contains_any",
                "passed": passed,
                "observed": observed_response,
                "expected": list(contains_any),
            }
        )

    if "sources_min" in expected:
        sources = response_payload.get("sources") if isinstance(response_payload.get("sources"), list) else []
        observed = len(sources)
        threshold = int(expected.get("sources_min") or 0)
        checks.append({"name": "sources_min", "passed": observed >= threshold, "observed": observed, "expected": threshold})

    if "execution_route" in expected:
        observed = str(response_payload.get("execution_route") or "")
        expected_value = str(expected.get("execution_route") or "")
        checks.append(
            {"name": "execution_route", "passed": observed == expected_value, "observed": observed, "expected": expected_value}
        )

    if "executor_status" in expected:
        observed = str(response_payload.get("executor_status") or "")
        expected_value = str(expected.get("executor_status") or "")
        checks.append(
            {"name": "executor_status", "passed": observed == expected_value, "observed": observed, "expected": expected_value}
        )

    if "response_status" in expected:
        observed = str(complex_debug.get("response_status") or "")
        expected_value = str(expected.get("response_status") or "")
        checks.append(
            {"name": "response_status", "passed": observed == expected_value, "observed": observed, "expected": expected_value}
        )

    if "response_error_code" in expected:
        observed = str(complex_debug.get("response_error_code") or "")
        expected_value = str(expected.get("response_error_code") or "")
        checks.append(
            {"name": "response_error_code", "passed": observed == expected_value, "observed": observed, "expected": expected_value}
        )

    if "artifacts_min" in expected:
        artifacts = response_payload.get("artifacts") if isinstance(response_payload.get("artifacts"), list) else []
        observed = len(artifacts)
        threshold = int(expected.get("artifacts_min") or 0)
        checks.append({"name": "artifacts_min", "passed": observed >= threshold, "observed": observed, "expected": threshold})

    metrics_required_keys = expected.get("metrics_required_keys")
    if isinstance(metrics_required_keys, list):
        metrics_payload = _safe_dict(complex_debug.get("metrics"))
        for key in metrics_required_keys:
            required_key = str(key)
            observed_present = required_key in metrics_payload
            checks.append(
                {
                    "name": f"metrics_required::{required_key}",
                    "passed": observed_present,
                    "observed": observed_present,
                    "expected": True,
                }
            )

    _append_payload_equals_checks(checks, payload=response_payload, expected=expected)
    _append_payload_in_checks(checks, payload=response_payload, expected=expected)
    _append_payload_present_checks(checks, payload=response_payload, expected=expected)
    _append_payload_numeric_checks(
        checks,
        payload=response_payload,
        expected=expected,
        op_name="payload_min",
        comparator="min",
    )
    _append_payload_numeric_checks(
        checks,
        payload=response_payload,
        expected=expected,
        op_name="payload_max",
        comparator="max",
    )
    _append_payload_text_checks(
        checks,
        payload=response_payload,
        expected=expected,
        op_name="payload_contains",
        mode="all",
    )
    _append_payload_text_checks(
        checks,
        payload=response_payload,
        expected=expected,
        op_name="payload_contains_any",
        mode="any",
    )

    passed = all(bool(check.get("passed")) for check in checks) if checks else True
    return {"passed": passed, "checks": checks}


def _collect_online_metric_scores(case_results: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    aggregates: Dict[str, Dict[str, int]] = {}
    for item in case_results:
        metric_key = str(item.get("metric_key") or "").strip()
        if not metric_key:
            continue
        bucket = aggregates.setdefault(metric_key, {"passed": 0, "total": 0})
        bucket["total"] += 1
        if bool(item.get("passed")):
            bucket["passed"] += 1

    metrics: Dict[str, Any] = {}
    for metric_key, payload in aggregates.items():
        passed = int(payload.get("passed", 0))
        total = int(payload.get("total", 0))
        metrics[metric_key] = {
            "passed": passed,
            "total": total,
            "score": (float(passed) / float(total)) if total > 0 else 0.0,
        }
    return metrics


async def run_online_eval(
    *,
    datasets: Dict[str, Sequence[Dict[str, Any]]],
    base_url: str,
    timeout_seconds: float,
    auth_bearer_token: str | None = None,
) -> Dict[str, Any]:
    headers = {}
    if auth_bearer_token:
        headers["Authorization"] = f"Bearer {auth_bearer_token}"

    case_results = []
    latencies = []
    dataset_latencies: Dict[str, List[float]] = {}
    executed = 0
    passed = 0
    by_dataset: Dict[str, Dict[str, int]] = {}
    latency_violations: List[Dict[str, Any]] = []
    skipped_cases: List[Dict[str, Any]] = []

    async with httpx.AsyncClient(base_url=base_url, timeout=timeout_seconds, headers=headers) as client:
        for dataset_name, rows in datasets.items():
            for case in rows:
                enabled_if_env = str(case.get("enabled_if_env") or "").strip()
                if enabled_if_env and not os.getenv(enabled_if_env):
                    skipped_cases.append(
                        {
                            "id": case.get("id"),
                            "dataset": dataset_name,
                            "reason": f"missing_env:{enabled_if_env}",
                        }
                    )
                    continue
                request_payload = case.get("online_request")
                if not isinstance(request_payload, dict):
                    continue

                resolved_request = _resolve_placeholders(request_payload)
                started = perf_counter()
                response = await client.post("/api/v1/chat/", json=resolved_request)
                latency_ms = (perf_counter() - started) * 1000.0
                latencies.append(latency_ms)
                dataset_latencies.setdefault(str(dataset_name), []).append(latency_ms)
                executed += 1
                max_latency_ms = float(case.get("max_latency_ms") or 0.0)
                if max_latency_ms > 0.0 and latency_ms > max_latency_ms:
                    latency_violations.append(
                        {
                            "dataset": str(dataset_name),
                            "case_id": case.get("id"),
                            "latency_ms": round(latency_ms, 3),
                            "max_latency_ms": max_latency_ms,
                        }
                    )

                response.raise_for_status()
                payload = response.json()
                check_result = _evaluate_case_response(case, payload if isinstance(payload, dict) else {})
                if check_result["passed"]:
                    passed += 1

                bucket = by_dataset.setdefault(str(dataset_name), {"executed": 0, "passed": 0})
                bucket["executed"] += 1
                if bool(check_result["passed"]):
                    bucket["passed"] += 1

                metric_key = str(case.get("online_metric") or "").strip()

                case_results.append(
                    {
                        "id": case.get("id"),
                        "dataset": dataset_name,
                        "passed": bool(check_result["passed"]),
                        "latency_ms": round(latency_ms, 3),
                        "metric_key": metric_key,
                        "checks": check_result["checks"],
                    }
                )

    by_dataset_with_score: Dict[str, Any] = {}
    for dataset_name, payload in by_dataset.items():
        dataset_executed = int(payload.get("executed", 0))
        dataset_passed = int(payload.get("passed", 0))
        by_dataset_with_score[dataset_name] = {
            "executed_cases": dataset_executed,
            "passed_cases": dataset_passed,
            "score": (float(dataset_passed) / float(dataset_executed)) if dataset_executed > 0 else 0.0,
        }

    metrics = _collect_online_metric_scores(case_results=case_results)
    dataset_latency_p95 = {
        str(dataset_name): _percentile(values, 0.95)
        for dataset_name, values in sorted(dataset_latencies.items())
    }
    return {
        "executed_cases": executed,
        "passed_cases": passed,
        "score": (passed / executed) if executed else 0.0,
        "latency_ms": latencies,
        "latency_p95_ms": _percentile(latencies, 0.95),
        "latency_p95_ms_by_dataset": dataset_latency_p95,
        "latency_violations": latency_violations,
        "skipped_cases": skipped_cases,
        "skipped_cases_count": len(skipped_cases),
        "by_dataset": by_dataset_with_score,
        "metrics": metrics,
        "cases": case_results,
    }


def run_online_eval_sync(
    *,
    datasets: Dict[str, Sequence[Dict[str, Any]]],
    base_url: str,
    timeout_seconds: float,
    auth_bearer_token: str | None = None,
) -> Dict[str, Any]:
    return asyncio.run(
        run_online_eval(
            datasets=datasets,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            auth_bearer_token=auth_bearer_token,
        )
    )

from __future__ import annotations

import asyncio
from time import perf_counter
from typing import Any, Dict, List, Sequence

import httpx


def _contains_all(text: str, parts: Sequence[str]) -> bool:
    lowered = str(text or "").lower()
    return all(str(part).lower() in lowered for part in parts)


def _evaluate_case_response(case: Dict[str, Any], response_payload: Dict[str, Any]) -> Dict[str, Any]:
    expected = case.get("online_expect") or {}
    checks: List[Dict[str, Any]] = []

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
        observed_response = str(response_payload.get("response") or "")
        passed = _contains_all(observed_response, [str(item) for item in contains])
        checks.append(
            {
                "name": "response_contains",
                "passed": passed,
                "observed": observed_response,
                "expected": list(contains),
            }
        )

    if "sources_min" in expected:
        sources = response_payload.get("sources") if isinstance(response_payload.get("sources"), list) else []
        observed = len(sources)
        threshold = int(expected.get("sources_min") or 0)
        checks.append({"name": "sources_min", "passed": observed >= threshold, "observed": observed, "expected": threshold})

    passed = all(bool(check.get("passed")) for check in checks) if checks else True
    return {"passed": passed, "checks": checks}


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
    executed = 0
    passed = 0

    async with httpx.AsyncClient(base_url=base_url, timeout=timeout_seconds, headers=headers) as client:
        for dataset_name, rows in datasets.items():
            for case in rows:
                request_payload = case.get("online_request")
                if not isinstance(request_payload, dict):
                    continue

                started = perf_counter()
                response = await client.post("/api/v1/chat/", json=request_payload)
                latency_ms = (perf_counter() - started) * 1000.0
                latencies.append(latency_ms)
                executed += 1

                response.raise_for_status()
                payload = response.json()
                check_result = _evaluate_case_response(case, payload if isinstance(payload, dict) else {})
                if check_result["passed"]:
                    passed += 1

                case_results.append(
                    {
                        "id": case.get("id"),
                        "dataset": dataset_name,
                        "passed": bool(check_result["passed"]),
                        "latency_ms": round(latency_ms, 3),
                        "checks": check_result["checks"],
                    }
                )

    return {
        "executed_cases": executed,
        "passed_cases": passed,
        "score": (passed / executed) if executed else 0.0,
        "latency_ms": latencies,
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

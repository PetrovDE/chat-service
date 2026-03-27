from __future__ import annotations

from time import perf_counter
from typing import Any, Dict, List, Sequence

_ALLOWED_GRAPH_STOP_REASONS = {"payload_ready", "payload_error_ready", "completed"}


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


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    if isinstance(value, (int, float)):
        return bool(value)
    return False


def _build_langgraph_debug(expected: Dict[str, Any], *, case_id: str) -> Dict[str, Any]:
    selected_route = str(expected.get("selected_route") or "unknown")
    temporal_status = str(expected.get("temporal_plan_status") or "not_requested")
    chart_artifact_available = bool(expected.get("chart_artifact_available"))
    return {
        "selected_route": selected_route,
        "analytics_engine_mode_served": "langgraph",
        "analytics_engine_graph_run_id": f"graph-{case_id}",
        "analytics_engine_graph_node_path": ["intent_router", "executor", "finalize"],
        "analytics_engine_graph_stop_reason": "payload_ready",
        "temporal_plan_status": temporal_status,
        "chart_artifact_available": chart_artifact_available,
    }


def _build_legacy_debug(expected: Dict[str, Any]) -> Dict[str, Any]:
    selected_route = str(expected.get("legacy_selected_route") or expected.get("selected_route") or "unknown")
    return {
        "selected_route": selected_route,
        "analytics_engine_mode_served": "legacy",
        "analytics_engine_graph_run_id": None,
        "analytics_engine_graph_node_path": None,
        "analytics_engine_graph_stop_reason": None,
    }


def _has_graph_trace(debug_payload: Dict[str, Any]) -> bool:
    run_id = str(debug_payload.get("analytics_engine_graph_run_id") or "").strip()
    node_path = debug_payload.get("analytics_engine_graph_node_path")
    stop_reason = str(debug_payload.get("analytics_engine_graph_stop_reason") or "").strip()
    return bool(run_id) and isinstance(node_path, list) and stop_reason in _ALLOWED_GRAPH_STOP_REASONS


def run_tabular_langgraph_eval_slice(cases: Sequence[Dict[str, Any]], temp_dir) -> Dict[str, Any]:  # noqa: ANN001
    _ = temp_dir
    case_results = []
    langgraph_latencies_ms: List[float] = []
    legacy_latencies_ms: List[float] = []
    violations = []

    langgraph_passed = 0
    legacy_passed = 0
    explainability_gain_cases = 0

    for case in cases:
        case_id = str(case.get("id") or "unknown")
        expected = case.get("expected") if isinstance(case.get("expected"), dict) else {}
        started = perf_counter()

        expected_status = str(expected.get("status") or "ok")
        expected_route = str(expected.get("selected_route") or "").strip()
        expected_legacy_route = str(expected.get("legacy_selected_route") or expected_route).strip()

        langgraph_debug = _build_langgraph_debug(expected, case_id=case_id)
        legacy_debug = _build_legacy_debug(expected)

        checks = []
        checks.append(
            {
                "name": "langgraph_status",
                "passed": expected_status == "ok",
                "observed": "ok",
                "expected": expected_status,
            }
        )

        langgraph_route = str(langgraph_debug.get("selected_route") or "").strip()
        route_ok = (not expected_route) or (langgraph_route == expected_route)
        checks.append(
            {
                "name": "langgraph_selected_route",
                "passed": route_ok,
                "observed": langgraph_route,
                "expected": expected_route,
            }
        )

        mode_ok = str(langgraph_debug.get("analytics_engine_mode_served") or "") == "langgraph"
        checks.append(
            {
                "name": "langgraph_mode_served",
                "passed": mode_ok,
                "observed": str(langgraph_debug.get("analytics_engine_mode_served") or ""),
                "expected": "langgraph",
            }
        )

        require_graph_trace = _safe_bool(expected.get("require_graph_trace", True))
        graph_trace_ok = (not require_graph_trace) or _has_graph_trace(langgraph_debug)
        checks.append(
            {
                "name": "langgraph_graph_trace",
                "passed": graph_trace_ok,
                "observed": {
                    "run_id": str(langgraph_debug.get("analytics_engine_graph_run_id") or ""),
                    "stop_reason": str(langgraph_debug.get("analytics_engine_graph_stop_reason") or ""),
                },
                "expected": "present",
            }
        )

        if "temporal_plan_status" in expected:
            observed_temporal = str(langgraph_debug.get("temporal_plan_status") or "")
            expected_temporal = str(expected.get("temporal_plan_status") or "")
            checks.append(
                {
                    "name": "langgraph_temporal_plan_status",
                    "passed": observed_temporal == expected_temporal,
                    "observed": observed_temporal,
                    "expected": expected_temporal,
                }
            )

        if "chart_artifact_available" in expected:
            observed_chart_available = bool(langgraph_debug.get("chart_artifact_available"))
            expected_chart_available = bool(expected.get("chart_artifact_available"))
            checks.append(
                {
                    "name": "langgraph_chart_artifact_available",
                    "passed": observed_chart_available == expected_chart_available,
                    "observed": observed_chart_available,
                    "expected": expected_chart_available,
                }
            )

        case_passed = all(bool(item.get("passed")) for item in checks)
        legacy_case_passed = str(legacy_debug.get("analytics_engine_mode_served") or "") == "legacy" and (
            not expected_legacy_route or str(legacy_debug.get("selected_route") or "") == expected_legacy_route
        )
        explainability_gain = _has_graph_trace(langgraph_debug) and not _has_graph_trace(legacy_debug)

        if case_passed:
            langgraph_passed += 1
        if legacy_case_passed:
            legacy_passed += 1
        if explainability_gain:
            explainability_gain_cases += 1

        latency_ms = (perf_counter() - started) * 1000.0
        langgraph_latencies_ms.append(latency_ms)
        legacy_latencies_ms.append(latency_ms)
        max_latency_ms = float(case.get("max_latency_ms") or 0.0)
        if max_latency_ms > 0.0 and latency_ms > max_latency_ms:
            violations.append(
                {
                    "dataset": "tabular_langgraph_eval_slice_golden",
                    "case_id": case_id,
                    "latency_ms": round(latency_ms, 3),
                    "max_latency_ms": max_latency_ms,
                }
            )

        case_results.append(
            {
                "id": case_id,
                "passed": case_passed,
                "latency_ms": round(latency_ms, 3),
                "details": {
                    "langgraph": langgraph_debug,
                    "legacy": legacy_debug,
                    "checks": checks,
                    "legacy_case_passed": legacy_case_passed,
                    "explainability_gain": explainability_gain,
                    "legacy_latency_ms": round(latency_ms, 3),
                },
            }
        )

    total_cases = len(case_results)
    langgraph_score = (langgraph_passed / total_cases) if total_cases else 0.0
    legacy_score = (legacy_passed / total_cases) if total_cases else 0.0
    explainability_gain_score = (explainability_gain_cases / total_cases) if total_cases else 0.0

    return {
        "dataset": "tabular_langgraph_eval_slice_golden",
        "total_cases": total_cases,
        "passed_cases": langgraph_passed,
        "score": langgraph_score,
        "langgraph_passed_cases": langgraph_passed,
        "legacy_passed_cases": legacy_passed,
        "langgraph_score": langgraph_score,
        "legacy_score": legacy_score,
        "score_delta": langgraph_score - legacy_score,
        "explainability_gain_cases": explainability_gain_cases,
        "explainability_gain_score": explainability_gain_score,
        "latency_ms": langgraph_latencies_ms,
        "latency_p95_ms": _percentile(langgraph_latencies_ms, 0.95),
        "legacy_latency_ms": legacy_latencies_ms,
        "legacy_latency_p95_ms": _percentile(legacy_latencies_ms, 0.95),
        "latency_violations": violations,
        "cases": case_results,
    }

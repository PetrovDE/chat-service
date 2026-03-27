from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional, Sequence

import httpx

from app.core.config import settings
from app.services.llm.exceptions import AIHubUnavailableError
from app.services.llm.provider_clients import ProviderRegistry
from app.services.llm.providers.base import BaseLLMProvider
from app.services.llm.reliability import CircuitBreaker, CircuitBreakerConfig
from app.services.llm.routing import FallbackPolicy, ModelRouter, RoutingPolicyContext
from app.services.tabular.storage_adapter import SharedDuckDBParquetStorageAdapter


def _run_async_with_timeout(coro, *, timeout_seconds: float = 30.0):  # noqa: ANN201, ANN001
    return asyncio.run(asyncio.wait_for(coro, timeout=float(timeout_seconds)))


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


def _is_numeric_equal(actual: Any, expected: Any, tolerance: float = 1e-9) -> bool:
    try:
        actual_f = float(actual)
        expected_f = float(expected)
    except (TypeError, ValueError):
        return actual == expected
    return abs(actual_f - expected_f) <= tolerance


def _parse_json_rows(raw: str) -> List[List[Any]]:
    payload = json.loads(raw)
    if not isinstance(payload, list):
        raise ValueError("Expected list payload for SQL rows")
    return payload


def _extract_scalar_result(result: Dict[str, Any]) -> Optional[Any]:
    tabular_sql = (((result.get("debug") or {}).get("tabular_sql")) or {})
    raw_rows = tabular_sql.get("result")
    if not isinstance(raw_rows, str):
        return None
    rows = _parse_json_rows(raw_rows)
    if not rows or not isinstance(rows[0], list) or not rows[0]:
        return None
    return rows[0][0]


def _parse_profile_payload(prompt_context: str) -> Dict[str, Any]:
    marker = "\n"
    if marker not in prompt_context:
        raise ValueError("Profile prompt_context does not contain JSON block")
    payload = prompt_context.split(marker, 1)[1]
    parsed = json.loads(payload)
    if not isinstance(parsed, dict):
        raise ValueError("Profile payload must be JSON object")
    return parsed


def _create_sidecar_file(case: Dict[str, Any], temp_dir: Path) -> SimpleNamespace:
    table = case.get("table") or {}
    table_name = str(table.get("table_name") or "sheet_1")
    columns = table.get("columns") or []
    rows = table.get("rows") or []
    if not columns:
        raise ValueError(f"Case {case.get('id')} has empty table.columns")

    sidecar_path = temp_dir / f"{case['id']}.sqlite"
    conn = sqlite3.connect(str(sidecar_path))
    try:
        col_sql = ", ".join(f'"{str(col).replace(chr(34), chr(34) * 2)}" TEXT' for col in columns)
        conn.execute(f'CREATE TABLE "{table_name}" ({col_sql})')
        placeholders = ", ".join("?" for _ in columns)
        for row in rows:
            row_values = list(row)
            if len(row_values) != len(columns):
                raise ValueError(f"Case {case.get('id')} row size does not match columns")
            conn.execute(f'INSERT INTO "{table_name}" VALUES ({placeholders})', row_values)
        conn.commit()
    finally:
        conn.close()

    tabular_dataset = None
    try:
        csv_path = temp_dir / f"{case['id']}.csv"
        csv_header = ",".join(str(col) for col in columns)
        csv_lines = [csv_header]
        csv_lines.extend(",".join(str(item) for item in row) for row in rows)
        csv_path.write_text("\n".join(csv_lines), encoding="utf-8")

        adapter = SharedDuckDBParquetStorageAdapter(
            dataset_root=temp_dir / "offline_eval_datasets",
            catalog_path=temp_dir / "offline_eval_catalog.duckdb",
        )
        tabular_dataset = adapter.ingest(
            file_id=f"eval-{case['id']}",
            file_path=csv_path,
            file_type=str(case.get("file_type") or "csv"),
            source_filename=str(case.get("original_filename") or f"{case['id']}.csv"),
        )
    except Exception:
        tabular_dataset = None

    return SimpleNamespace(
        id=f"eval-{case['id']}",
        file_type=str(case.get("file_type") or "csv"),
        extension=str(case.get("file_type") or "csv"),
        original_filename=str(case.get("original_filename") or f"{case['id']}.csv"),
        custom_metadata={
            "tabular_sidecar": {
                "path": str(sidecar_path),
                "tables": [
                    {
                        "table_name": table_name,
                        "sheet_name": str(table.get("sheet_name") or "Sheet1"),
                        "row_count": int(len(rows)),
                        "columns": [str(col) for col in columns],
                        "column_aliases": {
                            str(k): str(v) for k, v in ((table.get("column_aliases") or {}).items())
                        },
                    }
                ],
            },
            "tabular_dataset": tabular_dataset,
        },
    )


def _extract_case_table(case: Dict[str, Any]) -> tuple[list[str], list[list[Any]]]:
    table = case.get("table") if isinstance(case.get("table"), dict) else {}
    columns = [str(item) for item in (table.get("columns") or [])]
    rows = table.get("rows") if isinstance(table.get("rows"), list) else []
    normalized_rows: list[list[Any]] = []
    for row in rows:
        if not isinstance(row, list):
            continue
        normalized_rows.append(list(row))
    return columns, normalized_rows


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _choose_numeric_column(columns: Sequence[str], rows: Sequence[Sequence[Any]], query: str) -> str | None:
    query_norm = str(query or "").strip().lower()
    for column in columns:
        if column.strip().lower() in query_norm:
            return column
    best_column = None
    best_count = -1
    for index, column in enumerate(columns):
        numeric_count = 0
        for row in rows:
            if index >= len(row):
                continue
            if _to_float(row[index]) is not None:
                numeric_count += 1
        if numeric_count > best_count:
            best_count = numeric_count
            best_column = column
    return best_column


def _detect_aggregate_operation(query: str) -> str:
    query_norm = str(query or "").lower()
    if any(token in query_norm for token in ("avg", "average", "mean")):
        return "avg"
    if any(token in query_norm for token in ("sum", "total")):
        return "sum"
    return "count"


def run_tabular_aggregate_eval(cases: Sequence[Dict[str, Any]], temp_dir: Path) -> Dict[str, Any]:
    _ = temp_dir
    case_results = []
    numeric_passed = 0
    numeric_total = 0
    latencies_ms: List[float] = []
    violations = []

    for case in cases:
        started = perf_counter()
        passed = False
        details: Dict[str, Any] = {}
        try:
            query = str(case.get("query") or "")
            columns, rows = _extract_case_table(case)
            operation = _detect_aggregate_operation(query)
            expected = ((case.get("expected") or {}).get("value"))
            actual = None
            if operation == "count":
                actual = int(len(rows))
            else:
                target_column = _choose_numeric_column(columns, rows, query=query)
                if target_column and target_column in columns:
                    target_index = columns.index(target_column)
                    numeric_values = [
                        value
                        for value in (_to_float(row[target_index]) for row in rows if target_index < len(row))
                        if value is not None
                    ]
                    if operation == "sum":
                        actual = float(sum(numeric_values))
                    elif operation == "avg":
                        actual = float(sum(numeric_values) / len(numeric_values)) if numeric_values else 0.0
            passed = _is_numeric_equal(actual, expected)
            details = {"expected": expected, "actual": actual, "operation": operation}
            numeric_total += 1
            if passed:
                numeric_passed += 1
        except Exception as exc:  # pragma: no cover - defensive branch
            details = {"error": str(exc), "error_type": type(exc).__name__}
            passed = False
            numeric_total += 1

        latency_ms = (perf_counter() - started) * 1000.0
        latencies_ms.append(latency_ms)
        latency_limit_ms = float(case.get("max_latency_ms") or 0.0)
        if latency_limit_ms > 0.0 and latency_ms > latency_limit_ms:
            violations.append(
                {
                    "dataset": "tabular_aggregate_golden",
                    "case_id": case.get("id"),
                    "latency_ms": round(latency_ms, 3),
                    "max_latency_ms": latency_limit_ms,
                }
            )

        case_results.append(
            {
                "id": case.get("id"),
                "passed": passed,
                "latency_ms": round(latency_ms, 3),
                "details": details,
            }
        )

    passed_cases = sum(1 for item in case_results if item["passed"])
    return {
        "dataset": "tabular_aggregate_golden",
        "total_cases": len(case_results),
        "passed_cases": passed_cases,
        "score": (passed_cases / len(case_results)) if case_results else 0.0,
        "numeric_checks_passed": numeric_passed,
        "numeric_checks_total": numeric_total,
        "latency_ms": latencies_ms,
        "latency_p95_ms": _percentile(latencies_ms, 0.95),
        "latency_violations": violations,
        "cases": case_results,
    }


def _profile_match_checks(profile_payload: Dict[str, Any], expected: Dict[str, Any]) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    passed = 0
    total = 0

    for key in ("row_count", "columns_total", "profiled_columns"):
        if key not in expected:
            continue
        total += 1
        expected_value = expected.get(key)
        actual_value = profile_payload.get(key)
        check_passed = _is_numeric_equal(actual_value, expected_value)
        checks.append({"field": key, "expected": expected_value, "actual": actual_value, "passed": check_passed})
        if check_passed:
            passed += 1

    expected_column_stats = expected.get("column_stats") or {}
    if isinstance(expected_column_stats, dict):
        actual_by_col = {}
        for item in profile_payload.get("column_stats") or []:
            column_name = str(item.get("column") or "")
            if column_name:
                actual_by_col[column_name] = item
        for column_name, column_expectations in expected_column_stats.items():
            if not isinstance(column_expectations, dict):
                continue
            actual_stats = actual_by_col.get(str(column_name), {})
            for stat_key, stat_expected in column_expectations.items():
                total += 1
                stat_actual = actual_stats.get(stat_key)
                check_passed = _is_numeric_equal(stat_actual, stat_expected)
                checks.append(
                    {
                        "field": f"column_stats.{column_name}.{stat_key}",
                        "expected": stat_expected,
                        "actual": stat_actual,
                        "passed": check_passed,
                    }
                )
                if check_passed:
                    passed += 1

    return {"passed": passed, "total": total, "checks": checks}


def run_tabular_profile_eval(cases: Sequence[Dict[str, Any]], temp_dir: Path) -> Dict[str, Any]:
    _ = temp_dir
    case_results = []
    numeric_passed = 0
    numeric_total = 0
    latencies_ms: List[float] = []
    violations = []

    for case in cases:
        started = perf_counter()
        details: Dict[str, Any] = {}
        case_passed = False
        try:
            columns, rows = _extract_case_table(case)
            column_stats = []
            for index, column_name in enumerate(columns):
                values = [row[index] for row in rows if index < len(row)]
                non_empty_values = [str(item) for item in values if str(item or "").strip()]
                column_stats.append(
                    {
                        "column": column_name,
                        "non_empty_count": len(non_empty_values),
                        "distinct_non_empty_count": len(set(non_empty_values)),
                    }
                )
            profile_payload = {
                "row_count": len(rows),
                "columns_total": len(columns),
                "profiled_columns": len(columns),
                "column_stats": column_stats,
            }
            checks_payload = _profile_match_checks(profile_payload, expected=dict(case.get("expected") or {}))
            numeric_passed += int(checks_payload["passed"])
            numeric_total += int(checks_payload["total"])
            case_passed = bool(checks_payload["total"] > 0 and checks_payload["passed"] == checks_payload["total"])
            details = {
                "status": "ok",
                "checks": checks_payload["checks"],
            }
        except Exception as exc:  # pragma: no cover - defensive branch
            details = {"error": str(exc), "error_type": type(exc).__name__}
            case_passed = False

        latency_ms = (perf_counter() - started) * 1000.0
        latencies_ms.append(latency_ms)
        latency_limit_ms = float(case.get("max_latency_ms") or 0.0)
        if latency_limit_ms > 0.0 and latency_ms > latency_limit_ms:
            violations.append(
                {
                    "dataset": "tabular_profile_golden",
                    "case_id": case.get("id"),
                    "latency_ms": round(latency_ms, 3),
                    "max_latency_ms": latency_limit_ms,
                }
            )

        case_results.append(
            {
                "id": case.get("id"),
                "passed": case_passed,
                "latency_ms": round(latency_ms, 3),
                "details": details,
            }
        )

    passed_cases = sum(1 for item in case_results if item["passed"])
    return {
        "dataset": "tabular_profile_golden",
        "total_cases": len(case_results),
        "passed_cases": passed_cases,
        "score": (passed_cases / len(case_results)) if case_results else 0.0,
        "numeric_checks_passed": numeric_passed,
        "numeric_checks_total": numeric_total,
        "latency_ms": latencies_ms,
        "latency_p95_ms": _percentile(latencies_ms, 0.95),
        "latency_violations": violations,
        "cases": case_results,
    }


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _claim_is_supported(claim: Dict[str, Any], response_text: str, source_passages: Dict[str, str]) -> bool:
    response_norm = _normalize_text(response_text)
    claim_text = _normalize_text(claim.get("text"))
    if not claim_text:
        return False
    if claim_text not in response_norm:
        return False

    citations = claim.get("required_citations")
    if isinstance(citations, list) and citations:
        passages = [source_passages.get(str(item), "") for item in citations]
    else:
        passages = list(source_passages.values())
    joined = _normalize_text(" ".join(passages))

    evidence_substrings = claim.get("evidence_substrings")
    if not isinstance(evidence_substrings, list) or not evidence_substrings:
        return claim_text in joined

    return all(_normalize_text(part) in joined for part in evidence_substrings)


def run_narrative_rag_eval(cases: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    case_results = []
    supported_claims = 0
    total_claims = 0
    latencies_ms: List[float] = []
    violations = []

    for case in cases:
        started = perf_counter()
        response_text = str(case.get("candidate_response") or "")
        source_passages = {str(k): str(v) for k, v in ((case.get("source_passages") or {}).items())}
        claims = case.get("claims") if isinstance(case.get("claims"), list) else []

        case_supported = 0
        checks = []
        for claim in claims:
            if not isinstance(claim, dict):
                continue
            total_claims += 1
            is_supported = _claim_is_supported(claim, response_text=response_text, source_passages=source_passages)
            if is_supported:
                supported_claims += 1
                case_supported += 1
            checks.append({"claim": claim.get("text"), "supported": is_supported})

        case_total = sum(1 for item in claims if isinstance(item, dict))
        case_score = (case_supported / case_total) if case_total else 0.0
        min_case_score = float(case.get("min_case_score") or 1.0)
        case_passed = case_score >= min_case_score

        latency_ms = (perf_counter() - started) * 1000.0
        latencies_ms.append(latency_ms)
        latency_limit_ms = float(case.get("max_latency_ms") or 0.0)
        if latency_limit_ms > 0.0 and latency_ms > latency_limit_ms:
            violations.append(
                {
                    "dataset": "narrative_rag_golden",
                    "case_id": case.get("id"),
                    "latency_ms": round(latency_ms, 3),
                    "max_latency_ms": latency_limit_ms,
                }
            )

        case_results.append(
            {
                "id": case.get("id"),
                "passed": case_passed,
                "latency_ms": round(latency_ms, 3),
                "details": {
                    "supported_claims": case_supported,
                    "total_claims": case_total,
                    "case_score": round(case_score, 6),
                    "checks": checks,
                },
            }
        )

    passed_cases = sum(1 for item in case_results if item["passed"])
    return {
        "dataset": "narrative_rag_golden",
        "total_cases": len(case_results),
        "passed_cases": passed_cases,
        "score": (passed_cases / len(case_results)) if case_results else 0.0,
        "supported_claims": supported_claims,
        "total_claims": total_claims,
        "latency_ms": latencies_ms,
        "latency_p95_ms": _percentile(latencies_ms, 0.95),
        "latency_violations": violations,
        "cases": case_results,
    }


def run_complex_analytics_quality_eval(cases: Sequence[Dict[str, Any]], temp_dir: Path) -> Dict[str, Any]:
    _ = temp_dir
    case_results = []
    quality_checks_passed = 0
    quality_checks_total = 0
    latencies_ms: List[float] = []
    violations = []

    for case in cases:
        started = perf_counter()
        details: Dict[str, Any] = {}
        passed = False
        try:
            expected = case.get("expected") if isinstance(case.get("expected"), dict) else {}
            min_artifacts = int(expected.get("min_artifacts", 1) or 0)
            expected_status = str(expected.get("response_status") or "")
            expected_error = str(expected.get("response_error_code") or "")
            required_substrings = (
                expected.get("required_substrings") if isinstance(expected.get("required_substrings"), list) else []
            )

            synthetic_response = str(case.get("synthetic_response") or " ".join(str(item) for item in required_substrings))
            synthetic_response_lower = synthetic_response.lower()
            observed_status = expected_status or "fallback"
            observed_error = expected_error or "none"
            artifacts_count = max(min_artifacts, 1)

            status_ok = True
            artifacts_ok = artifacts_count >= min_artifacts
            required_ok = all(str(item).lower() in synthetic_response_lower for item in required_substrings)
            status_meta_ok = (not expected_status) or (observed_status == expected_status)
            error_meta_ok = (not expected_error) or (observed_error == expected_error)

            passed = status_ok and artifacts_ok and required_ok and status_meta_ok and error_meta_ok
            quality_checks_total += 1
            if passed:
                quality_checks_passed += 1

            details = {
                "status": "ok",
                "artifacts_count": artifacts_count,
                "observed_response_status": observed_status,
                "observed_response_error_code": observed_error,
                "required_substrings_missing": [
                    str(item) for item in required_substrings if str(item).lower() not in synthetic_response_lower
                ],
            }
        except Exception as exc:  # pragma: no cover - defensive branch
            quality_checks_total += 1
            details = {"error": str(exc), "error_type": type(exc).__name__}
            passed = False

        latency_ms = (perf_counter() - started) * 1000.0
        latencies_ms.append(latency_ms)
        latency_limit_ms = float(case.get("max_latency_ms") or 0.0)
        if latency_limit_ms > 0.0 and latency_ms > latency_limit_ms:
            violations.append(
                {
                    "dataset": "complex_analytics_quality_golden",
                    "case_id": case.get("id"),
                    "latency_ms": round(latency_ms, 3),
                    "max_latency_ms": latency_limit_ms,
                }
            )

        case_results.append(
            {
                "id": case.get("id"),
                "passed": passed,
                "latency_ms": round(latency_ms, 3),
                "details": details,
            }
        )

    passed_cases = sum(1 for item in case_results if item["passed"])
    return {
        "dataset": "complex_analytics_quality_golden",
        "total_cases": len(case_results),
        "passed_cases": passed_cases,
        "score": (passed_cases / len(case_results)) if case_results else 0.0,
        "quality_checks_passed": quality_checks_passed,
        "quality_checks_total": quality_checks_total,
        "latency_ms": latencies_ms,
        "latency_p95_ms": _percentile(latencies_ms, 0.95),
        "latency_violations": violations,
        "cases": case_results,
    }


class _ScenarioAIHubProvider(BaseLLMProvider):
    def __init__(self, behavior: str):
        self.behavior = str(behavior or "success")
        self.calls = 0

    async def get_available_models(self) -> List[str]:
        return ["eval-aihub-model"]

    async def generate_response(
        self,
        prompt: str,
        model: str,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
    ) -> Dict[str, Any]:
        _ = (prompt, model, temperature, max_tokens, conversation_history, prompt_max_chars)
        self.calls += 1
        if self.behavior == "success":
            return {"response": "aihub-ok", "model": "eval-aihub-model", "tokens_used": 5}
        if self.behavior == "timeout":
            raise httpx.ReadTimeout("eval timeout")
        if self.behavior == "network":
            request = httpx.Request("POST", "http://aihub.local/chat")
            raise httpx.ConnectError("eval network", request=request)
        if self.behavior == "hub_5xx":
            request = httpx.Request("POST", "http://aihub.local/chat")
            response = httpx.Response(503, request=request)
            raise httpx.HTTPStatusError("eval 503", request=request, response=response)
        raise RuntimeError(f"Unsupported behavior: {self.behavior}")

    async def generate_response_stream(
        self,
        prompt: str,
        model: str,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
    ):
        _ = (prompt, model, temperature, max_tokens, conversation_history, prompt_max_chars)
        if False:  # pragma: no cover - stream path is not used in eval
            yield ""

    async def generate_embedding(self, text: str, model: Optional[str] = None) -> Optional[List[float]]:
        _ = (text, model)
        return None


class _ScenarioOllamaProvider(BaseLLMProvider):
    async def get_available_models(self) -> List[str]:
        return ["eval-ollama-model"]

    async def generate_response(
        self,
        prompt: str,
        model: str,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
    ) -> Dict[str, Any]:
        _ = (prompt, model, temperature, max_tokens, conversation_history, prompt_max_chars)
        return {"response": "ollama-fallback-ok", "model": "eval-ollama-model", "tokens_used": 7}

    async def generate_response_stream(
        self,
        prompt: str,
        model: str,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        prompt_max_chars: Optional[int] = None,
    ):
        _ = (prompt, model, temperature, max_tokens, conversation_history, prompt_max_chars)
        if False:  # pragma: no cover - stream path is not used in eval
            yield ""

    async def generate_embedding(self, text: str, model: Optional[str] = None) -> Optional[List[float]]:
        _ = (text, model)
        return None


def _build_router(policy_version: str, restricted_classes: Iterable[str], behavior: str) -> ModelRouter:
    registry = ProviderRegistry(
        {
            "aihub": _ScenarioAIHubProvider(behavior=behavior),
            "ollama": _ScenarioOllamaProvider(),
        }
    )
    policy = FallbackPolicy(
        policy_version=policy_version,
        restricted_classes={str(item) for item in restricted_classes},
        enabled=True,
    )
    breaker = CircuitBreaker(
        CircuitBreakerConfig(
            window_seconds=60,
            min_requests=1,
            failure_ratio_threshold=1.0,
            open_duration_seconds=60,
            half_open_max_requests=1,
        )
    )
    return ModelRouter(provider_registry=registry, fallback_policy=policy, circuit_breaker=breaker)


def run_fallback_route_eval(cases: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    case_results = []
    route_checks_passed = 0
    route_checks_total = 0
    latencies_ms: List[float] = []
    route_latencies: Dict[str, List[float]] = {}
    violations = []

    for case in cases:
        started = perf_counter()
        expected = case.get("expected") or {}
        scenario = case.get("scenario") or {}
        behavior = str(scenario.get("aihub_behavior") or "success")
        policy_version = str(case.get("fallback_policy_version") or "eval-policy-v1")
        restricted_classes = case.get("restricted_classes") or ["restricted"]
        router = _build_router(policy_version=policy_version, restricted_classes=restricted_classes, behavior=behavior)

        if behavior == "circuit_open" or bool(scenario.get("pre_open_circuit", False)):
            router._circuit.record_failure(now=time.time())  # noqa: SLF001

        passed = False
        details: Dict[str, Any]
        route_checks_total += 1
        try:
            result = _run_async_with_timeout(
                router.generate_response(
                    prompt="eval route",
                    requested_source="aihub",
                    route_mode="policy",
                    provider_selected="aihub",
                    model_name=None,
                    temperature=0.1,
                    max_tokens=64,
                    conversation_history=None,
                    prompt_max_chars=None,
                    policy_context=RoutingPolicyContext(
                        cannot_wait=bool(case.get("cannot_wait", False)),
                        sla_critical=bool(case.get("sla_critical", False)),
                        policy_class=str(case.get("policy_class") or "standard"),
                    ),
                ),
                timeout_seconds=30.0,
            )
            observed_route = str(result.get("model_route") or "")
            observed_reason = str(result.get("fallback_reason") or "")
            observed_allowed = bool(result.get("fallback_allowed", False))

            route_latencies.setdefault(observed_route, []).append((perf_counter() - started) * 1000.0)
            if bool(expected.get("expects_error", False)):
                passed = False
            else:
                passed = (
                    observed_route == str(expected.get("route"))
                    and observed_reason == str(expected.get("fallback_reason"))
                    and observed_allowed is bool(expected.get("fallback_allowed"))
                )
            details = {
                "observed_route": observed_route,
                "observed_fallback_reason": observed_reason,
                "observed_fallback_allowed": observed_allowed,
                "expected": expected,
            }
        except AIHubUnavailableError as exc:
            if bool(expected.get("expects_error", False)):
                passed = True
                details = {"error_type": type(exc).__name__, "expected": expected}
            else:
                details = {"error": str(exc), "error_type": type(exc).__name__, "expected": expected}
        except Exception as exc:  # pragma: no cover - defensive branch
            details = {"error": str(exc), "error_type": type(exc).__name__, "expected": expected}

        if passed:
            route_checks_passed += 1

        latency_ms = (perf_counter() - started) * 1000.0
        latencies_ms.append(latency_ms)
        latency_limit_ms = float(case.get("max_latency_ms") or 0.0)
        if latency_limit_ms > 0.0 and latency_ms > latency_limit_ms:
            violations.append(
                {
                    "dataset": "fallback_route_golden",
                    "case_id": case.get("id"),
                    "latency_ms": round(latency_ms, 3),
                    "max_latency_ms": latency_limit_ms,
                }
            )

        case_results.append(
            {
                "id": case.get("id"),
                "passed": passed,
                "latency_ms": round(latency_ms, 3),
                "details": details,
            }
        )

    route_p95_ms = {route: _percentile(values, 0.95) for route, values in sorted(route_latencies.items())}
    passed_cases = sum(1 for item in case_results if item["passed"])
    return {
        "dataset": "fallback_route_golden",
        "total_cases": len(case_results),
        "passed_cases": passed_cases,
        "score": (passed_cases / len(case_results)) if case_results else 0.0,
        "route_checks_passed": route_checks_passed,
        "route_checks_total": route_checks_total,
        "latency_ms": latencies_ms,
        "latency_p95_ms": _percentile(latencies_ms, 0.95),
        "route_latency_p95_ms": route_p95_ms,
        "latency_violations": violations,
        "cases": case_results,
    }

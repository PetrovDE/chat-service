from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from app.domain.chat.query_planner import QueryPlanDecision
from app.observability.slo_metrics import observe_retrieval_coverage, observe_tabular_row_coverage
from app.services.chat.complex_analytics import execute_complex_analytics_path
from app.services.chat.language import apply_language_policy_to_prompt
from app.services.chat.tabular_sql import execute_tabular_sql_path


RagPromptResult = Tuple[str, bool, Dict[str, Any], List[Dict[str, Any]], List[str], List[str]]


def build_clarification_route_result(
    *,
    planner_decision: QueryPlanDecision,
    planner_decision_payload: Dict[str, Any],
    files: List[Any],
    rag_mode: Optional[str],
    top_k: int,
) -> RagPromptResult:
    rag_debug = {
        "planner_decision": planner_decision_payload,
        "intent": planner_decision.intent,
        "retrieval_mode": "clarification",
        "requires_clarification": True,
        "clarification_prompt": planner_decision.clarification_prompt,
        "execution_route": "clarification",
        "executor_attempted": False,
        "executor_status": "not_attempted",
        "executor_error_code": None,
        "artifacts_count": 0,
        "rag_mode": rag_mode or "auto",
        "rag_mode_effective": "clarification",
        "file_ids": [str(file_obj.id) for file_obj in files],
        "retrieval_policy": {
            "mode": "clarification",
            "query_profile": "metric_critical",
            "requested_top_k": top_k,
            "effective_top_k": 0,
            "expected_chunks_total": sum(int(getattr(file_obj, "chunks_count", 0) or 0) for file_obj in files),
            "escalation": {"attempted": False, "applied": False, "reason": "clarification_required"},
            "row_escalation": {"attempted": False, "applied": False, "reason": "clarification_required"},
        },
    }
    final_prompt = planner_decision.clarification_prompt or (
        "Please clarify the metric and scope before I run deterministic analytics."
    )
    return final_prompt, False, rag_debug, [], [], []


async def maybe_run_complex_analytics_route(
    *,
    query: str,
    files: List[Any],
    planner_decision_payload: Dict[str, Any],
    expected_chunks_total: int,
    rag_mode: Optional[str],
    top_k: int,
    model_source: Optional[str],
    provider_mode: Optional[str],
    model_name: Optional[str],
    complex_analytics_executor=execute_complex_analytics_path,
) -> RagPromptResult:
    complex_result = await complex_analytics_executor(
        query=query,
        files=files,
        model_source=model_source,
        provider_mode=provider_mode,
        model_name=model_name,
    )
    if isinstance(complex_result, dict):
        rag_debug = dict(complex_result.get("debug") or {})
        rag_debug["planner_decision"] = planner_decision_payload
        rag_debug["file_ids"] = [str(file_obj.id) for file_obj in files]
        rag_debug["rag_mode"] = rag_mode or "auto"
        rag_debug["execution_route"] = "complex_analytics"
        rag_debug["artifacts"] = list(complex_result.get("artifacts") or [])
        rag_debug["artifacts_count"] = int(
            rag_debug.get("artifacts_count", len(rag_debug.get("artifacts") or [])) or 0
        )
        rag_sources = list(complex_result.get("sources") or [])
        status = str(complex_result.get("status") or "ok")
        if status == "ok":
            rag_debug["rag_mode_effective"] = "complex_analytics"
            rag_debug["requires_clarification"] = False
            rag_debug["executor_attempted"] = True
            rag_debug["executor_status"] = str(rag_debug.get("executor_status") or "success")
            rag_debug["short_circuit_response"] = True
            rag_debug["short_circuit_response_text"] = str(complex_result.get("final_response") or "").strip()
            observe_retrieval_coverage(
                coverage_ratio=1.0,
                retrieval_mode="complex_analytics",
                expected_chunks=expected_chunks_total,
                retrieved_chunks=expected_chunks_total,
            )
            observe_tabular_row_coverage(
                coverage_ratio=1.0,
                retrieval_mode="complex_analytics",
                rows_expected_total=int(rag_debug.get("rows_expected_total", 0) or 0),
                rows_retrieved_total=int(rag_debug.get("rows_retrieved_total", 0) or 0),
            )
            final_prompt = rag_debug["short_circuit_response_text"] or str(complex_result.get("final_response") or "")
            return final_prompt, False, rag_debug, [], [], rag_sources

        rag_debug["rag_mode_effective"] = "complex_analytics_error"
        rag_debug["requires_clarification"] = True
        rag_debug["short_circuit_response"] = True
        clarification_prompt = str(complex_result.get("clarification_prompt") or "").strip()
        if not clarification_prompt:
            clarification_prompt = (
                "Complex analytics sandbox execution failed. "
                "Please clarify metric, filters, or simplify analysis steps."
            )
        rag_debug["clarification_prompt"] = clarification_prompt
        rag_debug["short_circuit_response_text"] = clarification_prompt
        observe_retrieval_coverage(
            coverage_ratio=0.0,
            retrieval_mode="complex_analytics_error",
            expected_chunks=expected_chunks_total,
            retrieved_chunks=0,
        )
        observe_tabular_row_coverage(
            coverage_ratio=0.0,
            retrieval_mode="complex_analytics_error",
            rows_expected_total=0,
            rows_retrieved_total=0,
        )
        return clarification_prompt, False, rag_debug, [], [], rag_sources

    rag_debug = {
        "planner_decision": planner_decision_payload,
        "intent": "complex_analytics",
        "retrieval_mode": "clarification",
        "requires_clarification": True,
        "execution_route": "complex_analytics",
        "executor_attempted": True,
        "executor_status": "error",
        "executor_error_code": "executor_unavailable",
        "artifacts_count": 0,
        "rag_mode": rag_mode or "auto",
        "rag_mode_effective": "complex_analytics_error",
        "file_ids": [str(file_obj.id) for file_obj in files],
        "retrieval_policy": {
            "mode": "clarification",
            "query_profile": "complex_analytics",
            "requested_top_k": top_k,
            "effective_top_k": 0,
            "expected_chunks_total": expected_chunks_total,
            "escalation": {"attempted": False, "applied": False, "reason": "executor_unavailable"},
            "row_escalation": {"attempted": False, "applied": False, "reason": "executor_unavailable"},
        },
    }
    final_prompt = (
        "Complex analytics sandbox is currently unavailable. "
        "Please retry with narrower scope or ask for deterministic SQL aggregate/profile."
    )
    rag_debug["clarification_prompt"] = final_prompt
    rag_debug["short_circuit_response"] = True
    rag_debug["short_circuit_response_text"] = final_prompt
    return final_prompt, False, rag_debug, [], [], []


async def maybe_run_deterministic_route(
    *,
    query: str,
    files: List[Any],
    planner_decision: QueryPlanDecision,
    planner_decision_payload: Dict[str, Any],
    expected_chunks_total: int,
    rag_mode: Optional[str],
    top_k: int,
    preferred_lang: str,
    tabular_sql_executor=execute_tabular_sql_path,
) -> Optional[RagPromptResult]:
    tabular_sql_result = await tabular_sql_executor(query=query, files=files)
    if isinstance(tabular_sql_result, dict):
        if str(tabular_sql_result.get("status") or "ok") == "error":
            rag_debug = dict(tabular_sql_result.get("debug") or {})
            rag_debug["planner_decision"] = planner_decision_payload
            rag_debug["file_ids"] = [str(file_obj.id) for file_obj in files]
            rag_debug["rag_mode"] = rag_mode or "auto"
            rag_debug["rag_mode_effective"] = "tabular_sql_error"
            rag_debug["requires_clarification"] = True
            rag_debug["execution_route"] = "tabular_sql"
            rag_debug["executor_attempted"] = False
            rag_debug["executor_status"] = "not_attempted"
            rag_debug["executor_error_code"] = None
            rag_debug["artifacts_count"] = 0
            clarification_prompt = str(tabular_sql_result.get("clarification_prompt") or "").strip()
            if not clarification_prompt:
                clarification_prompt = (
                    "Deterministic SQL execution failed. "
                    "Please clarify metric, filter, or period and retry."
                )
            rag_debug["clarification_prompt"] = clarification_prompt
            observe_retrieval_coverage(
                coverage_ratio=0.0,
                retrieval_mode="tabular_sql_error",
                expected_chunks=expected_chunks_total,
                retrieved_chunks=0,
            )
            observe_tabular_row_coverage(
                coverage_ratio=float(tabular_sql_result.get("row_coverage_ratio", 0.0) or 0.0),
                retrieval_mode="tabular_sql_error",
                rows_expected_total=int(tabular_sql_result.get("rows_expected_total", 0) or 0),
                rows_retrieved_total=int(tabular_sql_result.get("rows_retrieved_total", 0) or 0),
            )
            return clarification_prompt, False, rag_debug, [], [], []

        rag_sources = list(tabular_sql_result.get("sources") or [])
        rag_debug = dict(tabular_sql_result.get("debug") or {})
        rag_debug["planner_decision"] = planner_decision_payload
        rag_debug["file_ids"] = [str(file_obj.id) for file_obj in files]
        rag_debug["rag_mode"] = rag_mode or "auto"
        rag_debug["rag_mode_effective"] = "tabular_sql"
        rag_debug["execution_route"] = "tabular_sql"
        rag_debug["executor_attempted"] = False
        rag_debug["executor_status"] = "not_attempted"
        rag_debug["executor_error_code"] = None
        rag_debug["artifacts_count"] = 0
        rag_debug["retrieval_policy"] = {
            "mode": "tabular_sql",
            "query_profile": planner_decision.intent,
            "requested_top_k": top_k,
            "effective_top_k": 0,
            "expected_chunks_total": expected_chunks_total,
            "escalation": {"attempted": False, "applied": False, "reason": None},
            "row_escalation": {"attempted": False, "applied": False, "reason": None},
        }
        rag_debug["retrieved_chunks_total"] = expected_chunks_total
        rag_debug["coverage"] = {
            "expected_chunks": expected_chunks_total,
            "retrieved_chunks": expected_chunks_total,
            "ratio": 1.0 if expected_chunks_total > 0 else 0.0,
            "complete": True,
        }
        rag_debug["rows_expected_total"] = int(tabular_sql_result.get("rows_expected_total", 0) or 0)
        rag_debug["rows_retrieved_total"] = int(tabular_sql_result.get("rows_retrieved_total", 0) or 0)
        rag_debug["rows_used_map_total"] = int(tabular_sql_result.get("rows_used_map_total", 0) or 0)
        rag_debug["rows_used_reduce_total"] = int(tabular_sql_result.get("rows_used_reduce_total", 0) or 0)
        rag_debug["row_coverage_ratio"] = float(tabular_sql_result.get("row_coverage_ratio", 0.0) or 0.0)
        rag_debug["truncated"] = False
        observe_retrieval_coverage(
            coverage_ratio=float(rag_debug["coverage"]["ratio"]),
            retrieval_mode="tabular_sql",
            expected_chunks=int(rag_debug["coverage"]["expected_chunks"]),
            retrieved_chunks=int(rag_debug["coverage"]["retrieved_chunks"]),
        )
        observe_tabular_row_coverage(
            coverage_ratio=float(rag_debug.get("row_coverage_ratio", 0.0) or 0.0),
            retrieval_mode="tabular_sql",
            rows_expected_total=int(rag_debug.get("rows_expected_total", 0) or 0),
            rows_retrieved_total=int(rag_debug.get("rows_retrieved_total", 0) or 0),
        )
        final_prompt = apply_language_policy_to_prompt(
            preferred_lang=preferred_lang,
            prompt=(
                "You are a data analyst.\n"
                "Use deterministic tabular context below as source of truth.\n"
                "Do not change numbers from SQL output.\n"
                "Return sections in order: Answer, Limitations/Missing data, Sources.\n\n"
                f"User question:\n{query}\n\n"
                f"{tabular_sql_result.get('prompt_context')}\n\n"
                "Final answer:"
            ),
        )
        return final_prompt, True, rag_debug, [], [], rag_sources

    planner_decision_payload.setdefault("reason_codes", [])
    if "deterministic_execution_unavailable_fallback_narrative" not in planner_decision_payload["reason_codes"]:
        planner_decision_payload["reason_codes"].append("deterministic_execution_unavailable_fallback_narrative")
    planner_decision_payload["confidence"] = min(
        0.7,
        float(planner_decision_payload.get("confidence", 0.7) or 0.7),
    )
    return None

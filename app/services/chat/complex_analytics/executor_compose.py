from __future__ import annotations

from typing import Any, Dict, Optional


async def apply_compose_stage_runtime(
    *,
    payload: Dict[str, Any],
    query: str,
    primary_table_name: str,
    model_source: Optional[str],
    provider_mode: Optional[str],
    model_name: Optional[str],
    prefer_local_for_broad: bool,
    is_broad_full_analysis_query_fn,
    apply_response_meta_fn,
    build_local_formatter_meta_fn,
    compose_response_fn,
    format_answer_fn,
    wants_python_code_fn,
) -> None:
    fallback_response = str(payload.get("final_response") or "").strip()
    if prefer_local_for_broad and is_broad_full_analysis_query_fn(query):
        apply_response_meta_fn(
            payload.get("debug", {}),
            build_local_formatter_meta_fn("broad_query_local_formatter"),
        )
        if not isinstance(payload.get("final_response"), str) or not payload["final_response"].strip():
            payload["final_response"] = fallback_response
        return

    execution_metrics = payload.get("debug", {}).get("complex_analytics", {}).get("metrics", {})
    execution_notes = payload.get("debug", {}).get("complex_analytics", {}).get("notes", [])
    execution_stdout = payload.get("debug", {}).get("complex_analytics", {}).get("stdout", "")
    execution_code = payload.get("debug", {}).get("complex_analytics", {}).get("code_preview") or ""
    if isinstance(execution_metrics, dict) and execution_stdout:
        execution_metrics = dict(execution_metrics)
        execution_metrics["stdout"] = execution_stdout
    generated_response, response_meta = await compose_response_fn(
        query=query,
        table_name=primary_table_name,
        metrics=execution_metrics if isinstance(execution_metrics, dict) else {},
        notes=execution_notes if isinstance(execution_notes, list) else [],
        artifacts=payload.get("artifacts") or [],
        executed_code=execution_code if isinstance(execution_code, str) else "",
        model_source=model_source,
        provider_mode=provider_mode,
        model_name=model_name,
    )
    if generated_response:
        payload["final_response"] = generated_response
    apply_response_meta_fn(payload.get("debug", {}), response_meta)
    if not isinstance(payload.get("final_response"), str) or not payload["final_response"].strip():
        payload["final_response"] = fallback_response or format_answer_fn(
            query=query,
            table_name=primary_table_name,
            metrics=payload.get("debug", {}).get("complex_analytics", {}).get("metrics", {}),
            notes=payload.get("debug", {}).get("complex_analytics", {}).get("notes", []),
            artifacts=payload.get("artifacts") or [],
            executed_code=payload.get("debug", {}).get("complex_analytics", {}).get("code_preview") or "",
            include_code=wants_python_code_fn(query),
            insights=payload.get("debug", {}).get("complex_analytics", {}).get("metrics", {}).get("insights", []),
        )

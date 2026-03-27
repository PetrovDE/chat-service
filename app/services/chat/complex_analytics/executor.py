from __future__ import annotations

import asyncio
import logging
import uuid
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional, Tuple

from app.core.config import settings
from app.observability.metrics import inc_counter, observe_ms
from app.services.llm.manager import llm_manager
from app.services.tabular.sql_execution import (
    ResolvedTabularDataset,
    ResolvedTabularTable,
)

from .artifacts import (
    artifact_public_url,
    artifact_relative_path,
    cleanup_complex_analytics_artifacts,
    sanitize_artifact_for_response,
)
from .analysis_enrichment import enrich_metrics_from_dataframe
from .codegen import (
    generate_complex_analysis_code,
)
from .composer import (
    compose_complex_analytics_response,
    format_complex_analytics_answer,
    wants_python_code,
)
from .dataset_context import (
    collect_datasets_for_file,
    resolve_table_for_query,
)
from .errors import (
    COMPLEX_ANALYTICS_ERROR_CODEGEN,
    COMPLEX_ANALYTICS_ERROR_DATASET,
    COMPLEX_ANALYTICS_ERROR_DEPENDENCY,
    COMPLEX_ANALYTICS_ERROR_MISSING_ARTIFACTS,
    COMPLEX_ANALYTICS_ERROR_OUTPUT_LIMIT,
    COMPLEX_ANALYTICS_ERROR_RUNTIME,
    COMPLEX_ANALYTICS_ERROR_SECURITY,
    COMPLEX_ANALYTICS_ERROR_TIMEOUT,
    COMPLEX_ANALYTICS_ERROR_VALIDATION,
    ComplexAnalyticsOutputLimitError,
    ComplexAnalyticsSecurityError,
    ComplexAnalyticsValidationError,
    SandboxResult,
)
from .execution_limits import resolve_max_artifacts_limit
from .executor_compose import apply_compose_stage_runtime
from .executor_support import (
    build_error_payload as _support_build_error_payload,
    build_executor_error_payload as _support_build_executor_error_payload,
    clarification_for_error as _support_clarification_for_error,
    find_target_tabular_context as _support_find_target_tabular_context,
    resolve_primary_table_context as _support_resolve_primary_table_context,
    validate_executor_result as _support_validate_executor_result,
)
from .planner import is_complex_analytics_query
from .report_quality import build_local_formatter_meta, is_broad_full_analysis_query
from .sandbox import execute_sandboxed_python
from .telemetry import apply_response_meta

logger = logging.getLogger(__name__)

_collect_datasets_for_file = collect_datasets_for_file
_resolve_table_for_query = resolve_table_for_query


def _clarification_for_error(code: str, details: Optional[str] = None) -> str:
    return _support_clarification_for_error(code, details)


def _build_error_payload(
    *,
    query: str,
    target_file: Optional[Any],
    dataset: Optional[ResolvedTabularDataset],
    code: str,
    message: str,
    debug_details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return _support_build_error_payload(
        query=query,
        target_file=target_file,
        dataset=dataset,
        code=code,
        message=message,
        debug_details=debug_details,
    )


def _find_target_tabular_context(
    *,
    query: str,
    files: List[Any],
) -> Tuple[Optional[Any], Optional[ResolvedTabularDataset], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    return _support_find_target_tabular_context(
        query=query,
        files=files,
        collect_datasets_for_file=_collect_datasets_for_file,
    )


def _resolve_primary_table_context(
    *,
    query: str,
    target_file: Any,
    dataset: ResolvedTabularDataset,
    datasets: Dict[str, Any],
) -> Tuple[Optional[ResolvedTabularTable], Optional[Any], Optional[Dict[str, Any]]]:
    return _support_resolve_primary_table_context(
        query=query,
        target_file=target_file,
        dataset=dataset,
        datasets=datasets,
        resolve_table_for_query=_resolve_table_for_query,
    )


async def _apply_compose_stage(
    *,
    payload: Dict[str, Any],
    query: str,
    primary_table_name: str,
    model_source: Optional[str],
    provider_mode: Optional[str],
    model_name: Optional[str],
) -> None:
    await apply_compose_stage_runtime(
        payload=payload,
        query=query,
        primary_table_name=primary_table_name,
        model_source=model_source,
        provider_mode=provider_mode,
        model_name=model_name,
        prefer_local_for_broad=bool(getattr(settings, "COMPLEX_ANALYTICS_PREFER_LOCAL_COMPOSER_FOR_BROAD_QUERY", True)),
        is_broad_full_analysis_query_fn=is_broad_full_analysis_query,
        apply_response_meta_fn=apply_response_meta,
        build_local_formatter_meta_fn=build_local_formatter_meta,
        compose_response_fn=_compose_complex_analytics_response,
        format_answer_fn=_format_complex_analytics_answer,
        wants_python_code_fn=_wants_python_code,
    )


def _build_executor_error_payload(
    *,
    exc: Exception,
    query: str,
    target_file: Any,
    dataset: ResolvedTabularDataset,
    codegen_meta: Optional[Dict[str, Any]],
    timeout_seconds: float,
) -> Dict[str, Any]:
    return _support_build_executor_error_payload(
        exc=exc,
        query=query,
        target_file=target_file,
        dataset=dataset,
        codegen_meta=codegen_meta,
        timeout_seconds=timeout_seconds,
        logger=logger,
    )


def _validate_executor_result(
    *,
    sandbox_result: SandboxResult,
    plan_contract: Optional[Dict[str, Any]],
) -> None:
    _support_validate_executor_result(
        sandbox_result=sandbox_result,
        plan_contract=plan_contract,
    )


def _execute_complex_analytics_sync(
    *,
    query: str,
    target_file: Any,
    dataset: ResolvedTabularDataset,
    datasets: Dict[str, Any],
    primary_table: ResolvedTabularTable,
    code: str,
    codegen_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    run_id = uuid.uuid4().hex
    artifacts_root = settings.get_complex_analytics_artifact_dir()
    _cleanup_complex_analytics_artifacts(artifacts_root=artifacts_root)
    artifacts_dir = artifacts_root / run_id

    effective_codegen_meta = dict(codegen_meta or {})
    primary_frame = datasets.get(primary_table.table_name)
    max_artifacts_limit = resolve_max_artifacts_limit(
        query=query,
        codegen_meta=effective_codegen_meta,
        primary_frame=primary_frame,
    )
    plan_contract = (
        dict(effective_codegen_meta.get("plan_contract"))
        if isinstance(effective_codegen_meta.get("plan_contract"), dict)
        else {}
    )
    try:
        sandbox_result = execute_sandboxed_python(
            code=code,
            datasets=datasets,
            artifacts_dir=artifacts_dir,
            max_output_chars=int(settings.COMPLEX_ANALYTICS_MAX_OUTPUT_CHARS),
            max_artifacts=max_artifacts_limit,
        )
    except Exception as exec_error:
        raise ComplexAnalyticsValidationError(
            COMPLEX_ANALYTICS_ERROR_VALIDATION,
            f"Generated code runtime failure: {type(exec_error).__name__}: {exec_error}",
        ) from exec_error

    metrics = sandbox_result.result.get("metrics")
    notes = sandbox_result.result.get("notes")
    insights = sandbox_result.result.get("insights")
    if isinstance(metrics, dict) and primary_frame is not None:
        metrics = enrich_metrics_from_dataframe(metrics=metrics, frame=primary_frame)
        sandbox_result.result["metrics"] = metrics

    artifacts = sandbox_result.artifacts
    result_artifacts = sandbox_result.result.get("artifacts")
    if isinstance(result_artifacts, list):
        for index, item in enumerate(result_artifacts):
            if index >= len(artifacts):
                break
            if isinstance(item, dict):
                artifacts[index].update({k: v for k, v in item.items() if k != "path"})
    response_artifacts: List[Dict[str, Any]] = []
    for artifact in artifacts:
        path_value = str(artifact.get("path") or "")
        if path_value:
            public_url = artifact_public_url(path_value)
            if public_url:
                artifact["url"] = public_url
        response_artifacts.append(_sanitize_artifact_for_response(artifact))

    sandbox_result = SandboxResult(
        result=sandbox_result.result,
        stdout=sandbox_result.stdout,
        artifacts=response_artifacts,
    )
    _validate_executor_result(
        sandbox_result=sandbox_result,
        plan_contract=plan_contract,
    )

    if response_artifacts:
        inc_counter("complex_analytics_artifacts_generated_total", value=len(response_artifacts))
        for artifact in response_artifacts:
            inc_counter(
                "complex_analytics_artifact_kind_total",
                kind=str(artifact.get("kind") or "unknown"),
            )
    inc_counter("complex_analytics_executor_success_total", engine=str(dataset.engine))

    answer_text = _format_complex_analytics_answer(
        query=query,
        table_name=primary_table.table_name,
        metrics=metrics if isinstance(metrics, dict) else {},
        notes=notes if isinstance(notes, list) else [],
        artifacts=response_artifacts,
        executed_code=code,
        include_code=_wants_python_code(query),
        insights=insights if isinstance(insights, list) else [],
    )
    source_label = getattr(target_file, "original_filename", "unknown")
    return {
        "status": "ok",
        "final_response": answer_text,
        "sources": [
            (
                f"{source_label} | table={primary_table.table_name} "
                f"| dataset_v={dataset.dataset_version} | complex_analytics"
            )
        ],
        "artifacts": response_artifacts,
        "debug": {
            "retrieval_mode": "complex_analytics",
            "intent": "complex_analytics",
            "execution_route": "complex_analytics",
            "executor_attempted": True,
            "executor_status": "success",
            "executor_error_code": None,
            "artifacts_count": len(response_artifacts),
            "complex_analytics": {
                "query": query,
                "dataset_id": dataset.dataset_id,
                "dataset_version": dataset.dataset_version,
                "dataset_provenance_id": dataset.dataset_provenance_id,
                "table_name": primary_table.table_name,
                "table_version": primary_table.table_version,
                "table_provenance_id": primary_table.provenance_id,
                "stdout": sandbox_result.stdout,
                "code_preview": code[:1200],
                "code_source": str((effective_codegen_meta or {}).get("code_source") or "none"),
                "codegen_auto_visual_patch_applied": bool(
                    (effective_codegen_meta or {}).get("codegen_auto_visual_patch_applied")
                    or (
                        (effective_codegen_meta or {}).get("complex_analytics_codegen") or {}
                    ).get("auto_visual_patch_applied")
                ),
                "codegen": dict(effective_codegen_meta or {}),
                "complex_analytics_code_generation_prompt_status": str(
                    (effective_codegen_meta or {}).get("complex_analytics_code_generation_prompt_status") or "unknown"
                ),
                "complex_analytics_code_generation_source": str(
                    (effective_codegen_meta or {}).get("complex_analytics_code_generation_source") or "unknown"
                ),
                "complex_analytics_codegen": dict((effective_codegen_meta or {}).get("complex_analytics_codegen") or {}),
                "sandbox": {
                    "secure_eval": True,
                    "artifacts_limit": max_artifacts_limit,
                    "artifacts_limit_base": int(getattr(settings, "COMPLEX_ANALYTICS_MAX_ARTIFACTS", 16) or 16),
                    "output_limit_chars": int(settings.COMPLEX_ANALYTICS_MAX_OUTPUT_CHARS),
                },
                "plan_contract": dict((effective_codegen_meta or {}).get("plan_contract") or {}),
                "metrics": metrics if isinstance(metrics, dict) else {},
                "notes": notes if isinstance(notes, list) else [],
                "artifact_dir": artifact_relative_path(str(artifacts_dir)) or run_id,
                "response_status": "not_attempted",
                "response_error_code": None,
                "response_meta": None,
            },
        },
    }


async def execute_complex_analytics_path(
    *,
    query: str,
    files: List[Any],
    model_source: Optional[str] = None,
    provider_mode: Optional[str] = None,
    model_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not is_complex_analytics_query(query):
        return None

    target_file, dataset, datasets, early_error = _find_target_tabular_context(query=query, files=files)
    if early_error is not None:
        return early_error
    if target_file is None or dataset is None or datasets is None:
        return _build_error_payload(
            query=query,
            target_file=None,
            dataset=None,
            code=COMPLEX_ANALYTICS_ERROR_DATASET,
            message="No tabular dataset attached to current conversation",
        )
    primary_table, primary_frame, primary_error = _resolve_primary_table_context(
        query=query,
        target_file=target_file,
        dataset=dataset,
        datasets=datasets,
    )
    if primary_error is not None:
        return primary_error
    if primary_table is None or primary_frame is None:
        return _build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_DATASET,
            message="Primary table resolution failed",
        )

    generated_code, codegen_meta = await _generate_complex_analysis_code(
        query=query,
        primary_table_name=primary_table.table_name,
        primary_frame=primary_frame,
        model_source=model_source,
        provider_mode=provider_mode,
        model_name=model_name,
    )
    code_source = str((codegen_meta or {}).get("code_source") or "none")
    if code_source != "llm":
        reason = str((codegen_meta or {}).get("codegen_error") or "codegen_unavailable")
        return _build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_CODEGEN,
            message=f"Code generation did not produce executable llm code; reason={reason}",
            debug_details={"codegen": dict(codegen_meta or {})},
        )

    timeout_seconds = float(settings.COMPLEX_ANALYTICS_TIMEOUT_SECONDS)
    started = perf_counter()
    try:
        payload = await asyncio.wait_for(
            asyncio.to_thread(
                _execute_complex_analytics_sync,
                query=query,
                target_file=target_file,
                dataset=dataset,
                datasets=datasets,
                primary_table=primary_table,
                code=generated_code,
                codegen_meta=codegen_meta,
            ),
            timeout=timeout_seconds,
        )
        observe_ms("complex_analytics_executor_ms", (perf_counter() - started) * 1000.0)
        status = str(payload.get("status") or "ok")
        if status == "ok":
            await _apply_compose_stage(
                payload=payload,
                query=query,
                primary_table_name=primary_table.table_name,
                model_source=model_source,
                provider_mode=provider_mode,
                model_name=model_name,
            )
        if payload["debug"]["complex_analytics"].get("response_status") is None:
            payload["debug"]["complex_analytics"]["response_status"] = "not_attempted"
        inc_counter(
            "complex_analytics_executor_total",
            status=status,
            engine=str(dataset.engine),
        )
        return payload
    except Exception as exc:  # pragma: no cover - fallback protection path
        return _build_executor_error_payload(
            exc=exc,
            query=query,
            target_file=target_file,
            dataset=dataset,
            codegen_meta=codegen_meta,
            timeout_seconds=timeout_seconds,
        )


# Compatibility aliases for moved helpers used by internal tests.
_artifact_public_url = artifact_public_url
_artifact_relative_path = artifact_relative_path
_sanitize_artifact_for_response = sanitize_artifact_for_response
_cleanup_complex_analytics_artifacts = cleanup_complex_analytics_artifacts
_format_complex_analytics_answer = format_complex_analytics_answer
_wants_python_code = wants_python_code
_compose_complex_analytics_response = compose_complex_analytics_response


async def _generate_complex_analysis_code(
    *,
    query: str,
    primary_table_name: str,
    primary_frame: Any,
    model_source: Optional[str],
    provider_mode: Optional[str],
    model_name: Optional[str],
) -> Tuple[str, Dict[str, Any]]:
    return await generate_complex_analysis_code(
        query=query,
        primary_table_name=primary_table_name,
        primary_frame=primary_frame,
        model_source=model_source,
        provider_mode=provider_mode,
        model_name=model_name,
        llm_client=llm_manager,
    )

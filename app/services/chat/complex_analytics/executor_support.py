from __future__ import annotations

import logging
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.observability.metrics import inc_counter
from app.services.tabular.sql_execution import (
    ResolvedTabularDataset,
    ResolvedTabularTable,
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
from .telemetry import build_error_debug_payload


def clarification_for_error(code: str, details: Optional[str] = None) -> str:
    detail_tail = f" ({details})" if details else ""
    if code == COMPLEX_ANALYTICS_ERROR_SECURITY:
        return "Complex analytics sandbox blocked unsafe operations. Remove network/subprocess/system operations and retry."
    if code == COMPLEX_ANALYTICS_ERROR_TIMEOUT:
        return "Complex analytics sandbox timed out. Reduce dataframe scope or simplify the analysis steps and retry."
    if code == COMPLEX_ANALYTICS_ERROR_OUTPUT_LIMIT:
        return "Complex analytics sandbox output exceeded limits. Reduce printed output or chart count and retry."
    if code == COMPLEX_ANALYTICS_ERROR_DATASET:
        return "Complex analytics requires a tabular dataset in this conversation."
    if code == COMPLEX_ANALYTICS_ERROR_DEPENDENCY:
        return "Complex analytics runtime dependency is missing. Install pandas/numpy/duckdb/matplotlib/seaborn in offline environment."
    if code == COMPLEX_ANALYTICS_ERROR_CODEGEN:
        return (
            "Complex analytics code generation failed for this request. "
            "Please specify target columns/metrics/charts explicitly and retry."
        )
    if code == COMPLEX_ANALYTICS_ERROR_MISSING_ARTIFACTS:
        return (
            "Visualization was requested but no valid chart artifacts were produced. "
            "Specify concrete columns for dependency analysis (for example: x, y, grouping) and retry."
        )
    if code == COMPLEX_ANALYTICS_ERROR_VALIDATION:
        return "Complex analytics result validation failed. Clarify requested outputs and retry."
    return f"Complex analytics sandbox failed{detail_tail}. Please clarify or simplify the request."


def build_error_payload(
    *,
    query: str,
    target_file: Optional[Any],
    dataset: Optional[ResolvedTabularDataset],
    code: str,
    message: str,
    debug_details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    source_label = getattr(target_file, "original_filename", "unknown")
    dataset_version = getattr(dataset, "dataset_version", None)
    dataset_id = getattr(dataset, "dataset_id", None)

    return {
        "status": "error",
        "final_response": clarification_for_error(code, details=message),
        "clarification_prompt": clarification_for_error(code, details=message),
        "sources": [f"{source_label} | complex_analytics_error={code}"] if target_file is not None else [],
        "artifacts": [],
        "debug": build_error_debug_payload(
            query=query,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            code=code,
            message=message,
            details=debug_details,
        ),
    }


def find_target_tabular_context(
    *,
    query: str,
    files: List[Any],
    collect_datasets_for_file: Callable[[Any], Optional[Tuple[ResolvedTabularDataset, Dict[str, Any]]]],
) -> Tuple[Optional[Any], Optional[ResolvedTabularDataset], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    target_file = None
    dataset = None
    datasets = None
    for file_obj in files:
        file_type = str(getattr(file_obj, "file_type", "") or "").lower()
        if file_type not in {"xlsx", "xls", "csv"}:
            continue
        try:
            resolved = collect_datasets_for_file(file_obj)
        except RuntimeError as exc:
            message = str(exc)
            code = COMPLEX_ANALYTICS_ERROR_RUNTIME
            if "required for complex analytics executor" in message:
                code = COMPLEX_ANALYTICS_ERROR_DEPENDENCY
            return None, None, None, build_error_payload(
                query=query,
                target_file=file_obj,
                dataset=None,
                code=code,
                message=message,
            )
        if resolved is None:
            continue
        target_file = file_obj
        dataset, datasets = resolved
        break

    if target_file is None or dataset is None or datasets is None:
        return None, None, None, build_error_payload(
            query=query,
            target_file=None,
            dataset=None,
            code=COMPLEX_ANALYTICS_ERROR_DATASET,
            message="No tabular dataset attached to current conversation",
        )
    return target_file, dataset, datasets, None


def resolve_primary_table_context(
    *,
    query: str,
    target_file: Any,
    dataset: ResolvedTabularDataset,
    datasets: Dict[str, Any],
    resolve_table_for_query: Callable[..., Optional[ResolvedTabularTable]],
) -> Tuple[Optional[ResolvedTabularTable], Optional[Any], Optional[Dict[str, Any]]]:
    primary_table = resolve_table_for_query(query=query, dataset=dataset)
    if primary_table is None:
        return None, None, build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_DATASET,
            message="No tables available in tabular dataset",
        )

    primary_frame = datasets.get(primary_table.table_name)
    if primary_frame is None:
        return None, None, build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_DATASET,
            message=f"Primary table frame not found: {primary_table.table_name}",
        )
    return primary_table, primary_frame, None


def build_executor_error_payload(
    *,
    exc: Exception,
    query: str,
    target_file: Any,
    dataset: ResolvedTabularDataset,
    codegen_meta: Optional[Dict[str, Any]],
    timeout_seconds: float,
    logger: logging.Logger,
) -> Dict[str, Any]:
    if isinstance(exc, TimeoutError):
        inc_counter("complex_analytics_executor_error_total", error_code=COMPLEX_ANALYTICS_ERROR_TIMEOUT)
        return build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_TIMEOUT,
            message=f"Execution exceeded timeout={timeout_seconds}s",
        )
    if isinstance(exc, ComplexAnalyticsSecurityError):
        inc_counter("complex_analytics_executor_error_total", error_code=COMPLEX_ANALYTICS_ERROR_SECURITY)
        return build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_SECURITY,
            message=str(exc),
        )
    if isinstance(exc, ComplexAnalyticsOutputLimitError):
        inc_counter("complex_analytics_executor_error_total", error_code=COMPLEX_ANALYTICS_ERROR_OUTPUT_LIMIT)
        return build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=COMPLEX_ANALYTICS_ERROR_OUTPUT_LIMIT,
            message=str(exc),
        )
    if isinstance(exc, ComplexAnalyticsValidationError):
        error_code = str(exc.error_code or COMPLEX_ANALYTICS_ERROR_VALIDATION)
        inc_counter("complex_analytics_executor_error_total", error_code=error_code)
        return build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=error_code,
            message=str(exc),
            debug_details={"codegen": dict(codegen_meta or {})},
        )
    if isinstance(exc, RuntimeError):
        message = str(exc)
        code = COMPLEX_ANALYTICS_ERROR_RUNTIME
        if "required for complex analytics executor" in message:
            code = COMPLEX_ANALYTICS_ERROR_DEPENDENCY
        inc_counter("complex_analytics_executor_error_total", error_code=code)
        return build_error_payload(
            query=query,
            target_file=target_file,
            dataset=dataset,
            code=code,
            message=message,
        )

    logger.warning("Complex analytics executor failed: %s", exc, exc_info=True)
    inc_counter("complex_analytics_executor_error_total", error_code=COMPLEX_ANALYTICS_ERROR_RUNTIME)
    return build_error_payload(
        query=query,
        target_file=target_file,
        dataset=dataset,
        code=COMPLEX_ANALYTICS_ERROR_RUNTIME,
        message=str(exc),
        debug_details={"traceback": traceback.format_exc(limit=20)},
    )


def validate_executor_result(
    *,
    sandbox_result: SandboxResult,
    plan_contract: Optional[Dict[str, Any]],
) -> None:
    result_payload = sandbox_result.result if isinstance(sandbox_result.result, dict) else {}
    if not isinstance(result_payload.get("metrics"), dict):
        raise ComplexAnalyticsValidationError(
            COMPLEX_ANALYTICS_ERROR_VALIDATION,
            "Sandbox result must contain metrics dictionary",
        )

    contract = plan_contract if isinstance(plan_contract, dict) else {}
    expects_visualization = bool(contract.get("expects_visualization"))
    if expects_visualization and not sandbox_result.artifacts:
        raise ComplexAnalyticsValidationError(
            COMPLEX_ANALYTICS_ERROR_MISSING_ARTIFACTS,
            "Visualization requested but no chart artifacts produced",
        )

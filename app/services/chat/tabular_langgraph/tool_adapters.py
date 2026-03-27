from __future__ import annotations

import asyncio
from time import perf_counter
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.core.config import settings
from app.observability.metrics import inc_counter, observe_ms
from app.services.chat import tabular_llm_guarded_planner as guarded_planner
from app.services.chat import tabular_sql as legacy_sql
from app.services.chat.language import detect_preferred_response_language
from app.services.chat.tabular_intent_router import TabularIntentDecision, classify_tabular_query
from app.services.chat.tabular_query_parser import parse_tabular_query
from app.services.chat.tabular_scope_selector import TabularScopeDecision, select_tabular_scope
from app.services.tabular import SQL_ERROR_EXECUTION_FAILED, SQL_ERROR_TIMEOUT, TabularSQLException, resolve_tabular_dataset


def collect_eligible_files(files: Sequence[Any]) -> List[Any]:
    return legacy_sql._collect_eligible_tabular_files(files)


def resolve_scope(*, query: str, files: Sequence[Any]) -> TabularScopeDecision:
    return select_tabular_scope(
        query=query,
        files=collect_eligible_files(files),
        resolve_dataset_fn=resolve_tabular_dataset,
    )


def parse_and_classify(*, query: str, table: Any) -> tuple[str, TabularIntentDecision]:
    parsed_query = parse_tabular_query(query)
    decision = classify_tabular_query(query=query, table=table)
    return str(parsed_query.route or ""), decision


def apply_scope_debug(*, payload: Dict[str, Any], scope_debug_fields: Dict[str, Any]) -> Dict[str, Any]:
    return legacy_sql._apply_scope_debug_fields(payload=payload, scope_debug=scope_debug_fields)


def build_scope_clarification_payload(
    *,
    query: str,
    scope_kind: str,
    scope_options: Sequence[str],
    scope_debug_fields: Dict[str, Any],
) -> Dict[str, Any]:
    return legacy_sql.build_scope_clarification_route_payload(
        query=query,
        scope_kind=scope_kind,
        scope_options=list(scope_options),
        scope_debug=scope_debug_fields,
    )


def build_missing_column_payload(
    *,
    query: str,
    decision: TabularIntentDecision,
    dataset: Any,
    table: Any,
    target_file: Any,
    scope_debug_fields: Dict[str, Any],
) -> Dict[str, Any]:
    payload = legacy_sql._build_missing_column_response(
        query=query,
        decision=decision,
        dataset=dataset,
        table=table,
        target_file=target_file,
    )
    return apply_scope_debug(payload=payload, scope_debug_fields=scope_debug_fields)


def build_schema_question_payload(
    *,
    query: str,
    decision: TabularIntentDecision,
    dataset: Any,
    table: Any,
    target_file: Any,
    scope_debug_fields: Dict[str, Any],
) -> Dict[str, Any]:
    payload = legacy_sql._build_schema_question_payload(
        query=query,
        dataset=dataset,
        table=table,
        target_file=target_file,
        decision=decision,
    )
    return apply_scope_debug(payload=payload, scope_debug_fields=scope_debug_fields)


def is_guarded_candidate(*, parsed_query_route: str, selected_route: str) -> bool:
    return bool(
        guarded_planner._is_guarded_mode_candidate(
            parsed_query_route=parsed_query_route,
            selected_route=selected_route,
        )
    )


async def call_plan_llm(*, query: str, table: Any, feedback: Sequence[str]) -> tuple[Optional[Dict[str, Any]], str]:
    prompt = guarded_planner._build_plan_prompt(query=query, table=table, feedback=feedback)
    max_tokens = int(getattr(settings, "TABULAR_LLM_GUARDED_PLAN_MAX_TOKENS", 800) or 800)
    timeout_seconds = float(getattr(settings, "TABULAR_LLM_GUARDED_PLAN_TIMEOUT_SECONDS", 5.0) or 5.0)
    return await guarded_planner._call_llm_json(
        prompt=prompt,
        max_tokens=max_tokens,
        timeout_seconds=timeout_seconds,
        policy_class="tabular_llm_guarded_plan",
    )


def normalize_and_validate_plan(*, raw_plan: Dict[str, Any], query: str, table: Any) -> Any:
    normalized = guarded_planner.normalize_plan_payload(raw_plan=raw_plan, query=query)
    return guarded_planner._validate_plan(plan=normalized, table=table, query=query)


async def call_execution_spec_llm(
    *,
    query: str,
    validated_plan: Dict[str, Any],
    feedback: Sequence[str],
) -> tuple[Optional[Dict[str, Any]], str]:
    prompt = guarded_planner._build_execution_spec_prompt(
        query=query,
        validated_plan=validated_plan,
        feedback=feedback,
    )
    max_tokens = int(getattr(settings, "TABULAR_LLM_GUARDED_EXECUTION_MAX_TOKENS", 700) or 700)
    timeout_seconds = float(getattr(settings, "TABULAR_LLM_GUARDED_EXECUTION_TIMEOUT_SECONDS", 5.0) or 5.0)
    return await guarded_planner._call_llm_json(
        prompt=prompt,
        max_tokens=max_tokens,
        timeout_seconds=timeout_seconds,
        policy_class="tabular_llm_guarded_execution",
    )


def normalize_and_validate_execution_spec(*, raw_execution_spec: Dict[str, Any], validated_plan: Dict[str, Any]) -> Any:
    normalized = guarded_planner.normalize_execution_spec_payload(
        raw_execution_spec=raw_execution_spec,
        validated_plan=validated_plan,
    )
    return guarded_planner._validate_execution_spec(
        execution_spec=normalized,
        validated_plan=validated_plan,
    )


def validate_sql_for_execution(*, table: Any, execution_spec: Dict[str, Any]) -> tuple[Any, Dict[str, Any]]:
    sql_bundle = guarded_planner._build_sql_from_execution_spec(table=table, execution_spec=execution_spec)
    sql_validation = guarded_planner._validate_sql(
        sql=str(sql_bundle.get("sql") or ""),
        table=table,
        execution_spec=execution_spec,
    )
    return sql_validation, sql_bundle


async def execute_guarded_sql(*, dataset: Any, table: Any, guarded_sql: str, count_sql: str) -> Dict[str, Any]:
    return await asyncio.to_thread(
        guarded_planner._execute_sql,
        dataset=dataset,
        table=table,
        guarded_sql=guarded_sql,
        count_sql=count_sql,
    )


def build_guarded_success_payload(
    *,
    query: str,
    dataset: Any,
    table: Any,
    target_file: Any,
    validated_plan: Dict[str, Any],
    execution_spec: Dict[str, Any],
    guarded_sql: str,
    guard_debug: Dict[str, Any],
    rows: List[Tuple[Any, ...]],
    rows_effective: int,
    repair_iteration_index: int,
    repair_iteration_count: int,
    repair_iteration_trace: Sequence[Dict[str, Any]],
    scope_debug_fields: Dict[str, Any],
) -> Dict[str, Any]:
    payload = guarded_planner._build_success_payload(
        query=query,
        dataset=dataset,
        table=table,
        target_file=target_file,
        validated_plan=validated_plan,
        execution_spec=execution_spec,
        guarded_sql=guarded_sql,
        guard_debug=guard_debug,
        rows=rows,
        rows_effective=rows_effective,
        repair_iteration_index=repair_iteration_index,
        repair_iteration_count=repair_iteration_count,
        repair_iteration_trace=repair_iteration_trace,
    )
    return apply_scope_debug(payload=payload, scope_debug_fields=scope_debug_fields)


def build_guarded_retry_payload(
    *,
    query: str,
    dataset: Any,
    table: Any,
    target_file: Any,
    selected_route: str,
    validated_plan: Dict[str, Any],
    plan_validation_status: str,
    sql_validation_status: str,
    post_execution_validation_status: str,
    repair_iteration_index: int,
    repair_iteration_count: int,
    repair_failure_reason: str,
    repair_iteration_trace: Sequence[Dict[str, Any]],
    scope_debug_fields: Dict[str, Any],
) -> Dict[str, Any]:
    payload = guarded_planner._build_retry_exhausted_payload(
        query=query,
        dataset=dataset,
        table=table,
        target_file=target_file,
        selected_route=selected_route,
        last_plan=validated_plan,
        plan_validation_status=plan_validation_status,
        sql_validation_status=sql_validation_status,
        post_execution_validation_status=post_execution_validation_status,
        repair_iteration_index=repair_iteration_index,
        repair_iteration_count=repair_iteration_count,
        repair_failure_reason=repair_failure_reason,
        repair_iteration_trace=repair_iteration_trace,
    )
    return apply_scope_debug(payload=payload, scope_debug_fields=scope_debug_fields)


async def execute_deterministic_payload(
    *,
    query: str,
    decision: TabularIntentDecision,
    dataset: Any,
    table: Any,
    target_file: Any,
    scope_debug_fields: Dict[str, Any],
) -> Dict[str, Any]:
    selected_route = str(decision.selected_route or "")
    intent_kind = decision.legacy_intent
    detected_language = detect_preferred_response_language(query)
    timeout_seconds = float(settings.TABULAR_SQL_TIMEOUT_SECONDS)

    try:
        started = perf_counter()
        if selected_route in {"overview"}:
            payload = await asyncio.wait_for(
                asyncio.to_thread(
                    legacy_sql._execute_profile_sync,
                    dataset=dataset,
                    table=table,
                    target_file=target_file,
                    timeout_seconds=timeout_seconds,
                ),
                timeout=timeout_seconds,
            )
            payload = legacy_sql._apply_route_debug(
                payload=payload,
                decision=decision,
                detected_language=detected_language,
            )
        elif selected_route in {"filtering"} or intent_kind == "lookup":
            payload = await asyncio.wait_for(
                asyncio.to_thread(
                    legacy_sql._execute_lookup_sync,
                    query=query,
                    dataset=dataset,
                    table=table,
                    target_file=target_file,
                    timeout_seconds=timeout_seconds,
                ),
                timeout=timeout_seconds,
            )
            payload = legacy_sql._apply_route_debug(
                payload=payload,
                decision=decision,
                detected_language=detected_language,
            )
        elif selected_route in {"chart", "trend", "comparison"}:
            payload = await asyncio.wait_for(
                asyncio.to_thread(
                    legacy_sql._execute_chart_sync,
                    query=query,
                    dataset=dataset,
                    table=table,
                    target_file=target_file,
                    timeout_seconds=timeout_seconds,
                    decision=decision,
                ),
                timeout=timeout_seconds,
            )
            payload = legacy_sql._apply_route_debug(
                payload=payload,
                decision=decision,
                detected_language=detected_language,
            )
        else:
            payload = await asyncio.wait_for(
                asyncio.to_thread(
                    legacy_sql._execute_aggregate_sync,
                    query=query,
                    dataset=dataset,
                    table=table,
                    target_file=target_file,
                    timeout_seconds=timeout_seconds,
                ),
                timeout=timeout_seconds,
            )
            payload = legacy_sql._apply_route_debug(
                payload=payload,
                decision=decision,
                detected_language=detected_language,
            )

        observe_ms("tabular_sql_path_ms", (perf_counter() - started) * 1000.0, intent=intent_kind)
        inc_counter("tabular_sql_path_total", intent=intent_kind, engine=dataset.engine)
        return apply_scope_debug(payload=payload, scope_debug_fields=scope_debug_fields)
    except TimeoutError:
        inc_counter("tabular_sql_path_timeout_total", intent=intent_kind, engine=dataset.engine)
        inc_counter(
            "tabular_sql_path_error_total",
            intent=intent_kind,
            engine=dataset.engine,
            error_code=SQL_ERROR_TIMEOUT,
        )
        timeout_exc = TabularSQLException(
            code=SQL_ERROR_TIMEOUT,
            message="Tabular SQL path timeout",
            details={"timeout_seconds": timeout_seconds},
        )
        payload = legacy_sql._build_tabular_error_result(
            query=query,
            intent_kind=intent_kind,
            dataset=dataset,
            table=table,
            target_file=target_file,
            error_payload=legacy_sql.to_tabular_error_payload(timeout_exc),
        )
        payload = legacy_sql._apply_route_debug(
            payload=payload,
            decision=decision,
            detected_language=detected_language,
        )
        return apply_scope_debug(payload=payload, scope_debug_fields=scope_debug_fields)
    except Exception as exc:
        error_payload = legacy_sql.to_tabular_error_payload(exc)
        error_code = str(error_payload.get("code") or SQL_ERROR_EXECUTION_FAILED)
        inc_counter(
            "tabular_sql_path_error_total",
            intent=intent_kind,
            engine=getattr(dataset, "engine", "unknown"),
            error_code=error_code,
        )
        if str(error_payload.get("category") or "") == "guardrail":
            inc_counter(
                "tabular_sql_guardrail_violation_total",
                intent=intent_kind,
                engine=dataset.engine,
                code=error_code,
            )
        payload = legacy_sql._build_tabular_error_result(
            query=query,
            intent_kind=intent_kind,
            dataset=dataset,
            table=table,
            target_file=target_file,
            error_payload=error_payload,
        )
        payload = legacy_sql._apply_route_debug(
            payload=payload,
            decision=decision,
            detected_language=detected_language,
        )
        return apply_scope_debug(payload=payload, scope_debug_fields=scope_debug_fields)

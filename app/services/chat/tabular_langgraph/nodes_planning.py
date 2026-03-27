from __future__ import annotations

from typing import Dict

from app.services.chat import tabular_llm_guarded_planner as guarded_planner
from app.services.chat.tabular_langgraph import tool_adapters
from app.services.chat.tabular_langgraph.node_utils import append_trace
from app.services.chat.tabular_langgraph.state import TabularLangGraphState


async def build_plan(state: TabularLangGraphState) -> Dict[str, object]:
    if bool(state.get("skip_guarded_plan", False)):
        return {
            "next_step": "execute_tools",
            "node_trace": append_trace(state, node="build_plan", status="skipped", reason="guarded_disabled"),
        }

    table = state.get("table")
    raw_plan, plan_call_status = await tool_adapters.call_plan_llm(
        query=state["query"],
        table=table,
        feedback=list(state.get("plan_feedback") or []),
    )
    if plan_call_status in {"llm_timeout", "llm_runtime_error"}:
        return {
            "skip_guarded_plan": True,
            "next_step": "execute_tools",
            "node_trace": append_trace(state, node="build_plan", status="skipped", reason=plan_call_status),
        }

    if plan_call_status != "success" or not isinstance(raw_plan, dict):
        return {
            "retry_next": True,
            "retry_reason": "plan_invalid_json",
            "plan_feedback": [f"plan parse failed: {plan_call_status}"],
            "next_step": "repair_or_clarify",
            "node_trace": append_trace(state, node="build_plan", status="retry", reason="plan_invalid_json"),
        }

    return {
        "raw_plan": raw_plan,
        "next_step": "validate_plan",
        "node_trace": append_trace(state, node="build_plan", status="ok"),
    }


def validate_plan(state: TabularLangGraphState) -> Dict[str, object]:
    raw_plan = state.get("raw_plan") if isinstance(state.get("raw_plan"), dict) else None
    if not isinstance(raw_plan, dict):
        return {
            "retry_next": True,
            "retry_reason": "missing_raw_plan",
            "next_step": "repair_or_clarify",
            "node_trace": append_trace(state, node="validate_plan", status="retry", reason="missing_raw_plan"),
        }

    validation = tool_adapters.normalize_and_validate_plan(
        raw_plan=raw_plan,
        query=state["query"],
        table=state.get("table"),
    )
    if validation.status != "success" or not isinstance(validation.payload, dict):
        errors = list(validation.errors) or [validation.reason]
        return {
            "retry_next": True,
            "retry_reason": str(validation.reason or "plan_validation_failed"),
            "plan_feedback": [f"plan validation failed: {item}" for item in errors[:6]],
            "next_step": "repair_or_clarify",
            "node_trace": append_trace(state, node="validate_plan", status="retry", reason=str(validation.reason)),
        }

    return {
        "validated_plan": dict(validation.payload),
        "next_step": "build_execution_spec",
        "node_trace": append_trace(state, node="validate_plan", status="ok"),
    }


async def build_execution_spec(state: TabularLangGraphState) -> Dict[str, object]:
    validated_plan = state.get("validated_plan") if isinstance(state.get("validated_plan"), dict) else None
    if not isinstance(validated_plan, dict):
        return {
            "retry_next": True,
            "retry_reason": "missing_validated_plan",
            "next_step": "repair_or_clarify",
            "node_trace": append_trace(state, node="build_execution_spec", status="retry", reason="missing_validated_plan"),
        }

    raw_execution_spec, execution_call_status = await tool_adapters.call_execution_spec_llm(
        query=state["query"],
        validated_plan=validated_plan,
        feedback=list(state.get("execution_feedback") or []),
    )
    if execution_call_status in {"llm_timeout", "llm_runtime_error"}:
        return {
            "skip_guarded_plan": True,
            "next_step": "execute_tools",
            "node_trace": append_trace(state, node="build_execution_spec", status="skipped", reason=execution_call_status),
        }

    if execution_call_status != "success" or not isinstance(raw_execution_spec, dict):
        return {
            "retry_next": True,
            "retry_reason": "execution_spec_invalid_json",
            "execution_feedback": [f"execution spec parse failed: {execution_call_status}"],
            "next_step": "repair_or_clarify",
            "node_trace": append_trace(state, node="build_execution_spec", status="retry", reason="execution_spec_invalid_json"),
        }

    return {
        "raw_execution_spec": raw_execution_spec,
        "next_step": "validate_execution_spec",
        "node_trace": append_trace(state, node="build_execution_spec", status="ok"),
    }


def validate_execution_spec(state: TabularLangGraphState) -> Dict[str, object]:
    validated_plan = state.get("validated_plan") if isinstance(state.get("validated_plan"), dict) else None
    raw_execution_spec = state.get("raw_execution_spec") if isinstance(state.get("raw_execution_spec"), dict) else None
    if not isinstance(validated_plan, dict) or not isinstance(raw_execution_spec, dict):
        return {
            "retry_next": True,
            "retry_reason": "execution_spec_missing",
            "next_step": "repair_or_clarify",
            "node_trace": append_trace(state, node="validate_execution_spec", status="retry", reason="execution_spec_missing"),
        }

    execution_validation = tool_adapters.normalize_and_validate_execution_spec(
        raw_execution_spec=raw_execution_spec,
        validated_plan=validated_plan,
    )
    if execution_validation.status != "success" or not isinstance(execution_validation.payload, dict):
        errors = list(execution_validation.errors) or [execution_validation.reason]
        return {
            "retry_next": True,
            "retry_reason": str(execution_validation.reason or "execution_spec_validation_failed"),
            "execution_feedback": [f"execution spec validation failed: {item}" for item in errors[:6]],
            "next_step": "repair_or_clarify",
            "node_trace": append_trace(state, node="validate_execution_spec", status="retry", reason=str(execution_validation.reason)),
        }

    execution_spec = dict(execution_validation.payload)
    sql_validation, sql_bundle = tool_adapters.validate_sql_for_execution(
        table=state.get("table"),
        execution_spec=execution_spec,
    )
    if sql_validation.status != "success" or not isinstance(sql_validation.payload, dict):
        return {
            "retry_next": True,
            "retry_reason": str(sql_validation.reason or "sql_validation_failed"),
            "execution_feedback": [f"sql validation failed: {sql_validation.reason}"],
            "next_step": "repair_or_clarify",
            "node_trace": append_trace(state, node="validate_execution_spec", status="retry", reason=str(sql_validation.reason)),
        }

    return {
        "execution_spec": execution_spec,
        "guarded_sql": str(sql_validation.payload.get("guarded_sql") or ""),
        "count_sql": str(sql_bundle.get("count_sql") or ""),
        "guard_debug": dict(sql_validation.payload.get("guard_debug") or {}),
        "raw_execution_spec": raw_execution_spec,
        "next_step": "execute_tools",
        "node_trace": append_trace(state, node="validate_execution_spec", status="ok"),
    }


def repair_or_clarify(state: TabularLangGraphState) -> Dict[str, object]:
    if not bool(state.get("retry_next", False)):
        return {
            "next_step": "compose_answer",
            "node_trace": append_trace(state, node="repair_or_clarify", status="ok", reason="no_retry"),
        }

    if not bool(state.get("guarded_enabled", False)):
        return {
            "skip_guarded_plan": True,
            "retry_next": False,
            "next_step": "execute_tools",
            "node_trace": append_trace(state, node="repair_or_clarify", status="ok", reason="guarded_not_enabled"),
        }

    max_attempts = int(state.get("max_attempts", 1) or 1)
    current_attempt = int(state.get("repair_iteration_index", 0) or 0) + 1
    if current_attempt < max_attempts:
        return {
            "repair_iteration_index": current_attempt,
            "retry_next": False,
            "next_step": "build_plan",
            "node_trace": append_trace(
                state,
                node="repair_or_clarify",
                status="retry",
                reason=str(state.get("retry_reason") or "retry"),
            ),
        }

    validated_plan = state.get("validated_plan") if isinstance(state.get("validated_plan"), dict) else {}
    selected_route = str(state.get("selected_route") or "aggregation")
    if validated_plan:
        selected_route = guarded_planner._route_from_validated_plan(validated_plan)

    payload = tool_adapters.build_guarded_retry_payload(
        query=state["query"],
        dataset=state.get("dataset"),
        table=state.get("table"),
        target_file=state.get("target_file"),
        selected_route=selected_route,
        validated_plan=validated_plan,
        plan_validation_status="failed",
        sql_validation_status="failed",
        post_execution_validation_status="failed",
        repair_iteration_index=max_attempts,
        repair_iteration_count=max_attempts,
        repair_failure_reason=str(state.get("retry_reason") or "retries_exhausted"),
        repair_iteration_trace=list(state.get("node_trace") or []),
        scope_debug_fields=dict(state.get("scope_debug_fields") or {}),
    )
    return {
        "payload": payload,
        "retry_next": False,
        "next_step": "compose_answer",
        "node_trace": append_trace(state, node="repair_or_clarify", status="ok", reason="retries_exhausted"),
    }

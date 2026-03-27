from __future__ import annotations

from typing import Any, Dict, List

from app.services.chat import tabular_llm_guarded_planner as guarded_planner
from app.services.chat.tabular_langgraph import tool_adapters
from app.services.chat.tabular_langgraph.node_utils import GRAPH_VERSION, append_trace
from app.services.chat.tabular_langgraph.state import TabularLangGraphState


def _append_compact(values: List[str], value: Any, *, limit: int = 16) -> List[str]:
    candidate = str(value or "").strip()
    if not candidate:
        return list(values or [])
    merged = list(values or [])
    merged.append(candidate)
    return merged[-limit:]


async def execute_tools(state: TabularLangGraphState) -> Dict[str, object]:
    if isinstance(state.get("payload"), dict) or bool(state.get("terminal_none_payload", False)):
        return {
            "next_step": "validate_result",
            "executed_tools": list(state.get("executed_tools") or []),
            "tool_errors": list(state.get("tool_errors") or []),
            "node_trace": append_trace(state, node="execute_tools", status="ok", reason="payload_already_set"),
        }

    query = state["query"]
    scope_debug_fields = dict(state.get("scope_debug_fields") or {})
    decision = state.get("intent_decision")
    dataset = state.get("dataset")
    table = state.get("table")
    target_file = state.get("target_file")
    selected_route = str(state.get("selected_route") or "")
    executed_tools = [str(item) for item in list(state.get("executed_tools") or []) if str(item or "").strip()]
    tool_errors = [str(item) for item in list(state.get("tool_errors") or []) if str(item or "").strip()]

    if bool(state.get("guarded_enabled", False)) and not bool(state.get("skip_guarded_plan", False)):
        execution_spec = state.get("execution_spec") if isinstance(state.get("execution_spec"), dict) else None
        guarded_sql = str(state.get("guarded_sql") or "").strip()
        count_sql = str(state.get("count_sql") or "").strip() or guarded_sql
        validated_plan = state.get("validated_plan") if isinstance(state.get("validated_plan"), dict) else {}
        if execution_spec and guarded_sql:
            executed_tools = _append_compact(executed_tools, "execute_guarded_sql")
            try:
                execution_output = await tool_adapters.execute_guarded_sql(
                    dataset=dataset,
                    table=table,
                    guarded_sql=guarded_sql,
                    count_sql=count_sql,
                )
                rows = list(execution_output.get("rows") or [])
                rows_effective = int(execution_output.get("rows_effective", 0) or 0)
                post_validation = guarded_planner.validate_post_execution(rows=rows, execution_spec=execution_spec)
                if post_validation.status != "success":
                    tool_errors = _append_compact(
                        tool_errors,
                        f"post_execution_validation_failed:{post_validation.reason}",
                    )
                    return {
                        "retry_next": True,
                        "retry_reason": str(post_validation.reason or "post_execution_validation_failed"),
                        "execution_feedback": [f"post execution validation failed: {post_validation.reason}"],
                        "executed_tools": executed_tools,
                        "tool_errors": tool_errors,
                        "next_step": "repair_or_clarify",
                        "node_trace": append_trace(
                            state,
                            node="execute_tools",
                            status="retry",
                            reason=str(post_validation.reason),
                        ),
                    }

                repair_index = int(state.get("repair_iteration_index", 0) or 0)
                repair_count = int(state.get("repair_iteration_count", state.get("max_attempts", 1)) or 1)
                payload = tool_adapters.build_guarded_success_payload(
                    query=query,
                    dataset=dataset,
                    table=table,
                    target_file=target_file,
                    validated_plan=validated_plan,
                    execution_spec=execution_spec,
                    guarded_sql=guarded_sql,
                    guard_debug=dict(state.get("guard_debug") or {}),
                    rows=rows,
                    rows_effective=rows_effective,
                    repair_iteration_index=max(1, repair_index),
                    repair_iteration_count=repair_count,
                    repair_iteration_trace=list(state.get("node_trace") or []),
                    scope_debug_fields=scope_debug_fields,
                )
                return {
                    "payload": payload,
                    "retry_next": False,
                    "executed_tools": executed_tools,
                    "tool_errors": tool_errors,
                    "next_step": "validate_result",
                    "node_trace": append_trace(state, node="execute_tools", status="ok", reason="guarded_success"),
                }
            except Exception as exc:  # pragma: no cover - defensive runtime guard
                tool_errors = _append_compact(tool_errors, f"execute_guarded_sql:{type(exc).__name__}")
                return {
                    "retry_next": True,
                    "retry_reason": type(exc).__name__,
                    "execution_feedback": [f"execution failed: {type(exc).__name__}"],
                    "executed_tools": executed_tools,
                    "tool_errors": tool_errors,
                    "next_step": "repair_or_clarify",
                    "node_trace": append_trace(state, node="execute_tools", status="retry", reason=type(exc).__name__),
                }

    if selected_route == "unsupported_missing_column":
        executed_tools = _append_compact(executed_tools, "build_missing_column_payload")
        payload = tool_adapters.build_missing_column_payload(
            query=query,
            decision=decision,
            dataset=dataset,
            table=table,
            target_file=target_file,
            scope_debug_fields=scope_debug_fields,
        )
    elif selected_route == "schema_question":
        executed_tools = _append_compact(executed_tools, "build_schema_question_payload")
        payload = tool_adapters.build_schema_question_payload(
            query=query,
            decision=decision,
            dataset=dataset,
            table=table,
            target_file=target_file,
            scope_debug_fields=scope_debug_fields,
        )
    else:
        executed_tools = _append_compact(executed_tools, "execute_deterministic_payload")
        payload = await tool_adapters.execute_deterministic_payload(
            query=query,
            decision=decision,
            dataset=dataset,
            table=table,
            target_file=target_file,
            scope_debug_fields=scope_debug_fields,
        )

    if isinstance(payload, dict) and str(payload.get("status") or "").lower() == "error":
        payload_debug = payload.get("debug") if isinstance(payload.get("debug"), dict) else {}
        tool_errors = _append_compact(
            tool_errors,
            str(
                payload_debug.get("fallback_reason")
                or payload_debug.get("executor_error_code")
                or "deterministic_payload_error"
            ),
        )

    return {
        "payload": payload,
        "retry_next": False,
        "executed_tools": executed_tools,
        "tool_errors": tool_errors,
        "next_step": "validate_result",
        "node_trace": append_trace(state, node="execute_tools", status="ok", reason="deterministic"),
    }


def validate_result(state: TabularLangGraphState) -> Dict[str, object]:
    payload = state.get("payload")
    if payload is None and bool(state.get("terminal_none_payload", False)):
        return {
            "next_step": "emit_debug_trace",
            "graph_stop_reason": "terminal_none_payload",
            "node_trace": append_trace(state, node="validate_result", status="ok", reason="none_payload"),
        }
    if not isinstance(payload, dict):
        return {
            "retry_next": True,
            "retry_reason": "invalid_executor_payload",
            "graph_stop_reason": "invalid_executor_payload",
            "next_step": "repair_or_clarify",
            "node_trace": append_trace(state, node="validate_result", status="retry", reason="invalid_executor_payload"),
        }

    status = str(payload.get("status") or "")
    if status not in {"ok", "error"}:
        return {
            "retry_next": True,
            "retry_reason": "invalid_status",
            "graph_stop_reason": "invalid_status",
            "next_step": "repair_or_clarify",
            "node_trace": append_trace(state, node="validate_result", status="retry", reason="invalid_status"),
        }

    return {
        "next_step": "compose_answer",
        "graph_stop_reason": "payload_ready" if status == "ok" else "payload_error_ready",
        "node_trace": append_trace(state, node="validate_result", status="ok"),
    }


def compose_answer(state: TabularLangGraphState) -> Dict[str, object]:
    return {
        "next_step": "emit_debug_trace",
        "graph_stop_reason": str(state.get("graph_stop_reason") or "compose_answer"),
        "node_trace": append_trace(state, node="compose_answer", status="ok"),
    }


def emit_debug_trace(state: TabularLangGraphState) -> Dict[str, object]:
    payload = state.get("payload")
    if not isinstance(payload, dict):
        return {
            "next_step": "done",
            "graph_stop_reason": str(state.get("graph_stop_reason") or "none_payload"),
            "node_trace": append_trace(state, node="emit_debug_trace", status="ok", reason="none_payload"),
        }

    debug = payload.get("debug")
    if not isinstance(debug, dict):
        debug = {}
        payload["debug"] = debug
    tabular_debug = debug.get("tabular_sql")
    if not isinstance(tabular_debug, dict):
        tabular_debug = {}
        debug["tabular_sql"] = tabular_debug

    graph_trace = list(state.get("node_trace") or [])
    node_path = [
        str(item.get("node") or "").strip()
        for item in graph_trace
        if isinstance(item, dict) and str(item.get("node") or "").strip()
    ]
    guarded_mode_used = bool(state.get("guarded_enabled", False)) and not bool(state.get("skip_guarded_plan", False))
    repair_index = int(state.get("repair_iteration_index", 0) or 0)
    repair_count = int(state.get("repair_iteration_count", 0) or 0)
    if guarded_mode_used:
        graph_attempts = max(1, repair_index + 1, repair_count)
    else:
        graph_attempts = 1 if node_path else 0
    stop_reason = str(state.get("graph_stop_reason") or "completed")

    plan_summary = state.get("plan_summary") if isinstance(state.get("plan_summary"), dict) else {}
    execution_spec_summary = (
        state.get("execution_spec_summary")
        if isinstance(state.get("execution_spec_summary"), dict)
        else {}
    )
    plan_validation_failures = [
        str(item) for item in list(state.get("plan_validation_failures") or []) if str(item or "").strip()
    ]
    execution_spec_validation_failures = [
        str(item)
        for item in list(state.get("execution_spec_validation_failures") or [])
        if str(item or "").strip()
    ]
    executed_tools = [str(item) for item in list(state.get("executed_tools") or []) if str(item or "").strip()]
    tool_errors = [str(item) for item in list(state.get("tool_errors") or []) if str(item or "").strip()]
    planner_mode = "llm_guarded" if bool(state.get("guarded_enabled", False)) and not bool(state.get("skip_guarded_plan", False)) else "deterministic"
    clarification_reason_code = str(
        state.get("clarification_reason_code")
        or debug.get("clarification_reason_code")
        or "none"
    )
    additive_fields = {
        "analytics_engine_graph_version": GRAPH_VERSION,
        "analytics_engine_graph_trace": graph_trace,
        "analytics_engine_graph_run_id": str(state.get("graph_run_id") or "").strip() or None,
        "analytics_engine_graph_node_path": node_path,
        "analytics_engine_graph_attempts": graph_attempts,
        "analytics_engine_graph_stop_reason": stop_reason,
        "graph_run_id": str(state.get("graph_run_id") or "").strip() or None,
        "graph_node_path": node_path,
        "graph_attempts": graph_attempts,
        "stop_reason": stop_reason,
        "planner_mode": str(debug.get("planner_mode") or planner_mode),
        "plan_hash": str(state.get("plan_hash") or "").strip() or None,
        "plan_summary": plan_summary,
        "plan_validation_failures": plan_validation_failures,
        "execution_spec_summary": execution_spec_summary,
        "execution_spec_validation_failures": execution_spec_validation_failures,
        "executed_tools": executed_tools,
        "tool_errors": tool_errors,
        "clarification_reason_code": clarification_reason_code,
    }
    debug.update(additive_fields)
    tabular_debug.update(additive_fields)

    return {
        "payload": payload,
        "next_step": "done",
        "node_trace": append_trace(state, node="emit_debug_trace", status="ok"),
    }

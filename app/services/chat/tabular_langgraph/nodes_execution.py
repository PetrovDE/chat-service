from __future__ import annotations

from typing import Dict

from app.services.chat import tabular_llm_guarded_planner as guarded_planner
from app.services.chat.tabular_langgraph import tool_adapters
from app.services.chat.tabular_langgraph.node_utils import GRAPH_VERSION, append_trace
from app.services.chat.tabular_langgraph.state import TabularLangGraphState


async def execute_tools(state: TabularLangGraphState) -> Dict[str, object]:
    if isinstance(state.get("payload"), dict) or bool(state.get("terminal_none_payload", False)):
        return {
            "next_step": "validate_result",
            "node_trace": append_trace(state, node="execute_tools", status="ok", reason="payload_already_set"),
        }

    query = state["query"]
    scope_debug_fields = dict(state.get("scope_debug_fields") or {})
    decision = state.get("intent_decision")
    dataset = state.get("dataset")
    table = state.get("table")
    target_file = state.get("target_file")
    selected_route = str(state.get("selected_route") or "")

    if bool(state.get("guarded_enabled", False)) and not bool(state.get("skip_guarded_plan", False)):
        execution_spec = state.get("execution_spec") if isinstance(state.get("execution_spec"), dict) else None
        guarded_sql = str(state.get("guarded_sql") or "").strip()
        count_sql = str(state.get("count_sql") or "").strip() or guarded_sql
        validated_plan = state.get("validated_plan") if isinstance(state.get("validated_plan"), dict) else {}
        if execution_spec and guarded_sql:
            try:
                execution_output = await tool_adapters.execute_guarded_sql(
                    dataset=dataset,
                    table=table,
                    guarded_sql=guarded_sql,
                    count_sql=count_sql,
                )
                rows = list(execution_output.get("rows") or [])
                rows_effective = int(execution_output.get("rows_effective", 0) or 0)
                post_validation = guarded_planner._validate_post_execution(rows=rows, execution_spec=execution_spec)
                if post_validation.status != "success":
                    return {
                        "retry_next": True,
                        "retry_reason": str(post_validation.reason or "post_execution_validation_failed"),
                        "execution_feedback": [f"post execution validation failed: {post_validation.reason}"],
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
                    "next_step": "validate_result",
                    "node_trace": append_trace(state, node="execute_tools", status="ok", reason="guarded_success"),
                }
            except Exception as exc:  # pragma: no cover - defensive runtime guard
                return {
                    "retry_next": True,
                    "retry_reason": type(exc).__name__,
                    "execution_feedback": [f"execution failed: {type(exc).__name__}"],
                    "next_step": "repair_or_clarify",
                    "node_trace": append_trace(state, node="execute_tools", status="retry", reason=type(exc).__name__),
                }

    if selected_route == "unsupported_missing_column":
        payload = tool_adapters.build_missing_column_payload(
            query=query,
            decision=decision,
            dataset=dataset,
            table=table,
            target_file=target_file,
            scope_debug_fields=scope_debug_fields,
        )
    elif selected_route == "schema_question":
        payload = tool_adapters.build_schema_question_payload(
            query=query,
            decision=decision,
            dataset=dataset,
            table=table,
            target_file=target_file,
            scope_debug_fields=scope_debug_fields,
        )
    else:
        payload = await tool_adapters.execute_deterministic_payload(
            query=query,
            decision=decision,
            dataset=dataset,
            table=table,
            target_file=target_file,
            scope_debug_fields=scope_debug_fields,
        )

    return {
        "payload": payload,
        "retry_next": False,
        "next_step": "validate_result",
        "node_trace": append_trace(state, node="execute_tools", status="ok", reason="deterministic"),
    }


def validate_result(state: TabularLangGraphState) -> Dict[str, object]:
    payload = state.get("payload")
    if payload is None and bool(state.get("terminal_none_payload", False)):
        return {
            "next_step": "emit_debug_trace",
            "node_trace": append_trace(state, node="validate_result", status="ok", reason="none_payload"),
        }
    if not isinstance(payload, dict):
        return {
            "retry_next": True,
            "retry_reason": "invalid_executor_payload",
            "next_step": "repair_or_clarify",
            "node_trace": append_trace(state, node="validate_result", status="retry", reason="invalid_executor_payload"),
        }

    status = str(payload.get("status") or "")
    if status not in {"ok", "error"}:
        return {
            "retry_next": True,
            "retry_reason": "invalid_status",
            "next_step": "repair_or_clarify",
            "node_trace": append_trace(state, node="validate_result", status="retry", reason="invalid_status"),
        }

    return {
        "next_step": "compose_answer",
        "node_trace": append_trace(state, node="validate_result", status="ok"),
    }


def compose_answer(state: TabularLangGraphState) -> Dict[str, object]:
    return {
        "next_step": "emit_debug_trace",
        "node_trace": append_trace(state, node="compose_answer", status="ok"),
    }


def emit_debug_trace(state: TabularLangGraphState) -> Dict[str, object]:
    payload = state.get("payload")
    if not isinstance(payload, dict):
        return {
            "next_step": "done",
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
    planner_mode = "llm_guarded" if bool(state.get("guarded_enabled", False)) and not bool(state.get("skip_guarded_plan", False)) else "deterministic"
    additive_fields = {
        "analytics_engine_graph_version": GRAPH_VERSION,
        "analytics_engine_graph_trace": graph_trace,
        "planner_mode": str(debug.get("planner_mode") or planner_mode),
    }
    debug.update(additive_fields)
    tabular_debug.update(additive_fields)

    return {
        "payload": payload,
        "next_step": "done",
        "node_trace": append_trace(state, node="emit_debug_trace", status="ok"),
    }

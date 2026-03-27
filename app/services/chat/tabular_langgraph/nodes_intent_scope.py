from __future__ import annotations

from typing import Dict

from app.core.config import settings
from app.services.chat.tabular_query_parser import parse_tabular_query
from app.services.chat.tabular_langgraph import tool_adapters
from app.services.chat.tabular_langgraph.node_utils import append_trace
from app.services.chat.tabular_langgraph.state import TabularLangGraphState


def detect_intent(state: TabularLangGraphState) -> Dict[str, object]:
    parsed = parse_tabular_query(state["query"])
    return {
        "parsed_route": str(parsed.route or ""),
        "next_step": "resolve_scope",
        "node_trace": append_trace(state, node="detect_intent", status="ok"),
    }


def resolve_scope(state: TabularLangGraphState) -> Dict[str, object]:
    scope = tool_adapters.resolve_scope(query=state["query"], files=state["files"])
    scope_debug_fields = dict(scope.debug_fields or {})
    parsed_route = str(state.get("parsed_route") or "")

    if scope.status == "no_tabular_dataset":
        return {
            "scope_status": scope.status,
            "terminal_none_payload": True,
            "payload": None,
            "next_step": "emit_debug_trace",
            "scope_debug_fields": scope_debug_fields,
            "node_trace": append_trace(state, node="resolve_scope", status="ok", reason="no_tabular_dataset"),
        }

    if scope.status == "ambiguous_file":
        payload = tool_adapters.build_scope_clarification_payload(
            query=state["query"],
            scope_kind="file",
            scope_options=list(scope.clarification_options or []),
            scope_debug_fields=scope_debug_fields,
        )
        return {
            "scope_status": scope.status,
            "payload": payload,
            "scope_debug_fields": scope_debug_fields,
            "next_step": "compose_answer",
            "node_trace": append_trace(state, node="resolve_scope", status="ok", reason="ambiguous_file"),
        }

    if scope.status == "ambiguous_table" and parsed_route != "schema_question":
        payload = tool_adapters.build_scope_clarification_payload(
            query=state["query"],
            scope_kind="sheet/table",
            scope_options=list(scope.clarification_options or []),
            scope_debug_fields=scope_debug_fields,
        )
        return {
            "scope_status": scope.status,
            "payload": payload,
            "scope_debug_fields": scope_debug_fields,
            "next_step": "compose_answer",
            "node_trace": append_trace(state, node="resolve_scope", status="ok", reason="ambiguous_table"),
        }

    return {
        "scope_status": scope.status,
        "scope_debug_fields": scope_debug_fields,
        "target_file": scope.target_file,
        "dataset": scope.dataset,
        "table": scope.table,
        "next_step": "inspect_data_sources",
        "node_trace": append_trace(state, node="resolve_scope", status="ok", reason="selected"),
    }


def inspect_data_sources(state: TabularLangGraphState) -> Dict[str, object]:
    target_file = state.get("target_file")
    dataset = state.get("dataset")
    table = state.get("table")

    if target_file is None or dataset is None or table is None:
        return {
            "terminal_none_payload": True,
            "payload": None,
            "next_step": "emit_debug_trace",
            "node_trace": append_trace(state, node="inspect_data_sources", status="error", reason="missing_scope_data"),
        }

    parsed_query_route, decision = tool_adapters.parse_and_classify(query=state["query"], table=table)
    intent_kind = decision.legacy_intent
    if intent_kind is None:
        return {
            "terminal_none_payload": True,
            "payload": None,
            "next_step": "emit_debug_trace",
            "node_trace": append_trace(state, node="inspect_data_sources", status="error", reason="unknown_intent"),
        }

    selected_route = str(decision.selected_route or "")
    guarded_enabled = bool(getattr(settings, "TABULAR_LLM_GUARDED_PLANNER_ENABLED", False)) and tool_adapters.is_guarded_candidate(
        parsed_query_route=parsed_query_route,
        selected_route=selected_route,
    )
    max_attempts = int(getattr(settings, "TABULAR_LLM_GUARDED_MAX_ATTEMPTS", 3) or 3)
    max_attempts = max(1, min(5, max_attempts))

    return {
        "intent_decision": decision,
        "selected_route": selected_route,
        "intent_kind": intent_kind,
        "guarded_enabled": guarded_enabled,
        "skip_guarded_plan": not guarded_enabled,
        "max_attempts": max_attempts,
        "repair_iteration_index": 0,
        "repair_iteration_count": max_attempts,
        "plan_feedback": [],
        "execution_feedback": [],
        "next_step": "build_plan" if guarded_enabled else "execute_tools",
        "node_trace": append_trace(state, node="inspect_data_sources", status="ok"),
    }

from __future__ import annotations

from typing import Any

from langgraph.graph import END, START, StateGraph

from app.services.chat.tabular_langgraph import nodes
from app.services.chat.tabular_langgraph.state import TabularLangGraphState


def _next_step(state: TabularLangGraphState) -> str:
    return str(state.get("next_step") or "done")


def build_tabular_langgraph() -> Any:
    graph = StateGraph(TabularLangGraphState)

    graph.add_node("detect_intent", nodes.detect_intent)
    graph.add_node("resolve_scope", nodes.resolve_scope)
    graph.add_node("inspect_data_sources", nodes.inspect_data_sources)
    graph.add_node("build_plan", nodes.build_plan)
    graph.add_node("validate_plan", nodes.validate_plan)
    graph.add_node("build_execution_spec", nodes.build_execution_spec)
    graph.add_node("validate_execution_spec", nodes.validate_execution_spec)
    graph.add_node("execute_tools", nodes.execute_tools)
    graph.add_node("validate_result", nodes.validate_result)
    graph.add_node("repair_or_clarify", nodes.repair_or_clarify)
    graph.add_node("compose_answer", nodes.compose_answer)
    graph.add_node("emit_debug_trace", nodes.emit_debug_trace)

    graph.add_edge(START, "detect_intent")
    graph.add_edge("detect_intent", "resolve_scope")

    graph.add_conditional_edges(
        "resolve_scope",
        _next_step,
        {
            "inspect_data_sources": "inspect_data_sources",
            "compose_answer": "compose_answer",
            "emit_debug_trace": "emit_debug_trace",
            "done": END,
        },
    )
    graph.add_conditional_edges(
        "inspect_data_sources",
        _next_step,
        {
            "build_plan": "build_plan",
            "execute_tools": "execute_tools",
            "emit_debug_trace": "emit_debug_trace",
            "done": END,
        },
    )
    graph.add_conditional_edges(
        "build_plan",
        _next_step,
        {
            "validate_plan": "validate_plan",
            "execute_tools": "execute_tools",
            "repair_or_clarify": "repair_or_clarify",
            "done": END,
        },
    )
    graph.add_conditional_edges(
        "validate_plan",
        _next_step,
        {
            "build_execution_spec": "build_execution_spec",
            "repair_or_clarify": "repair_or_clarify",
            "done": END,
        },
    )
    graph.add_conditional_edges(
        "build_execution_spec",
        _next_step,
        {
            "validate_execution_spec": "validate_execution_spec",
            "execute_tools": "execute_tools",
            "repair_or_clarify": "repair_or_clarify",
            "done": END,
        },
    )
    graph.add_conditional_edges(
        "validate_execution_spec",
        _next_step,
        {
            "execute_tools": "execute_tools",
            "repair_or_clarify": "repair_or_clarify",
            "done": END,
        },
    )
    graph.add_conditional_edges(
        "execute_tools",
        _next_step,
        {
            "validate_result": "validate_result",
            "repair_or_clarify": "repair_or_clarify",
            "done": END,
        },
    )
    graph.add_conditional_edges(
        "validate_result",
        _next_step,
        {
            "compose_answer": "compose_answer",
            "repair_or_clarify": "repair_or_clarify",
            "emit_debug_trace": "emit_debug_trace",
            "done": END,
        },
    )
    graph.add_conditional_edges(
        "repair_or_clarify",
        _next_step,
        {
            "build_plan": "build_plan",
            "execute_tools": "execute_tools",
            "compose_answer": "compose_answer",
            "done": END,
        },
    )
    graph.add_edge("compose_answer", "emit_debug_trace")
    graph.add_conditional_edges(
        "emit_debug_trace",
        _next_step,
        {"done": END},
    )

    return graph.compile()

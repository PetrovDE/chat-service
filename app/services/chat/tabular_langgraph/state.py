from __future__ import annotations

from typing import Any, Dict, List, NotRequired, Optional, TypedDict


class TabularLangGraphState(TypedDict):
    query: str
    files: List[Any]
    graph_run_id: NotRequired[str]
    graph_started_at_ms: NotRequired[int]
    graph_stop_reason: NotRequired[str]

    parsed_route: NotRequired[str]
    scope_status: NotRequired[str]
    scope_debug_fields: NotRequired[Dict[str, Any]]

    target_file: NotRequired[Any]
    dataset: NotRequired[Any]
    table: NotRequired[Any]
    intent_decision: NotRequired[Any]
    selected_route: NotRequired[str]
    intent_kind: NotRequired[Optional[str]]

    guarded_enabled: NotRequired[bool]
    max_attempts: NotRequired[int]
    repair_iteration_index: NotRequired[int]
    repair_iteration_count: NotRequired[int]
    plan_feedback: NotRequired[List[str]]
    execution_feedback: NotRequired[List[str]]

    raw_plan: NotRequired[Dict[str, Any]]
    validated_plan: NotRequired[Dict[str, Any]]
    plan_hash: NotRequired[str]
    plan_summary: NotRequired[Dict[str, Any]]
    plan_validation_failures: NotRequired[List[str]]
    raw_execution_spec: NotRequired[Dict[str, Any]]
    execution_spec: NotRequired[Dict[str, Any]]
    execution_spec_summary: NotRequired[Dict[str, Any]]
    execution_spec_validation_failures: NotRequired[List[str]]
    guarded_sql: NotRequired[str]
    count_sql: NotRequired[str]
    guard_debug: NotRequired[Dict[str, Any]]
    executed_tools: NotRequired[List[str]]
    tool_errors: NotRequired[List[str]]

    retry_reason: NotRequired[str]
    retry_next: NotRequired[bool]
    skip_guarded_plan: NotRequired[bool]

    payload: NotRequired[Optional[Dict[str, Any]]]
    terminal_none_payload: NotRequired[bool]

    node_trace: NotRequired[List[Dict[str, Any]]]
    next_step: NotRequired[str]

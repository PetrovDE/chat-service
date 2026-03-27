from types import SimpleNamespace

from app.services.chat.tabular_langgraph import nodes


def test_detect_intent_sets_route_and_next_step():
    result = nodes.detect_intent({"query": "count rows", "files": []})

    assert result["next_step"] == "resolve_scope"
    assert isinstance(result.get("parsed_route"), str)


def test_resolve_scope_ambiguous_file_moves_to_compose(monkeypatch):
    scope = SimpleNamespace(
        status="ambiguous_file",
        debug_fields={"scope_selection_status": "ambiguous_file"},
        clarification_options=["a.csv", "b.csv"],
        target_file=None,
        dataset=None,
        table=None,
    )

    monkeypatch.setattr("app.services.chat.tabular_langgraph.tool_adapters.resolve_scope", lambda **kwargs: scope)
    monkeypatch.setattr(
        "app.services.chat.tabular_langgraph.tool_adapters.build_scope_clarification_payload",
        lambda **kwargs: {"status": "error", "debug": {"selected_route": "ambiguous_data_scope"}},
    )

    result = nodes.resolve_scope({"query": "count rows", "files": [], "parsed_route": "aggregation"})

    assert result["next_step"] == "compose_answer"
    assert isinstance(result.get("payload"), dict)
    assert result.get("graph_stop_reason") == "ambiguous_file_scope"


def test_inspect_data_sources_routes_to_execute_tools_when_guarded_disabled(monkeypatch):
    decision = SimpleNamespace(
        selected_route="aggregation",
        legacy_intent="aggregate",
    )

    monkeypatch.setattr(
        "app.services.chat.tabular_langgraph.tool_adapters.parse_and_classify",
        lambda **kwargs: ("aggregation", decision),
    )
    monkeypatch.setattr(
        "app.services.chat.tabular_langgraph.tool_adapters.is_guarded_candidate",
        lambda **kwargs: True,
    )
    monkeypatch.setattr(
        "app.services.chat.tabular_langgraph.nodes_intent_scope.settings.TABULAR_LLM_GUARDED_PLANNER_ENABLED",
        False,
    )

    result = nodes.inspect_data_sources(
        {
            "query": "count rows",
            "files": [],
            "target_file": object(),
            "dataset": object(),
            "table": object(),
        }
    )

    assert result["next_step"] == "execute_tools"
    assert result["guarded_enabled"] is False


def test_build_plan_invalid_json_uses_semantic_fallback(monkeypatch):
    async def _bad_plan(**kwargs):  # noqa: ANN003
        _ = kwargs
        return None, "invalid_json"

    monkeypatch.setattr("app.services.chat.tabular_langgraph.tool_adapters.call_plan_llm", _bad_plan)

    result = nodes.build_plan(
        {
            "query": "count rows",
            "skip_guarded_plan": False,
            "table": object(),
            "plan_feedback": [],
        }
    )

    # nodes.build_plan is async
    import asyncio

    output = asyncio.run(result)
    assert output["next_step"] == "validate_plan"
    assert isinstance(output.get("semantic_plan_hint"), dict)


def test_emit_debug_trace_adds_graph_and_planner_visibility_fields():
    state = {
        "payload": {"status": "ok", "debug": {"tabular_sql": {}, "planner_mode": "llm_guarded"}},
        "graph_run_id": "graph-run-1",
        "graph_stop_reason": "payload_ready",
        "repair_iteration_index": 1,
        "repair_iteration_count": 2,
        "guarded_enabled": True,
        "skip_guarded_plan": False,
        "node_trace": [
            {"node": "detect_intent", "status": "ok", "reason": "none"},
            {"node": "execute_tools", "status": "ok", "reason": "guarded_success"},
        ],
        "plan_hash": "abc123",
        "plan_summary": {"selected_route": "aggregation"},
        "execution_spec_summary": {"selected_route": "aggregation"},
        "plan_validation_failures": [],
        "execution_spec_validation_failures": [],
        "executed_tools": ["execute_guarded_sql"],
        "tool_errors": [],
    }

    output = nodes.emit_debug_trace(state)
    payload = output["payload"]
    debug = payload.get("debug") or {}

    assert debug.get("analytics_engine_graph_run_id") == "graph-run-1"
    assert debug.get("analytics_engine_graph_attempts") == 2
    assert debug.get("analytics_engine_graph_stop_reason") == "payload_ready"
    assert debug.get("graph_node_path") == ["detect_intent", "execute_tools"]
    assert debug.get("plan_hash") == "abc123"
    assert debug.get("plan_summary") == {"selected_route": "aggregation"}
    assert debug.get("execution_spec_summary") == {"selected_route": "aggregation"}
    assert debug.get("executed_tools") == ["execute_guarded_sql"]

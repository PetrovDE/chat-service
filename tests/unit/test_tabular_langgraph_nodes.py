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


def test_build_plan_invalid_json_schedules_repair(monkeypatch):
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
    assert output["next_step"] == "repair_or_clarify"
    assert output["retry_next"] is True

import asyncio

from app.services.chat import tabular_sql


def _payload(route: str):
    return {
        "status": "ok",
        "debug": {
            "selected_route": route,
            "tabular_sql": {},
        },
    }


def test_execute_tabular_sql_path_uses_langgraph_runtime_only(monkeypatch):
    calls = {"langgraph": 0}

    async def _langgraph(**kwargs):  # noqa: ANN003
        _ = kwargs
        calls["langgraph"] += 1
        return _payload("langgraph")

    monkeypatch.setattr("app.services.chat.tabular_langgraph.execute_tabular_langgraph_path", _langgraph)

    result = asyncio.run(tabular_sql.execute_tabular_sql_path(query="count rows", files=[]))

    assert isinstance(result, dict)
    assert calls == {"langgraph": 1}
    debug = result.get("debug") or {}
    assert debug.get("analytics_engine_mode_requested") == "langgraph"
    assert debug.get("analytics_engine_mode_served") == "langgraph"
    assert debug.get("analytics_engine_fallback_reason") == "none"
    assert debug.get("analytics_engine_shadow_enabled") is False
    assert debug.get("analytics_engine_rollback_mode_used") is False
    assert debug.get("analytics_engine_legacy_activation_reason") == "none"
    assert debug.get("engine_mode_requested") == "langgraph"
    assert debug.get("engine_mode_served") == "langgraph"
    assert debug.get("engine_fallback_reason") == "none"
    assert debug.get("rollback_mode_used") is False
    assert debug.get("legacy_activation_reason") == "none"


def test_execute_tabular_sql_path_returns_explicit_runtime_error_when_langgraph_raises(monkeypatch):
    async def _langgraph(**kwargs):  # noqa: ANN003
        _ = kwargs
        raise RuntimeError("langgraph failure")

    monkeypatch.setattr("app.services.chat.tabular_langgraph.execute_tabular_langgraph_path", _langgraph)

    result = asyncio.run(tabular_sql.execute_tabular_sql_path(query="count rows", files=[]))

    assert isinstance(result, dict)
    assert str(result.get("status") or "") == "error"
    assert str(result.get("clarification_prompt") or "").strip()
    debug = result.get("debug") or {}
    assert debug.get("analytics_engine_mode_requested") == "langgraph"
    assert debug.get("analytics_engine_mode_served") == "langgraph"
    assert debug.get("analytics_engine_fallback_reason") == "langgraph_runtime_exception"
    assert debug.get("analytics_engine_rollback_mode_used") is False
    assert debug.get("analytics_engine_legacy_activation_reason") == "none"
    assert debug.get("engine_fallback_reason") == "langgraph_runtime_exception"
    assert debug.get("fallback_reason") == "langgraph_runtime_exception"
    assert debug.get("executor_error_code") == "langgraph_runtime_exception"


def test_execute_tabular_sql_path_preserves_none_payload(monkeypatch):
    async def _langgraph(**kwargs):  # noqa: ANN003
        _ = kwargs
        return None

    monkeypatch.setattr("app.services.chat.tabular_langgraph.execute_tabular_langgraph_path", _langgraph)

    result = asyncio.run(tabular_sql.execute_tabular_sql_path(query="count rows", files=[]))

    assert result is None

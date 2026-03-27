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


def test_execute_tabular_sql_path_routes_to_legacy_mode(monkeypatch):
    calls = {"legacy": 0, "langgraph": 0}

    async def _legacy(**kwargs):  # noqa: ANN003
        _ = kwargs
        calls["legacy"] += 1
        return _payload("legacy")

    async def _langgraph(**kwargs):  # noqa: ANN003
        _ = kwargs
        calls["langgraph"] += 1
        return _payload("langgraph")

    monkeypatch.setattr(tabular_sql, "_execute_tabular_sql_path_legacy", _legacy)
    monkeypatch.setattr("app.services.chat.tabular_langgraph.execute_tabular_langgraph_path", _langgraph)
    monkeypatch.setattr(tabular_sql.settings, "ANALYTICS_ENGINE_MODE", "legacy")
    monkeypatch.setattr(tabular_sql.settings, "ANALYTICS_ENGINE_SHADOW", False)

    result = asyncio.run(tabular_sql.execute_tabular_sql_path(query="count rows", files=[]))

    assert isinstance(result, dict)
    assert calls == {"legacy": 1, "langgraph": 0}
    debug = result.get("debug") or {}
    assert debug.get("analytics_engine_mode_served") == "legacy"


def test_execute_tabular_sql_path_routes_to_langgraph_mode(monkeypatch):
    calls = {"legacy": 0, "langgraph": 0}

    async def _legacy(**kwargs):  # noqa: ANN003
        _ = kwargs
        calls["legacy"] += 1
        return _payload("legacy")

    async def _langgraph(**kwargs):  # noqa: ANN003
        _ = kwargs
        calls["langgraph"] += 1
        return _payload("langgraph")

    monkeypatch.setattr(tabular_sql, "_execute_tabular_sql_path_legacy", _legacy)
    monkeypatch.setattr("app.services.chat.tabular_langgraph.execute_tabular_langgraph_path", _langgraph)
    monkeypatch.setattr(tabular_sql.settings, "ANALYTICS_ENGINE_MODE", "langgraph")
    monkeypatch.setattr(tabular_sql.settings, "ANALYTICS_ENGINE_SHADOW", False)

    result = asyncio.run(tabular_sql.execute_tabular_sql_path(query="count rows", files=[]))

    assert isinstance(result, dict)
    assert calls == {"legacy": 0, "langgraph": 1}
    debug = result.get("debug") or {}
    assert debug.get("analytics_engine_mode_served") == "langgraph"


def test_execute_tabular_sql_path_langgraph_fallback_to_legacy(monkeypatch):
    calls = {"legacy": 0, "langgraph": 0}

    async def _legacy(**kwargs):  # noqa: ANN003
        _ = kwargs
        calls["legacy"] += 1
        return _payload("legacy")

    async def _langgraph(**kwargs):  # noqa: ANN003
        _ = kwargs
        calls["langgraph"] += 1
        raise RuntimeError("langgraph failure")

    monkeypatch.setattr(tabular_sql, "_execute_tabular_sql_path_legacy", _legacy)
    monkeypatch.setattr("app.services.chat.tabular_langgraph.execute_tabular_langgraph_path", _langgraph)
    monkeypatch.setattr(tabular_sql.settings, "ANALYTICS_ENGINE_MODE", "langgraph")
    monkeypatch.setattr(tabular_sql.settings, "ANALYTICS_ENGINE_SHADOW", False)

    result = asyncio.run(tabular_sql.execute_tabular_sql_path(query="count rows", files=[]))

    assert isinstance(result, dict)
    assert calls == {"legacy": 1, "langgraph": 1}
    debug = result.get("debug") or {}
    assert debug.get("analytics_engine_mode_served") == "legacy"
    assert debug.get("analytics_engine_fallback_reason") == "langgraph_exception"

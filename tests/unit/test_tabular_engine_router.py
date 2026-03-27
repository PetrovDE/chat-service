import asyncio

from app.services.chat import tabular_engine_router as router


def _payload(*, selected_route: str = "aggregation"):
    return {
        "status": "ok",
        "debug": {
            "selected_route": selected_route,
            "tabular_sql": {},
        },
    }


async def _legacy_executor(**kwargs):  # noqa: ANN003
    _ = kwargs
    return _payload(selected_route="legacy_route")


async def _langgraph_executor(**kwargs):  # noqa: ANN003
    _ = kwargs
    return _payload(selected_route="langgraph_route")


def test_engine_router_serves_legacy_mode(monkeypatch):
    monkeypatch.setattr(router.settings, "ANALYTICS_ENGINE_MODE", "legacy")
    monkeypatch.setattr(router.settings, "ANALYTICS_ENGINE_SHADOW", False)

    result = asyncio.run(
        router.execute_tabular_engine_route(
            query="count rows",
            files=[],
            legacy_executor=_legacy_executor,
            langgraph_executor=_langgraph_executor,
        )
    )

    assert isinstance(result, dict)
    debug = result.get("debug") or {}
    assert debug.get("analytics_engine_mode_requested") == "legacy"
    assert debug.get("analytics_engine_mode_served") == "legacy"
    assert debug.get("analytics_engine_fallback_reason") == "none"


def test_engine_router_serves_langgraph_mode(monkeypatch):
    monkeypatch.setattr(router.settings, "ANALYTICS_ENGINE_MODE", "langgraph")
    monkeypatch.setattr(router.settings, "ANALYTICS_ENGINE_SHADOW", False)

    result = asyncio.run(
        router.execute_tabular_engine_route(
            query="count rows",
            files=[],
            legacy_executor=_legacy_executor,
            langgraph_executor=_langgraph_executor,
        )
    )

    assert isinstance(result, dict)
    debug = result.get("debug") or {}
    assert debug.get("analytics_engine_mode_requested") == "langgraph"
    assert debug.get("analytics_engine_mode_served") == "langgraph"
    assert debug.get("analytics_engine_fallback_reason") == "none"


def test_engine_router_falls_back_to_legacy_when_langgraph_raises(monkeypatch):
    monkeypatch.setattr(router.settings, "ANALYTICS_ENGINE_MODE", "langgraph")
    monkeypatch.setattr(router.settings, "ANALYTICS_ENGINE_SHADOW", False)

    async def _failing_langgraph(**kwargs):  # noqa: ANN003
        _ = kwargs
        raise RuntimeError("boom")

    result = asyncio.run(
        router.execute_tabular_engine_route(
            query="count rows",
            files=[],
            legacy_executor=_legacy_executor,
            langgraph_executor=_failing_langgraph,
        )
    )

    assert isinstance(result, dict)
    debug = result.get("debug") or {}
    assert debug.get("analytics_engine_mode_requested") == "langgraph"
    assert debug.get("analytics_engine_mode_served") == "legacy"
    assert debug.get("analytics_engine_fallback_reason") == "langgraph_exception"


def test_engine_router_populates_shadow_summary(monkeypatch):
    monkeypatch.setattr(router.settings, "ANALYTICS_ENGINE_MODE", "legacy")
    monkeypatch.setattr(router.settings, "ANALYTICS_ENGINE_SHADOW", True)

    result = asyncio.run(
        router.execute_tabular_engine_route(
            query="count rows",
            files=[],
            legacy_executor=_legacy_executor,
            langgraph_executor=_langgraph_executor,
        )
    )

    assert isinstance(result, dict)
    debug = result.get("debug") or {}
    shadow = debug.get("analytics_engine_shadow") if isinstance(debug.get("analytics_engine_shadow"), dict) else {}
    assert debug.get("analytics_engine_shadow_enabled") is True
    assert shadow.get("mode") == "langgraph"
    assert shadow.get("error") is None

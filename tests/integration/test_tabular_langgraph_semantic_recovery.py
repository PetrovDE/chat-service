from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.chat import tabular_llm_guarded_planner as guarded_planner
from app.services.chat.tabular_sql import execute_tabular_sql_path
from app.services.tabular.storage_adapter import SharedDuckDBParquetStorageAdapter


RU_MONTH_SUM_CREATED_AT = (
    "\u043f\u043e\u0441\u0442\u0440\u043e\u0439 \u0433\u0440\u0430\u0444\u0438\u043a \u0441\u0443\u043c\u043c\u044b amount_rub "
    "\u043f\u043e \u043c\u0435\u0441\u044f\u0446\u0430\u043c \u043d\u0430 \u043e\u0441\u043d\u043e\u0432\u0435 created_at"
)
RU_SUM_BY_CITY = "\u043f\u043e\u043a\u0430\u0436\u0438 \u0441\u0443\u043c\u043c\u0443 amount_rub \u043f\u043e city"
RU_COUNT_BY_MONTH = (
    "\u043f\u043e\u0441\u0442\u0440\u043e\u0439 \u0433\u0440\u0430\u0444\u0438\u043a "
    "\u043a\u043e\u043b\u0438\u0447\u0435\u0441\u0442\u0432\u0430 \u0437\u0430\u043f\u0440\u043e\u0441\u043e\u0432 "
    "\u043f\u043e \u043c\u0435\u0441\u044f\u0446\u0430\u043c"
)
RU_SPEND_VOLUME_BY_MONTH = (
    "\u043f\u043e\u0441\u0442\u0440\u043e\u0439 \u0433\u0440\u0430\u0444\u0438\u043a "
    "\u043e\u0431\u044a\u0435\u043c\u0430 \u0437\u0430\u0442\u0440\u0430\u0442 \u043f\u043e \u043c\u0435\u0441\u044f\u0446\u0430\u043c"
)
RU_AMBIGUOUS_MONTH_SUM = (
    "\u043f\u043e\u0441\u0442\u0440\u043e\u0439 \u0433\u0440\u0430\u0444\u0438\u043a "
    "\u0441\u0443\u043c\u043c\u044b \u043f\u043e \u043c\u0435\u0441\u044f\u0446\u0430\u043c"
)


def _write_csv(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows), encoding="utf-8")


def _build_file(tmp_path: Path, *, rows: list[str], file_id: str) -> SimpleNamespace:
    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / f"{file_id}.csv"
    _write_csv(csv_path, rows)
    dataset = adapter.ingest(
        file_id=file_id,
        file_path=csv_path,
        file_type="csv",
        source_filename=f"{file_id}.csv",
    )
    assert dataset is not None
    return SimpleNamespace(
        id=file_id,
        extension="csv",
        file_type="csv",
        chunks_count=24,
        original_filename=f"{file_id}.csv",
        custom_metadata={"tabular_dataset": dataset},
    )


def _enable_langgraph_guarded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.services.chat.tabular_sql.settings.ANALYTICS_ENGINE_MODE", "langgraph")
    monkeypatch.setattr("app.services.chat.tabular_sql.settings.ANALYTICS_ENGINE_SHADOW", False)
    monkeypatch.setattr("app.services.chat.tabular_sql.settings.TABULAR_LLM_GUARDED_PLANNER_ENABLED", True)
    monkeypatch.setattr(guarded_planner.settings, "TABULAR_LLM_GUARDED_PLANNER_ENABLED", True)
    monkeypatch.setattr(guarded_planner.settings, "TABULAR_LLM_GUARDED_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(guarded_planner.settings, "TABULAR_LLM_GUARDED_PLAN_TIMEOUT_SECONDS", 3.0)
    monkeypatch.setattr(guarded_planner.settings, "TABULAR_LLM_GUARDED_EXECUTION_TIMEOUT_SECONDS", 3.0)
    monkeypatch.setattr(guarded_planner.settings, "TABULAR_LLM_GUARDED_PLAN_MAX_TOKENS", 512)
    monkeypatch.setattr(guarded_planner.settings, "TABULAR_LLM_GUARDED_EXECUTION_MAX_TOKENS", 512)


def _mock_chart_delivery(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_chart_delivery(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {
            "chart_rendered": True,
            "chart_artifact_available": True,
            "chart_artifact_exists": True,
            "chart_fallback_reason": "none",
            "chart_artifact_path": "tabular_sql/run/chart.png",
            "chart_artifact_id": "chart-1",
            "artifact": {
                "kind": "tabular_chart",
                "name": "chart.png",
                "path": "tabular_sql/run/chart.png",
                "url": "/uploads/tabular_sql/run/chart.png",
                "content_type": "image/png",
            },
        }

    monkeypatch.setattr(guarded_planner, "render_chart_artifact", _fake_chart_delivery)


def _llm_payload(payload: dict) -> dict:
    return {
        "response": json.dumps(payload, ensure_ascii=False),
        "model": "llama-test",
        "model_route": "ollama",
        "route_mode": "explicit",
        "provider_selected": "local",
        "provider_effective": "ollama",
        "fallback_reason": "none",
        "fallback_allowed": False,
        "fallback_attempted": False,
        "fallback_policy_version": "p1-aihub-first-v1",
        "aihub_attempted": False,
        "tokens_used": 64,
    }


def _mock_sloppy_llm_outputs(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_generate_response(**kwargs):  # noqa: ANN003
        policy_class = str(kwargs.get("policy_class") or "")
        if policy_class == "tabular_llm_guarded_plan":
            return _llm_payload(
                {
                    "task_type": "aggregate",
                    "requested_output_type": "table",
                    "source_scope": {"table_name": "unknown", "sheet_name": "unknown"},
                    "measures": [{"requested": "spending volume", "field": "spending volume", "aggregation": "sum"}],
                    "dimensions": [{"requested": "by months", "field": "by months"}],
                    "derived_time_grain": "none",
                    "source_datetime_field": None,
                    "filters": [],
                    "chart_type": "none",
                    "confidence": 0.01,
                    "ambiguity_flags": ["none"],
                }
            )
        if policy_class == "tabular_llm_guarded_execution":
            return _llm_payload(
                {
                    "selected_route": "comparison",
                    "requested_output_type": "table",
                    "measure": {"field": "spending volume", "aggregation": "sum"},
                    "dimension": {"field": "by months"},
                    "derived_time_grain": "none",
                    "source_datetime_field": None,
                    "filters": [{"field": "status", "operator": "eq", "value": "new"}],
                    "chart_type": "line",
                    "output_columns": ["group_key", "sum_amount"],
                }
            )
        raise AssertionError(f"Unexpected policy class: {policy_class}")

    monkeypatch.setattr(guarded_planner.llm_manager, "generate_response", _fake_generate_response)


@pytest.mark.parametrize(
    "query_text,expected_route,expected_aggregation,expected_time_grain",
    [
        (RU_MONTH_SUM_CREATED_AT, "chart", "sum", "month"),
        (RU_SUM_BY_CITY, "aggregation", "sum", "none"),
        (RU_COUNT_BY_MONTH, "chart", "count", "month"),
        (RU_SPEND_VOLUME_BY_MONTH, "chart", "sum", "month"),
    ],
)
def test_langgraph_semantic_recovery_for_common_ru_analytics_queries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    query_text: str,
    expected_route: str,
    expected_aggregation: str,
    expected_time_grain: str,
) -> None:
    pytest.importorskip("duckdb")
    _enable_langgraph_guarded(monkeypatch)
    _mock_chart_delivery(monkeypatch)
    _mock_sloppy_llm_outputs(monkeypatch)
    file_obj = _build_file(
        tmp_path,
        file_id="semantic-recovery-main",
        rows=[
            "request_id,created_at,amount_rub,city,status",
            "1,2026-01-05 10:00:00,100,ekb,new",
            "2,2026-01-15 12:00:00,150,msk,new",
            "3,2026-02-03 09:00:00,240,ekb,approved",
            "4,2026-02-20 18:30:00,180,spb,approved",
            "5,2026-03-08 11:15:00,300,msk,new",
            "6,2026-03-18 14:45:00,120,spb,approved",
        ],
    )

    result = asyncio.run(execute_tabular_sql_path(query=query_text, files=[file_obj]))

    assert isinstance(result, dict)
    assert str(result.get("status") or "") == "ok"
    debug = result.get("debug") or {}
    assert debug.get("analytics_engine_mode_served") == "langgraph"
    assert debug.get("planner_mode") == "llm_guarded"
    assert debug.get("selected_route") == expected_route
    assert debug.get("analytics_engine_graph_stop_reason") == "payload_ready"
    assert debug.get("plan_validation_status") == "success"
    assert debug.get("sql_validation_status") == "success"
    assert debug.get("fallback_reason") == "none"
    assert "selected_route_plan_mismatch" not in list(debug.get("execution_spec_validation_failures") or [])
    assert "missing_value_output_column" not in list(debug.get("execution_spec_validation_failures") or [])

    plan_json = debug.get("analytic_plan_json") if isinstance(debug.get("analytic_plan_json"), dict) else {}
    measure = (list(plan_json.get("measures") or [{}]) or [{}])[0]
    assert str(measure.get("aggregation") or "") == expected_aggregation
    assert str(plan_json.get("derived_time_grain") or "none") == expected_time_grain
    if expected_time_grain == "month":
        assert str(debug.get("source_datetime_field") or "").strip() == "created_at"


def test_langgraph_semantic_recovery_returns_specific_clarification_for_ambiguous_monthly_query(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pytest.importorskip("duckdb")
    _enable_langgraph_guarded(monkeypatch)
    _mock_chart_delivery(monkeypatch)
    _mock_sloppy_llm_outputs(monkeypatch)
    file_obj = _build_file(
        tmp_path,
        file_id="semantic-recovery-ambiguous",
        rows=[
            "request_id,first_date,second_date,amount_rub,status",
            "1,2026-01-05 10:00:00,2026-01-06 09:00:00,100,new",
            "2,2026-01-15 12:00:00,2026-01-16 18:00:00,150,new",
            "3,2026-02-03 09:00:00,2026-02-04 11:00:00,240,approved",
            "4,2026-02-20 18:30:00,2026-02-21 13:20:00,180,approved",
        ],
    )

    result = asyncio.run(execute_tabular_sql_path(query=RU_AMBIGUOUS_MONTH_SUM, files=[file_obj]))

    assert isinstance(result, dict)
    assert str(result.get("status") or "") == "error"
    clarification_prompt = str(result.get("clarification_prompt") or "")
    debug = result.get("debug") or {}
    assert debug.get("analytics_engine_mode_served") == "langgraph"
    assert debug.get("clarification_reason_code") == "ambiguous_date_grain_or_source"
    assert "\u043a\u043e\u043b\u043e\u043d\u043a\u0443 \u0434\u0430\u0442\u044b" in clarification_prompt
    assert "metric column and grouping scope" not in clarification_prompt
    assert "source_datetime" in str(debug.get("analytics_engine_graph_stop_reason") or "") or "datetime" in str(
        debug.get("analytics_engine_graph_stop_reason") or ""
    )

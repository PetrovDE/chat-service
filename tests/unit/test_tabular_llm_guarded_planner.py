import asyncio
from types import SimpleNamespace

from app.services.chat import tabular_llm_guarded_planner as planner
from app.services.tabular.sql_execution import ResolvedTabularDataset, ResolvedTabularTable


def _dataset_and_table():
    table = ResolvedTabularTable(
        table_name="requests",
        sheet_name="Sheet1",
        row_count=120,
        columns=["created_at", "amount_total", "city", "status"],
        column_aliases={"amount_total": "Amount Total"},
        table_version=1,
        provenance_id="tbl-1",
        parquet_path=None,
        column_metadata={
            "created_at": {"dtype": "timestamp"},
            "amount_total": {"dtype": "double"},
            "city": {"dtype": "varchar"},
            "status": {"dtype": "varchar"},
        },
    )
    dataset = ResolvedTabularDataset(
        engine="duckdb_parquet",
        dataset_id="ds-1",
        dataset_version=1,
        dataset_provenance_id="prov-1",
        tables=[table],
        catalog_path=None,
    )
    return dataset, table


def _target_file():
    return SimpleNamespace(
        id="file-1",
        original_filename="requests.xlsx",
    )


def _enable_guarded(monkeypatch, *, max_attempts=3):
    monkeypatch.setattr(planner.settings, "TABULAR_LLM_GUARDED_PLANNER_ENABLED", True)
    monkeypatch.setattr(planner.settings, "TABULAR_LLM_GUARDED_MAX_ATTEMPTS", max_attempts)
    monkeypatch.setattr(planner.settings, "TABULAR_LLM_GUARDED_PLAN_TIMEOUT_SECONDS", 2.0)
    monkeypatch.setattr(planner.settings, "TABULAR_LLM_GUARDED_EXECUTION_TIMEOUT_SECONDS", 2.0)
    monkeypatch.setattr(planner.settings, "TABULAR_LLM_GUARDED_PLAN_MAX_TOKENS", 256)
    monkeypatch.setattr(planner.settings, "TABULAR_LLM_GUARDED_EXECUTION_MAX_TOKENS", 256)


def test_llm_guarded_planning_path_for_spending_by_month(monkeypatch):
    _enable_guarded(monkeypatch, max_attempts=3)
    dataset, table = _dataset_and_table()
    file_obj = _target_file()

    async def _fake_call_llm_json(*, policy_class, **kwargs):  # noqa: ANN003
        if policy_class == "tabular_llm_guarded_plan":
            return (
                {
                    "task_type": "trend",
                    "requested_output_type": "chart",
                    "source_scope": {"table_name": "requests", "sheet_name": "Sheet1"},
                    "measures": [{"requested": "spending", "field": "amount_total", "aggregation": "sum"}],
                    "dimensions": [{"requested": "month", "field": "created_at"}],
                    "derived_time_grain": "month",
                    "source_datetime_field": "created_at",
                    "filters": [],
                    "chart_type": "line",
                    "confidence": 0.92,
                    "ambiguity_flags": ["none"],
                },
                "success",
            )
        return (
            {
                "selected_route": "trend",
                "requested_output_type": "chart",
                "measure": {"field": "amount_total", "aggregation": "sum"},
                "dimension": {"field": "created_at"},
                "derived_time_grain": "month",
                "source_datetime_field": "created_at",
                "filters": [],
                "chart_type": "line",
                "output_columns": ["bucket", "value"],
            },
            "success",
        )

    def _fake_execute_sql(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {
            "rows": [("2026-01", 10.0), ("2026-02", 12.0)],
            "rows_effective": 2,
        }

    def _fake_render_chart_artifact(**kwargs):  # noqa: ANN003
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
                "column": "created_at",
            },
        }

    monkeypatch.setattr(planner, "_call_llm_json", _fake_call_llm_json)
    monkeypatch.setattr(planner, "_execute_sql", _fake_execute_sql)
    monkeypatch.setattr(planner, "render_chart_artifact", _fake_render_chart_artifact)

    result = asyncio.run(
        planner.maybe_execute_llm_guarded_tabular(
            query="show spending by month",
            parsed_query_route="trend",
            selected_route="trend",
            dataset=dataset,
            table=table,
            target_file=file_obj,
        )
    )

    assert isinstance(result, dict)
    assert result["status"] == "ok"
    debug = result["debug"]
    assert debug["planner_mode"] == "llm_guarded"
    assert debug["plan_validation_status"] == "success"
    assert debug["sql_validation_status"] == "success"
    assert debug["post_execution_validation_status"] == "success"
    assert debug["selected_route"] == "trend"
    assert debug["requested_time_grain"] == "month"
    assert debug["source_datetime_field"] == "created_at"


def test_no_literal_month_column_required_when_datetime_source_exists():
    _, table = _dataset_and_table()
    plan = {
        "task_type": "trend",
        "requested_output_type": "table",
        "source_scope": {"table_name": "requests", "sheet_name": "Sheet1"},
        "measures": [{"requested": "spending", "field": "amount_total", "aggregation": "sum"}],
        "dimensions": [{"requested": "month", "field": "created_at"}],
        "derived_time_grain": "month",
        "source_datetime_field": "created_at",
        "filters": [],
        "chart_type": "none",
        "confidence": 0.9,
        "ambiguity_flags": ["none"],
    }

    validation = planner._validate_plan(plan=plan, table=table)
    assert validation.status == "success"
    assert validation.payload["source_datetime_field"] == "created_at"
    assert validation.payload["derived_time_grain"] == "month"


def test_plan_validation_catches_invalid_field_reference():
    _, table = _dataset_and_table()
    plan = {
        "task_type": "aggregate",
        "requested_output_type": "table",
        "source_scope": {"table_name": "requests", "sheet_name": "Sheet1"},
        "measures": [{"requested": "spending", "field": "missing_metric", "aggregation": "sum"}],
        "dimensions": [{"requested": "city", "field": "city"}],
        "derived_time_grain": "none",
        "source_datetime_field": None,
        "filters": [],
        "chart_type": "none",
        "confidence": 0.9,
        "ambiguity_flags": ["none"],
    }

    validation = planner._validate_plan(plan=plan, table=table)
    assert validation.status == "failed"
    assert any(item.startswith("measure_field_invalid") for item in list(validation.errors))


def test_sql_code_validation_catches_plan_mismatch():
    validated_plan = {
        "task_type": "aggregate",
        "requested_output_type": "table",
        "source_scope": {"table_name": "requests", "sheet_name": "Sheet1"},
        "measures": [{"requested": "spending", "field": "amount_total", "aggregation": "sum"}],
        "dimensions": [{"requested": "city", "field": "city"}],
        "derived_time_grain": "none",
        "source_datetime_field": None,
        "filters": [],
        "chart_type": "none",
        "confidence": 0.9,
        "ambiguity_flags": ["none"],
    }
    execution_spec = {
        "selected_route": "aggregation",
        "requested_output_type": "table",
        "measure": {"field": "status", "aggregation": "sum"},
        "dimension": {"field": "city"},
        "derived_time_grain": "none",
        "source_datetime_field": None,
        "filters": [],
        "chart_type": "none",
        "output_columns": ["group_key", "value"],
    }

    validation = planner._validate_execution_spec(
        execution_spec=execution_spec,
        validated_plan=validated_plan,
    )
    assert validation.status == "failed"
    assert "measure_field_plan_mismatch" in list(validation.errors)


def test_retry_loop_stops_at_configured_limit(monkeypatch):
    _enable_guarded(monkeypatch, max_attempts=2)
    dataset, table = _dataset_and_table()
    file_obj = _target_file()
    call_count = {"count": 0}

    async def _always_bad_plan(**kwargs):  # noqa: ANN003
        call_count["count"] += 1
        return ({"task_type": "aggregate", "requested_output_type": "table"}, "success")

    monkeypatch.setattr(planner, "_call_llm_json", _always_bad_plan)

    result = asyncio.run(
        planner.maybe_execute_llm_guarded_tabular(
            query="show spending by month",
            parsed_query_route="trend",
            selected_route="unsupported_missing_column",
            dataset=dataset,
            table=table,
            target_file=file_obj,
        )
    )

    assert isinstance(result, dict)
    assert result["status"] == "error"
    assert result["debug"]["repair_iteration_count"] == 2
    assert result["debug"]["repair_iteration_index"] == 2
    assert call_count["count"] == 2


def test_clarification_is_returned_after_repeated_validation_failures(monkeypatch):
    _enable_guarded(monkeypatch, max_attempts=2)
    dataset, table = _dataset_and_table()
    file_obj = _target_file()

    async def _always_bad_plan(**kwargs):  # noqa: ANN003
        return ({"task_type": "aggregate", "requested_output_type": "table"}, "success")

    monkeypatch.setattr(planner, "_call_llm_json", _always_bad_plan)

    result = asyncio.run(
        planner.maybe_execute_llm_guarded_tabular(
            query="show spending by month",
            parsed_query_route="trend",
            selected_route="unsupported_missing_column",
            dataset=dataset,
            table=table,
            target_file=file_obj,
        )
    )

    assert isinstance(result, dict)
    assert result["status"] == "error"
    assert isinstance(result.get("clarification_prompt"), str) and result["clarification_prompt"]
    assert result["debug"]["clarification_triggered_after_retries"] is True


def test_retry_loop_is_bounded_and_non_recursive(monkeypatch):
    _enable_guarded(monkeypatch, max_attempts=3)
    dataset, table = _dataset_and_table()
    file_obj = _target_file()
    call_count = {"count": 0}

    async def _always_bad_plan(**kwargs):  # noqa: ANN003
        call_count["count"] += 1
        return ({"task_type": "aggregate", "requested_output_type": "table"}, "success")

    monkeypatch.setattr(planner, "_call_llm_json", _always_bad_plan)

    result = asyncio.run(
        planner.maybe_execute_llm_guarded_tabular(
            query="show spending by month",
            parsed_query_route="trend",
            selected_route="unsupported_missing_column",
            dataset=dataset,
            table=table,
            target_file=file_obj,
        )
    )

    assert isinstance(result, dict)
    assert result["status"] == "error"
    assert call_count["count"] == 3


def test_chart_artifact_honesty_remains_explicit_when_delivery_fails(monkeypatch):
    _enable_guarded(monkeypatch, max_attempts=2)
    dataset, table = _dataset_and_table()
    file_obj = _target_file()

    async def _fake_call_llm_json(*, policy_class, **kwargs):  # noqa: ANN003
        if policy_class == "tabular_llm_guarded_plan":
            return (
                {
                    "task_type": "chart",
                    "requested_output_type": "chart",
                    "source_scope": {"table_name": "requests", "sheet_name": "Sheet1"},
                    "measures": [{"requested": "spending", "field": "amount_total", "aggregation": "sum"}],
                    "dimensions": [{"requested": "city", "field": "city"}],
                    "derived_time_grain": "none",
                    "source_datetime_field": None,
                    "filters": [],
                    "chart_type": "bar",
                    "confidence": 0.95,
                    "ambiguity_flags": ["none"],
                },
                "success",
            )
        return (
            {
                "selected_route": "chart",
                "requested_output_type": "chart",
                "measure": {"field": "amount_total", "aggregation": "sum"},
                "dimension": {"field": "city"},
                "derived_time_grain": "none",
                "source_datetime_field": None,
                "filters": [],
                "chart_type": "bar",
                "output_columns": ["group_key", "value"],
            },
            "success",
        )

    def _fake_execute_sql(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {
            "rows": [("north", 10.0), ("south", 20.0)],
            "rows_effective": 2,
        }

    def _fake_render_chart_artifact(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {
            "chart_rendered": True,
            "chart_artifact_available": False,
            "chart_artifact_exists": False,
            "chart_fallback_reason": "artifact_not_accessible",
            "chart_artifact_path": None,
            "chart_artifact_id": None,
            "artifact": None,
        }

    monkeypatch.setattr(planner, "_call_llm_json", _fake_call_llm_json)
    monkeypatch.setattr(planner, "_execute_sql", _fake_execute_sql)
    monkeypatch.setattr(planner, "render_chart_artifact", _fake_render_chart_artifact)

    result = asyncio.run(
        planner.maybe_execute_llm_guarded_tabular(
            query="show spending by city chart",
            parsed_query_route="chart",
            selected_route="chart",
            dataset=dataset,
            table=table,
            target_file=file_obj,
        )
    )

    assert isinstance(result, dict)
    assert result["status"] == "ok"
    debug = result["debug"]
    assert debug["chart_rendered"] is True
    assert debug["chart_artifact_available"] is False
    assert debug["chart_artifact_exists"] is False
    assert debug["fallback_type"] == "tabular_chart_render_failed"
    assert debug["fallback_reason"] == "artifact_not_accessible"

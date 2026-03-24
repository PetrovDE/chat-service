import asyncio
from types import SimpleNamespace

from app.services.chat import tabular_sql as tsql
from app.services.chat.complex_analytics import composer
from app.services.tabular.sql_execution import ResolvedTabularDataset, ResolvedTabularTable


def _build_dataset(columns):
    return ResolvedTabularDataset(
        engine="duckdb_parquet",
        dataset_id="ds-routing",
        dataset_version=1,
        dataset_provenance_id="prov-routing",
        tables=[
            ResolvedTabularTable(
                table_name="requests",
                sheet_name="Sheet1",
                row_count=120,
                columns=list(columns),
                column_aliases={},
                table_version=1,
                provenance_id="tbl-routing",
                parquet_path=None,
            )
        ],
        catalog_path=None,
    )


def _file_obj():
    return SimpleNamespace(
        id="file-1",
        file_type="csv",
        extension="csv",
        original_filename="requests.csv",
        custom_metadata={},
    )


def test_explicit_overview_route_uses_standard_pipeline_without_generic_template(monkeypatch):
    dataset = _build_dataset(["request_id", "created_at", "city", "product", "amount_rub", "status", "priority"])
    file_obj = _file_obj()

    monkeypatch.setattr(tsql, "resolve_tabular_dataset", lambda _: dataset)

    def fake_profile_sync(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {
            "status": "ok",
            "prompt_context": "deterministic profile context",
            "debug": {"retrieval_mode": "tabular_sql", "intent": "tabular_profile", "tabular_sql": {}},
            "sources": ["requests.csv | profile"],
            "rows_expected_total": 120,
            "rows_retrieved_total": 120,
            "rows_used_map_total": 120,
            "rows_used_reduce_total": 120,
            "row_coverage_ratio": 1.0,
        }

    monkeypatch.setattr(tsql, "_execute_profile_sync", fake_profile_sync)

    result = asyncio.run(tsql.execute_tabular_sql_path(query="сделай обзор таблицы", files=[file_obj]))

    assert result is not None
    assert result["status"] == "ok"
    assert result["debug"]["selected_route"] == "overview"
    assert result["debug"]["detected_intent"] == "overview"
    assert "Полный аналитический отчет" not in str(result.get("prompt_context") or "")
    assert "Full Analytics Report" not in str(result.get("prompt_context") or "")


def test_narrow_chart_request_with_missing_column_returns_controlled_response(monkeypatch):
    dataset = _build_dataset(["request_id", "created_at", "client_name", "city", "product", "amount_rub", "status", "priority"])
    file_obj = _file_obj()
    monkeypatch.setattr(tsql, "resolve_tabular_dataset", lambda _: dataset)

    result = asyncio.run(
        tsql.execute_tabular_sql_path(
            query="дай график распределения пользователей по месяцам рождения",
            files=[file_obj],
        )
    )

    assert result is not None
    assert result["status"] == "error"
    assert result["debug"]["selected_route"] == "unsupported_missing_column"
    assert "birth_date" in result["debug"]["unmatched_requested_fields"]
    assert result["debug"]["matched_columns"] == []
    clarification = str(result.get("clarification_prompt") or "")
    assert "нет колонки" in clarification
    assert "created_at" in clarification
    assert "city" in clarification
    assert "status" in clarification
    assert "product" in clarification
    assert "Полный аналитический отчет" not in clarification


def test_narrow_chart_request_with_matching_column_routes_chart(monkeypatch):
    dataset = _build_dataset(["request_id", "birth_date", "city", "status"])
    file_obj = _file_obj()
    monkeypatch.setattr(tsql, "resolve_tabular_dataset", lambda _: dataset)

    def fake_chart_sync(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {
            "status": "ok",
            "prompt_context": "chart_spec: {'x': 'month', 'y': 'count'}",
            "debug": {"retrieval_mode": "tabular_sql", "intent": "tabular_chart", "tabular_sql": {}},
            "sources": ["requests.csv | chart"],
            "rows_expected_total": 120,
            "rows_retrieved_total": 120,
            "rows_used_map_total": 120,
            "rows_used_reduce_total": 120,
            "row_coverage_ratio": 1.0,
        }

    monkeypatch.setattr(tsql, "_execute_chart_sync", fake_chart_sync)

    result = asyncio.run(
        tsql.execute_tabular_sql_path(
            query="дай график распределения пользователей по месяцам рождения",
            files=[file_obj],
        )
    )

    assert result is not None
    assert result["status"] == "ok"
    assert result["debug"]["selected_route"] == "chart"
    assert result["debug"]["detected_intent"] == "chart"
    assert "birth_date" in result["debug"]["matched_columns"]
    assert "chart_spec" in str(result.get("prompt_context") or "")
    assert "Полный аналитический отчет" not in str(result.get("prompt_context") or "")


def test_schema_question_routes_to_schema_question(monkeypatch):
    dataset = _build_dataset(["request_id", "created_at", "city", "status"])
    file_obj = _file_obj()
    monkeypatch.setattr(tsql, "resolve_tabular_dataset", lambda _: dataset)

    result = asyncio.run(tsql.execute_tabular_sql_path(query="какие колонки есть в файле", files=[file_obj]))

    assert result is not None
    assert result["status"] == "ok"
    assert result["debug"]["selected_route"] == "schema_question"
    assert result["debug"]["detected_intent"] == "schema_question"
    assert '"columns"' in str(result.get("prompt_context") or "")
    assert '"created_at"' in str(result.get("prompt_context") or "")


def test_regression_old_generic_report_template_not_present_in_runtime_formatter():
    text = composer.format_complex_analytics_answer(
        query="дай обзор",
        table_name="requests",
        metrics={"rows_total": 10, "columns_total": 2, "columns": ["a", "b"]},
        notes=[],
        artifacts=[],
        executed_code="result = {}",
        include_code=False,
        insights=[],
    )

    assert "Полный аналитический отчет" not in text
    assert "Full Analytics Report" not in text
    assert "### 1) Сводка" not in text
    assert "### 1) Summary" not in text


def test_regression_route_telemetry_contains_new_fields(monkeypatch):
    dataset = _build_dataset(["request_id", "created_at", "city", "status"])
    file_obj = _file_obj()
    monkeypatch.setattr(tsql, "resolve_tabular_dataset", lambda _: dataset)

    result = asyncio.run(tsql.execute_tabular_sql_path(query="какие колонки есть в файле", files=[file_obj]))

    assert result is not None
    debug = result["debug"]
    assert "detected_intent" in debug
    assert "selected_route" in debug
    assert "matched_columns" in debug
    assert "unmatched_requested_fields" in debug
    assert "fallback_reason" in debug

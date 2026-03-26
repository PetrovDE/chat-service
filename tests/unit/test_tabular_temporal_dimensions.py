from app.services.chat import tabular_sql as tsql
from app.services.chat.tabular_intent_router import classify_tabular_query
from app.services.tabular.sql_execution import ResolvedTabularTable


def _table(columns, *, aliases=None):
    return ResolvedTabularTable(
        table_name="requests",
        sheet_name="Sheet1",
        row_count=120,
        columns=list(columns),
        column_aliases=dict(aliases or {}),
        table_version=1,
        provenance_id="tbl-1",
        parquet_path=None,
    )


def test_temporal_chart_derives_month_from_datetime_without_literal_month_column():
    table = _table(["request_id", "created_at", "amount_rub", "status"])
    decision = classify_tabular_query(query="show spending by month", table=table)

    assert decision.selected_route in {"trend", "chart"}
    assert decision.requested_time_grain == "month"

    sql, chart_spec = tsql._build_chart_sql(
        query="show spending by month",
        table=table,
        decision=decision,
    )

    assert "TRY_CAST(\"created_at\" AS TIMESTAMP)" in sql
    assert "strftime(TRY_CAST(\"created_at\" AS TIMESTAMP), '%Y-%m')" in sql
    assert "SUM(" in sql
    assert chart_spec["requested_time_grain"] == "month"
    assert chart_spec["source_datetime_field"] == "created_at"
    assert chart_spec["derived_temporal_dimension"] == "month(created_at)"
    assert chart_spec["temporal_plan_status"] == "resolved"


def test_temporal_query_with_multiple_datetime_columns_requires_controlled_clarification():
    table = _table(["request_id", "created_at", "updated_at", "amount_rub"])
    decision = classify_tabular_query(query="show spending by month", table=table)

    assert decision.selected_route == "unsupported_missing_column"
    assert decision.fallback_reason == "missing_or_ambiguous_datetime_source"
    assert decision.temporal_plan_status == "ambiguous_datetime_source"
    assert "month" in decision.unmatched_requested_fields


def test_temporal_query_with_explicit_datetime_source_resolves_without_ambiguity():
    table = _table(["request_id", "created_at", "updated_at", "amount_rub"])
    decision = classify_tabular_query(query="show spending by month using created_at", table=table)

    assert decision.selected_route in {"trend", "chart"}
    assert decision.source_datetime_field == "created_at"
    sql, chart_spec = tsql._build_chart_sql(
        query="show spending by month using created_at",
        table=table,
        decision=decision,
    )
    assert "TRY_CAST(\"created_at\" AS TIMESTAMP)" in sql
    assert chart_spec["source_datetime_field"] == "created_at"
    assert chart_spec["temporal_plan_status"] == "resolved"


def test_temporal_chart_in_russian_derives_month_and_sum_measure():
    table = _table(["request_id", "created_at", "amount_rub", "status"])
    query = (
        "\u041f\u043e\u043a\u0430\u0436\u0438 \u0433\u0440\u0430\u0444\u0438\u043a "
        "\u043e\u0431\u044a\u0435\u043c\u0430 \u0437\u0430\u0442\u0440\u0430\u0442 "
        "\u043f\u043e \u043c\u0435\u0441\u044f\u0446\u0430\u043c"
    )
    decision = classify_tabular_query(query=query, table=table)

    assert decision.selected_route in {"trend", "chart"}
    assert decision.requested_time_grain == "month"
    assert decision.temporal_plan_status == "resolved"
    assert decision.source_datetime_field == "created_at"

    sql, chart_spec = tsql._build_chart_sql(
        query=query,
        table=table,
        decision=decision,
    )

    assert "strftime(TRY_CAST(\"created_at\" AS TIMESTAMP), '%Y-%m')" in sql
    assert "SUM(" in sql
    assert chart_spec["requested_time_grain"] == "month"
    assert chart_spec["source_datetime_field"] == "created_at"
    assert chart_spec["temporal_plan_status"] == "resolved"
    assert isinstance(chart_spec["temporal_aggregation_plan"], dict)
    assert chart_spec["temporal_aggregation_plan"]["operation"] == "sum"
    assert chart_spec["temporal_aggregation_plan"]["measure_column"] == "amount_rub"

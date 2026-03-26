import pytest

from app.services.chat import tabular_sql as tsql
from app.services.tabular.sql_errors import SQL_ERROR_EXECUTION_FAILED, TabularSQLException
from app.services.tabular.sql_execution import ResolvedTabularTable


def _table(columns, aliases=None):
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


def test_lookup_does_not_fallback_to_first_column_when_filter_column_is_missing():
    table = _table(["request_id", "created_at", "amount_rub"])

    with pytest.raises(TabularSQLException) as exc_info:
        tsql._build_lookup_sql(
            query="find rows where customer = acme",
            table=table,
        )

    assert exc_info.value.code == SQL_ERROR_EXECUTION_FAILED
    assert exc_info.value.details.get("fallback_reason") == "missing_lookup_filter_column"


def test_aggregate_does_not_silently_switch_to_count_when_metric_is_missing():
    table = _table(["request_id", "created_at"])

    with pytest.raises(TabularSQLException) as exc_info:
        tsql._build_sql(
            query="sum",
            table=table,
        )

    assert exc_info.value.code == SQL_ERROR_EXECUTION_FAILED
    assert exc_info.value.details.get("fallback_reason") == "missing_metric_column"


def test_chart_sql_requires_confident_dimension_match():
    table = _table(["request_id", "created_at"])
    decision = tsql.classify_tabular_query(
        query="build chart by customer happiness",
        table=table,
    )

    with pytest.raises(TabularSQLException) as exc_info:
        tsql._build_chart_sql(query="build chart by customer happiness", table=table, decision=decision)

    assert exc_info.value.code == SQL_ERROR_EXECUTION_FAILED
    assert exc_info.value.details.get("fallback_reason") == "requested_field_not_matched"


def test_runtime_does_not_depend_on_domain_hint_wordlists_for_column_selection():
    table = _table(["col_a", "col_b"])
    decision = tsql.classify_tabular_query(
        query="build chart by status",
        table=table,
    )

    assert decision.selected_route == "unsupported_missing_column"
    assert decision.matched_columns == []


def test_chart_sql_ambiguous_requested_field_returns_controlled_failure():
    table = _table(["status_code", "status_name"])
    decision = tsql.classify_tabular_query(
        query="build chart by status",
        table=table,
    )

    assert decision.selected_route == "unsupported_missing_column"
    assert decision.fallback_reason == "missing_required_columns"


def test_requested_status_code_is_not_substituted_with_request_id():
    table = _table(["request_id", "status_code"], aliases={"status_code": "Status Code"})
    decision = tsql.classify_tabular_query(
        query="build chart by status code",
        table=table,
    )

    sql, chart_spec = tsql._build_chart_sql(query="build chart by status code", table=table, decision=decision)

    assert "GROUP BY bucket" in sql
    assert chart_spec.get("matched_chart_field") == "status_code"
    assert chart_spec.get("matched_chart_field") != "request_id"


def test_route_debug_payload_contains_schema_match_fields():
    table = _table(["status_code", "request_id"], aliases={"status_code": "Status Code"})
    decision = tsql.classify_tabular_query(
        query="build chart by status code",
        table=table,
    )

    debug_fields = tsql._route_debug_payload(decision=decision, detected_language="en")

    assert debug_fields["requested_field_text"] == "status code"
    assert debug_fields["matched_column"] == "status_code"
    assert isinstance(debug_fields.get("match_score"), float)
    assert debug_fields.get("match_strategy")

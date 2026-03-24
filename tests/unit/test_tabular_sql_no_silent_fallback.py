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

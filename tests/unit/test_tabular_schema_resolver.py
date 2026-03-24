from app.services.chat.tabular_schema_resolver import (
    find_direct_column_mentions,
    resolve_requested_field,
)
from app.services.tabular.sql_execution import ResolvedTabularTable


def _table(columns, aliases=None):
    return ResolvedTabularTable(
        table_name="sheet_1",
        sheet_name="Sheet1",
        row_count=100,
        columns=list(columns),
        column_aliases=dict(aliases or {}),
        table_version=1,
        provenance_id="tbl-1",
        parquet_path=None,
    )


def test_schema_resolver_matches_exact_column_name():
    table = _table(["revenue_total", "region"])
    resolution = resolve_requested_field(requested_field_text="revenue_total", table=table)

    assert resolution.status == "matched"
    assert resolution.matched_column == "revenue_total"
    assert resolution.match_score is not None and resolution.match_score >= 0.95


def test_schema_resolver_matches_display_alias():
    table = _table(
        ["revenue_total", "region"],
        aliases={"revenue_total": "Выручка", "region": "Регион"},
    )
    resolution = resolve_requested_field(requested_field_text="выручка", table=table)

    assert resolution.status == "matched"
    assert resolution.matched_column == "revenue_total"


def test_schema_resolver_reports_ambiguous_match():
    table = _table(["status_code", "status_name"])
    resolution = resolve_requested_field(
        requested_field_text="status",
        table=table,
        min_confidence=0.55,
        ambiguity_gap=0.12,
    )

    assert resolution.status == "ambiguous"
    assert resolution.matched_column is None
    assert "status_code" in resolution.candidate_columns
    assert "status_name" in resolution.candidate_columns


def test_schema_resolver_reports_low_confidence_no_match():
    table = _table(["request_id", "created_at", "amount_rub"])
    resolution = resolve_requested_field(requested_field_text="customer mood", table=table)

    assert resolution.status == "no_match"
    assert resolution.matched_column is None


def test_direct_column_mentions_are_schema_first():
    table = _table(
        ["created_at", "amount_rub"],
        aliases={"amount_rub": "Сумма"},
    )
    mentions = find_direct_column_mentions("show amount_rub and сумма by month", table)

    assert "amount_rub" in mentions


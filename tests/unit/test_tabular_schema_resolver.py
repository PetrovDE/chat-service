from app.services.chat.tabular_schema_resolver import (
    find_direct_column_mentions,
    resolve_requested_field,
)
from app.services.tabular.sql_execution import ResolvedTabularTable


def _table(columns, aliases=None, metadata=None):
    return ResolvedTabularTable(
        table_name="sheet_1",
        sheet_name="Sheet1",
        row_count=100,
        columns=list(columns),
        column_aliases=dict(aliases or {}),
        table_version=1,
        provenance_id="tbl-1",
        parquet_path=None,
        column_metadata=dict(metadata or {}),
    )


def test_schema_resolver_matches_exact_column_name():
    table = _table(["revenue_total", "region"])
    resolution = resolve_requested_field(requested_field_text="revenue_total", table=table)

    assert resolution.status == "matched"
    assert resolution.matched_column == "revenue_total"
    assert resolution.match_score is not None and resolution.match_score >= 0.95


def test_schema_resolver_matches_normalized_column_name():
    table = _table(["status_code", "request_id"])
    resolution = resolve_requested_field(requested_field_text="Status Code", table=table)

    assert resolution.status == "matched"
    assert resolution.matched_column == "status_code"


def test_schema_resolver_matches_display_alias():
    table = _table(
        ["revenue_total", "region"],
        aliases={"revenue_total": "Revenue Total", "region": "Region Name"},
    )
    resolution = resolve_requested_field(requested_field_text="revenue total", table=table)

    assert resolution.status == "matched"
    assert resolution.matched_column == "revenue_total"
    assert resolution.match_strategy in {"display_name_match", "exact_normalized_match"}


def test_schema_resolver_matches_metadata_alias():
    table = _table(
        ["status_code", "request_id"],
        aliases={"status_code": "Status Code"},
        metadata={
            "status_code": {
                "display_name": "Status Code",
                "aliases": ["Ticket Status", "State Code"],
                "dtype": "varchar",
                "sample_values": ["open", "closed"],
            }
        },
    )
    resolution = resolve_requested_field(requested_field_text="ticket status", table=table)

    assert resolution.status == "matched"
    assert resolution.matched_column == "status_code"
    assert resolution.match_strategy == "metadata_alias_match"


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
    resolution = resolve_requested_field(requested_field_text="customer happiness", table=table)

    assert resolution.status == "no_match"
    assert resolution.matched_column is None


def test_direct_column_mentions_are_schema_first():
    table = _table(
        ["created_at", "amount_rub"],
        aliases={"amount_rub": "Amount"},
        metadata={"amount_rub": {"aliases": ["Total Amount"]}},
    )
    mentions = find_direct_column_mentions("show amount_rub and total amount by month", table)

    assert "amount_rub" in mentions

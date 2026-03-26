from __future__ import annotations

from types import SimpleNamespace

from app.services.chat.tabular_debug_contract import build_dataset_debug_fields


def test_tabular_dataset_debug_fields_include_metadata_contract_and_stats():
    dataset = SimpleNamespace(
        engine="duckdb_parquet",
        dataset_id="file-1",
        dataset_version=2,
        dataset_provenance_id="prov-ds",
        column_metadata_contract_version="tabular_column_metadata_v1",
        column_metadata_stats={
            "columns_total": 10,
            "columns_with_metadata": 10,
            "aliases_total": 14,
            "sample_values_total": 20,
            "aliases_trimmed_total": 3,
            "sample_values_trimmed_total": 8,
            "metadata_budget_enforced": True,
        },
    )
    table = SimpleNamespace(
        table_name="sheet_1_orders",
        table_version=2,
        provenance_id="prov-table",
        row_count=120,
        column_metadata_contract_version="tabular_column_metadata_v1",
        column_metadata_stats={
            "columns_total": 4,
            "columns_with_metadata": 4,
            "aliases_total": 5,
            "sample_values_total": 8,
            "aliases_trimmed_total": 1,
            "sample_values_trimmed_total": 2,
            "metadata_budget_enforced": False,
        },
    )

    debug_fields = build_dataset_debug_fields(dataset=dataset, table=table)

    assert debug_fields["column_metadata_contract_version"] == "tabular_column_metadata_v1"
    assert debug_fields["column_metadata_present"] is True
    assert debug_fields["column_metadata_columns_total"] == 4
    assert debug_fields["column_metadata_columns_with_metadata"] == 4
    assert debug_fields["column_metadata_aliases_total"] == 5
    assert debug_fields["column_metadata_sample_values_total"] == 8
    assert debug_fields["column_metadata_aliases_trimmed_total"] == 1
    assert debug_fields["column_metadata_sample_values_trimmed_total"] == 2
    assert debug_fields["column_metadata_budget_enforced"] is False
    assert debug_fields["dataset_column_metadata_columns_total"] == 10
    assert debug_fields["dataset_column_metadata_aliases_total"] == 14

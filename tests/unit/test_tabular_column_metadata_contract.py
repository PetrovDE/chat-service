from __future__ import annotations

import json

import pandas as pd

from app.services.tabular.column_metadata_contract import (
    ColumnMetadataBudgetConfig,
    TABULAR_COLUMN_METADATA_CONTRACT_VERSION,
    build_dataframe_column_metadata,
    sanitize_tabular_column_metadata,
)
from app.services.tabular.normalization import normalize_dataframe_columns


def test_dataframe_column_metadata_emits_canonical_contract_fields():
    df = pd.DataFrame(
        {
            "Order ID": ["REQ-1", "REQ-2", "REQ-3"],
            "Total Amount": ["10.5", "20.0", "11.2"],
            "State": ["open", "closed", "open"],
        }
    )
    columns, aliases = normalize_dataframe_columns(df)
    metadata, stats = build_dataframe_column_metadata(df=df, columns=columns, aliases=aliases)

    assert TABULAR_COLUMN_METADATA_CONTRACT_VERSION == "tabular_column_metadata_v1"
    assert set(metadata.keys()) == set(columns)

    order_entry = metadata["order_id"]
    assert order_entry["raw_name"] == "Order ID"
    assert order_entry["normalized_name"] == "order_id"
    assert order_entry["display_name"] == "Order ID"
    assert isinstance(order_entry.get("aliases"), list)
    assert order_entry.get("dtype") in {"text", "integer", "numeric", "datetime", "boolean", "empty", "unknown"}

    assert stats["columns_total"] == len(columns)
    assert stats["columns_with_metadata"] == len(columns)
    assert stats["metadata_bytes"] <= stats["metadata_budget_bytes"]


def test_sanitize_column_metadata_bounds_aliases_and_sample_values():
    raw_metadata = {
        "status_code": {
            "display_name": "Status Code",
            "aliases": [
                "Status",
                "Status Code",
                "Ticket Status",
                "State",
                "State Name",
                "Workflow State",
                "Lifecycle Stage",
            ],
            "dtype": "VARCHAR",
            "sample_values": [
                "open",
                "closed",
                "pending",
                "blocked",
                "qa",
                "done",
                "deployed",
            ],
        }
    }
    config = ColumnMetadataBudgetConfig(
        max_aliases_per_column=4,
        max_sample_values_per_column=3,
        alias_max_chars=24,
        sample_value_max_chars=12,
        max_column_metadata_bytes=4_000,
    )
    metadata, stats = sanitize_tabular_column_metadata(
        raw_metadata=raw_metadata,
        columns=["status_code"],
        aliases={"status_code": "Status Code"},
        config=config,
    )

    entry = metadata["status_code"]
    assert entry["dtype"] == "text"
    assert len(entry.get("aliases", [])) <= 4
    assert len(entry.get("sample_values", [])) <= 3
    assert stats["aliases_trimmed_total"] > 0
    assert stats["sample_values_trimmed_total"] > 0


def test_sanitize_column_metadata_enforces_table_budget():
    raw_metadata = {
        f"column_{idx}": {
            "display_name": f"Column {idx}",
            "aliases": [f"Column {idx} alias {n}" for n in range(12)],
            "dtype": "text",
            "sample_values": [f"value_{idx}_{n}_with_extra_text" for n in range(20)],
        }
        for idx in range(1, 24)
    }
    config = ColumnMetadataBudgetConfig(
        max_aliases_per_column=6,
        max_sample_values_per_column=5,
        max_column_metadata_bytes=1_400,
    )
    columns = [f"column_{idx}" for idx in range(1, 24)]
    metadata, stats = sanitize_tabular_column_metadata(
        raw_metadata=raw_metadata,
        columns=columns,
        aliases={column: column for column in columns},
        config=config,
    )

    serialized = json.dumps(metadata, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    assert len(serialized) <= 1_400
    assert stats["metadata_budget_enforced"] is True
    assert stats["columns_with_metadata"] <= stats["columns_total"]

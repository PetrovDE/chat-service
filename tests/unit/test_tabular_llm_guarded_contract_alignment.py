from app.services.chat.tabular_llm_guarded_contract_alignment import (
    normalize_execution_spec_payload,
    normalize_plan_payload,
)


def test_normalize_plan_payload_converts_trend_task_type_to_chart() -> None:
    raw_plan = {
        "task_type": "trend",
        "requested_output_type": "visualization",
        "measures": [{"requested": "spending", "field": "amount_rub", "aggregation": "sum"}],
        "dimensions": [],
        "derived_time_grain": "month",
        "source_datetime_field": "created_at",
        "filters": [],
        "chart_type": "time_series",
        "confidence": 0.8,
        "ambiguity_flags": ["none"],
    }

    normalized = normalize_plan_payload(raw_plan=raw_plan, query="show spending by month")

    assert normalized["task_type"] == "chart"
    assert normalized["requested_output_type"] == "chart"
    assert normalized["chart_type"] == "line"


def test_normalize_execution_spec_payload_backfills_route_and_columns_from_plan() -> None:
    validated_plan = {
        "task_type": "chart",
        "requested_output_type": "chart",
        "source_scope": {"table_name": "requests", "sheet_name": "Sheet1"},
        "measures": [{"requested": "spending", "field": "amount_rub", "aggregation": "sum"}],
        "dimensions": [],
        "derived_time_grain": "month",
        "source_datetime_field": "created_at",
        "filters": [],
        "chart_type": "line",
        "confidence": 0.91,
        "ambiguity_flags": ["none"],
    }
    raw_execution_spec = {
        "task_type": "trend",
        "requested_output_type": "visualization",
        "measure": {"field": "amount_rub", "aggregation": "sum"},
        "dimension": {"field": None},
        "chart_type": "time_series",
    }

    normalized = normalize_execution_spec_payload(
        raw_execution_spec=raw_execution_spec,
        validated_plan=validated_plan,
    )

    assert normalized["selected_route"] == "chart"
    assert normalized["requested_output_type"] == "chart"
    assert normalized["derived_time_grain"] == "month"
    assert normalized["source_datetime_field"] == "created_at"
    assert normalized["output_columns"] == ["bucket", "value"]

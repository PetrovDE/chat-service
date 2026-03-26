from pathlib import Path
from types import SimpleNamespace

from app.domain.chat.query_planner import ROUTE_DETERMINISTIC_ANALYTICS, plan_query
from app.observability.metrics import reset_metrics, snapshot_metrics


def _resolved_tabular_file(tmp_path: Path):
    parquet_path = tmp_path / "planner_contract.parquet"
    parquet_path.write_bytes(b"PAR1")
    return SimpleNamespace(
        id="file-contract",
        file_type="csv",
        extension="csv",
        original_filename="report.csv",
        custom_metadata={
            "tabular_dataset": {
                "dataset_id": "ds-contract",
                "dataset_version": 1,
                "dataset_provenance_id": "prov-contract",
                "catalog_path": str(tmp_path / "catalog.duckdb"),
                "tables": [
                    {
                        "table_name": "report",
                        "sheet_name": "Sheet1",
                        "row_count": 50,
                        "columns": ["city", "amount"],
                        "column_aliases": {"amount": "total_amount"},
                        "parquet_path": str(parquet_path),
                    }
                ],
            }
        },
    )


def test_query_planner_decision_contract_shape(tmp_path: Path):
    reset_metrics()
    decision = plan_query(
        query="How many rows are in the whole file?",
        files=[_resolved_tabular_file(tmp_path)],
    )
    payload = decision.as_dict()
    assert set(["route", "intent", "confidence", "requires_clarification", "reason_codes"]).issubset(payload.keys())
    assert isinstance(payload["route"], str)
    assert isinstance(payload["intent"], str)
    assert isinstance(payload["confidence"], float)
    assert isinstance(payload["requires_clarification"], bool)
    assert isinstance(payload["reason_codes"], list)
    counters = snapshot_metrics()["counters"]
    assert any(
        "llama_service_query_planner_route_total" in key
        and "route_class=deterministic" in key
        for key in counters
    )


def test_query_planner_metric_critical_ambiguous_returns_clarification(tmp_path: Path):
    decision = plan_query(
        query="What is the average?",
        files=[_resolved_tabular_file(tmp_path)],
    )
    payload = decision.as_dict()
    assert payload["route"] == ROUTE_DETERMINISTIC_ANALYTICS
    assert payload["requires_clarification"] is True
    assert "metric_critical_ambiguous" in payload["reason_codes"]
    assert isinstance(payload.get("clarification_prompt"), str) and payload["clarification_prompt"]


def test_query_planner_routes_python_requests_to_complex_analytics(tmp_path: Path):
    decision = plan_query(
        query="Run Python pandas analysis with heatmap and NLP on comment_text",
        files=[_resolved_tabular_file(tmp_path)],
    )
    payload = decision.as_dict()
    assert payload["route"] == "complex_analytics"
    assert payload["intent"] == "complex_analytics"
    assert payload["requires_clarification"] is False

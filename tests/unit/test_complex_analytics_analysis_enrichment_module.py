import pandas as pd

from app.services.chat.complex_analytics.analysis_enrichment import enrich_metrics_from_dataframe


def test_enrichment_fills_missing_profile_and_statistics():
    frame = pd.DataFrame(
        {
            "request_id": [1, 2, 3, 4],
            "amount_rub": [100.0, 120.0, 90.0, 200.0],
            "status": ["new", "done", "done", "new"],
        }
    )
    enriched = enrich_metrics_from_dataframe(
        metrics={"rows_total": 4, "columns_total": 3, "columns": ["request_id", "amount_rub", "status"]},
        frame=frame,
    )

    assert isinstance(enriched.get("column_profile"), list)
    assert len(enriched["column_profile"]) >= 3
    assert isinstance(enriched.get("numeric_summary"), list)
    assert any(str(item.get("column")) == "amount_rub" for item in enriched["numeric_summary"])
    assert isinstance(enriched.get("categorical_summary"), list)
    assert any(str(item.get("column")) == "status" for item in enriched["categorical_summary"])
    assert isinstance(enriched.get("insights"), list)
    assert enriched["insights"]

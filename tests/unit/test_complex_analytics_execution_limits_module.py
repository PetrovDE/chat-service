import pandas as pd

from app.services.chat.complex_analytics import execution_limits


def test_resolve_max_artifacts_limit_scales_for_broad_visual_query(monkeypatch):
    monkeypatch.setattr(execution_limits.settings, "COMPLEX_ANALYTICS_MAX_ARTIFACTS", 4)
    monkeypatch.setattr(execution_limits.settings, "COMPLEX_ANALYTICS_MAX_ARTIFACTS_HARD_CAP", 32)
    limit = execution_limits.resolve_max_artifacts_limit(
        query="Analyze this file fully, provide distributions, relationships and charts",
        codegen_meta={"plan_contract": {"expects_visualization": True, "expects_dependency": True}},
        primary_frame=pd.DataFrame({f"c{i}": [i, i + 1, i + 2] for i in range(30)}),
    )
    assert limit > 4
    assert limit <= 32

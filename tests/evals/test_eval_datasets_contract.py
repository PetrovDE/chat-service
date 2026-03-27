import json
from pathlib import Path

from scripts.evals.datasets import load_named_datasets
from scripts.evals.runner import OFFLINE_DATASET_NAMES, ONLINE_DATASET_NAMES


def test_eval_datasets_exist_and_have_required_fields():
    dataset_root = Path("tests/evals/datasets")
    dataset_names = tuple(OFFLINE_DATASET_NAMES) + tuple(ONLINE_DATASET_NAMES)
    datasets = load_named_datasets(dataset_root=dataset_root, dataset_names=dataset_names)

    assert set(datasets.keys()) == set(dataset_names)
    for dataset_name, rows in datasets.items():
        assert rows, f"{dataset_name} must not be empty"
        for row in rows:
            assert row.get("id"), f"{dataset_name} row missing id"
            if dataset_name in {"tabular_aggregate_golden", "tabular_profile_golden", "tabular_langgraph_eval_slice_golden"}:
                assert row.get("query")
                assert isinstance(row.get("table"), dict)
                assert isinstance(row.get("expected"), dict)
            if dataset_name == "narrative_rag_golden":
                assert isinstance(row.get("source_passages"), dict)
                assert isinstance(row.get("claims"), list)
            if dataset_name == "fallback_route_golden":
                assert isinstance(row.get("scenario"), dict)
                assert isinstance(row.get("expected"), dict)
            if dataset_name == "complex_analytics_quality_golden":
                assert row.get("query")
                assert isinstance(row.get("table"), dict)
                assert isinstance(row.get("expected"), dict)
                assert isinstance((row.get("expected") or {}).get("required_substrings"), list)
            if dataset_name == "complex_analytics_quality_online":
                assert isinstance(row.get("online_request"), dict)
                assert isinstance(row.get("online_expect"), dict)
                assert str(row.get("online_metric") or "").strip()
            if dataset_name in {
                "rag_retrieval_quality_online",
                "rag_failure_explainability_online",
                "tabular_followup_continuity_online",
            }:
                assert isinstance(row.get("online_request"), dict)
                assert isinstance(row.get("online_expect"), dict)
                assert str(row.get("online_metric") or "").strip()
                enabled_if_env = str(row.get("enabled_if_env") or "").strip()
                assert enabled_if_env, f"{dataset_name} row must declare enabled_if_env for controlled online rollout"


def test_stage6_golden_fixture_catalog_covers_required_categories():
    catalog_path = Path("tests/evals/datasets/golden_fixture_catalog.json")
    payload = json.loads(catalog_path.read_text(encoding="utf-8"))

    fixtures = payload.get("fixtures")
    assert isinstance(fixtures, list) and fixtures

    categories = {str(item.get("category") or "").strip().lower() for item in fixtures if isinstance(item, dict)}
    required_categories = {
        "text-heavy docs",
        "tabular files",
        "mixed-format uploads",
        "noisy or poorly structured files",
        "large files",
        "multiple documents in one conversation",
        "short but valid files",
        "weak-pdf and near-empty pdf edge cases",
    }
    assert required_categories.issubset(categories)

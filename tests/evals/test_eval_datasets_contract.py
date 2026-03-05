from pathlib import Path

from scripts.evals.datasets import load_named_datasets
from scripts.evals.runner import OFFLINE_DATASET_NAMES


def test_eval_datasets_exist_and_have_required_fields():
    dataset_root = Path("tests/evals/datasets")
    datasets = load_named_datasets(dataset_root=dataset_root, dataset_names=OFFLINE_DATASET_NAMES)

    assert set(datasets.keys()) == set(OFFLINE_DATASET_NAMES)
    for dataset_name, rows in datasets.items():
        assert rows, f"{dataset_name} must not be empty"
        for row in rows:
            assert row.get("id"), f"{dataset_name} row missing id"
            if dataset_name.startswith("tabular_"):
                assert row.get("query")
                assert isinstance(row.get("table"), dict)
                assert isinstance(row.get("expected"), dict)
            if dataset_name == "narrative_rag_golden":
                assert isinstance(row.get("source_passages"), dict)
                assert isinstance(row.get("claims"), list)
            if dataset_name == "fallback_route_golden":
                assert isinstance(row.get("scenario"), dict)
                assert isinstance(row.get("expected"), dict)

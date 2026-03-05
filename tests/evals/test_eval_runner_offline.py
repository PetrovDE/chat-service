from pathlib import Path

from scripts.evals.ci_gates import evaluate_ci_gates, load_gate_config
from scripts.evals.runner import run_eval_suite


def test_offline_eval_runner_passes_all_quality_gates():
    summary = run_eval_suite(
        mode="offline",
        dataset_root=Path("tests/evals/datasets"),
    )
    gates = evaluate_ci_gates(summary, load_gate_config(Path("tests/evals/gates.json")))

    numeric = summary["metrics"]["numeric_exact_match"]
    citation = summary["metrics"]["citation_faithfulness"]
    route = summary["metrics"]["route_correctness"]

    assert numeric["score"] == 1.0
    assert citation["score"] == 1.0
    assert route["score"] == 1.0
    assert gates["passed"] is True
    assert not summary["latency"]["violations"]

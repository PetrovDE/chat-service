from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from scripts.evals.ci_gates import evaluate_ci_gates, load_gate_config
from scripts.evals.runner import run_eval_suite


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run eval suite and enforce CI quality gates")
    parser.add_argument("--mode", choices=["offline", "online", "hybrid"], default="offline")
    parser.add_argument("--datasets-root", default="tests/evals/datasets")
    parser.add_argument("--gate-config", default="tests/evals/gates.json")
    parser.add_argument("--online-base-url", default=None)
    parser.add_argument("--online-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--online-auth-bearer-token", default=None)
    parser.add_argument("--output-json", default=None)
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    summary = run_eval_suite(
        mode=args.mode,
        dataset_root=Path(args.datasets_root),
        online_base_url=args.online_base_url,
        online_timeout_seconds=float(args.online_timeout_seconds),
        online_auth_bearer_token=args.online_auth_bearer_token,
    )
    gates = evaluate_ci_gates(summary, load_gate_config(Path(args.gate_config)))
    summary["ci_gates"] = gates

    rendered = json.dumps(summary, ensure_ascii=False, indent=2)
    print(rendered)

    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")

    return 0 if gates.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())

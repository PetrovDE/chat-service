from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.architecture.enforcement_checks import format_issues, run_architecture_checks


CONTRACT_TEST_SELECTION = (
    "tests/unit/test_tabular_sql_no_silent_fallback.py",
    "tests/unit/test_tabular_schema_resolver.py",
    "tests/integration/test_rag_debug_contract.py",
)

OPTIONAL_CONTRACT_TEST_DEPENDENCIES = (
    "pytest",
    "pydantic",
)


def _get_missing_optional_dependencies() -> list[str]:
    missing: list[str] = []
    for module_name in OPTIONAL_CONTRACT_TEST_DEPENDENCIES:
        if importlib.util.find_spec(module_name) is None:
            missing.append(module_name)
    return missing


def _run_contract_tests() -> int:
    command = [sys.executable, "-m", "pytest", "-q", *CONTRACT_TEST_SELECTION]
    print(f"Running contract tests: {' '.join(CONTRACT_TEST_SELECTION)}")
    completed = subprocess.run(command, cwd=str(ROOT))
    return int(completed.returncode)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run architecture quality gates.")
    parser.add_argument(
        "--strict-only",
        action="store_true",
        help="Run only static strict/warning architecture checks.",
    )
    parser.add_argument(
        "--with-contract-tests",
        action="store_true",
        help="Also run the fallback/matching/debug contract subset after strict checks.",
    )
    args = parser.parse_args()

    strict_issues, warning_issues = run_architecture_checks()

    print("== Architecture Strict Checks ==")
    print(format_issues(strict_issues))
    print("")
    print("== Architecture Warning Checks ==")
    print(format_issues(warning_issues))

    if strict_issues:
        print("")
        print("Strict architecture checks failed. See messages above.")
        return 1

    if args.strict_only:
        print("")
        print("Strict architecture checks passed (contract tests skipped).")
        return 0

    if not args.with_contract_tests:
        print("")
        print("Strict architecture checks passed.")
        print("Contract subset is available via --with-contract-tests.")
        return 0

    print("")
    print("Strict architecture checks passed. Running contract subset.")
    missing_optional_dependencies = _get_missing_optional_dependencies()
    if missing_optional_dependencies:
        print(
            "Contract subset skipped: missing optional dependency in this interpreter: "
            + ", ".join(missing_optional_dependencies)
        )
        print(
            "Install project dependencies (for example: pip install -r requirements.txt) "
            "and rerun with --with-contract-tests."
        )
        return 0

    contract_exit_code = _run_contract_tests()
    if contract_exit_code != 0:
        print("")
        print("Contract subset failed.")
        return contract_exit_code

    print("")
    print("Architecture checks and contract subset passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.architecture.enforcement_checks import format_issues, run_architecture_checks  # noqa: E402


def test_architecture_strict_quality_gates():
    strict_issues, _warning_issues = run_architecture_checks()
    assert not strict_issues, "Architecture quality gates failed:\n" + format_issues(strict_issues)


def test_architecture_warning_gates_execute():
    _strict_issues, warning_issues = run_architecture_checks()
    assert all(item.severity == "warning" for item in warning_issues)


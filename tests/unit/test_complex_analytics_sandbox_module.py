from pathlib import Path

import pandas as pd

from app.services.chat.complex_analytics import sandbox
from app.services.chat.complex_analytics.errors import ComplexAnalyticsSecurityError


def test_sandbox_executes_allowed_code(tmp_path: Path):
    out = sandbox.execute_sandboxed_python(
        code="result = {'rows': int(datasets['sheet_1'].shape[0])}",
        datasets={"sheet_1": pd.DataFrame({"x": [1, 2, 3]})},
        artifacts_dir=tmp_path / "sandbox_ok",
        max_output_chars=1000,
        max_artifacts=2,
    )
    assert out.result["rows"] == 3


def test_sandbox_blocks_forbidden_import():
    try:
        sandbox.execute_sandboxed_python(
            code="import os\nresult = {'ok': True}",
            datasets={"sheet_1": pd.DataFrame({"x": [1]})},
            artifacts_dir=Path("uploads") / "sandbox_blocked",
            max_output_chars=1000,
            max_artifacts=1,
        )
        assert False, "expected security error"
    except ComplexAnalyticsSecurityError as exc:
        assert "Import blocked" in str(exc)

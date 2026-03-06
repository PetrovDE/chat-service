import asyncio
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from app.services.chat.complex_analytics import executor as ca
from app.services.tabular.sql_execution import ResolvedTabularDataset, ResolvedTabularTable


def test_complex_analytics_intent_detector():
    assert ca.is_complex_analytics_query("Run Python pandas heatmap and NLP over comment_text") is True
    assert ca.is_complex_analytics_query("How many rows are in the file?") is False


def test_sandbox_allows_internal_dependency_imports_for_allowed_modules(tmp_path: Path):
    code = """
import pandas as pd
import numpy as np
result = {"rows": int(pd.DataFrame({"x": [1, 2, 3]}).shape[0]), "sum": int(np.array([1, 2, 3]).sum())}
"""
    out = ca.execute_sandboxed_python(
        code=code,
        datasets={"sheet_1": pd.DataFrame({"x": [1]})},
        artifacts_dir=tmp_path / "unit_test_artifacts_internal_imports",
        max_output_chars=1000,
        max_artifacts=1,
    )
    assert out.result["rows"] == 3
    assert out.result["sum"] == 6


def test_sandbox_blocks_forbidden_import():
    code = "import os\nresult = {'ok': True}"
    try:
        ca.execute_sandboxed_python(
            code=code,
            datasets={"sheet_1": pd.DataFrame({"x": [1]})},
            artifacts_dir=Path("uploads") / "unit_test_artifacts",
            max_output_chars=1000,
            max_artifacts=1,
        )
        assert False, "sandbox should block forbidden import"
    except ca.ComplexAnalyticsSecurityError as exc:
        assert "Import blocked" in str(exc)


def test_sandbox_blocks_forbidden_operation():
    code = "result = os.system('echo blocked')"
    try:
        ca.execute_sandboxed_python(
            code=code,
            datasets={"sheet_1": pd.DataFrame({"x": [1]})},
            artifacts_dir=Path("uploads") / "unit_test_artifacts_2",
            max_output_chars=1000,
            max_artifacts=1,
        )
        assert False, "sandbox should block forbidden operation"
    except ca.ComplexAnalyticsSecurityError as exc:
        assert "Call blocked" in str(exc)


def test_sandbox_blocks_getattr_bypass_attempt():
    code = "result = getattr(datasets, '__class__')"
    try:
        ca.execute_sandboxed_python(
            code=code,
            datasets={"sheet_1": pd.DataFrame({"x": [1]})},
            artifacts_dir=Path("uploads") / "unit_test_artifacts_3",
            max_output_chars=1000,
            max_artifacts=1,
        )
        assert False, "sandbox should block getattr-based bypass attempts"
    except ca.ComplexAnalyticsSecurityError as exc:
        assert "Call blocked" in str(exc)


def test_sandbox_blocks_dunder_attribute_access():
    code = "import pandas as pd\nresult = pd.__dict__"
    try:
        ca.execute_sandboxed_python(
            code=code,
            datasets={"sheet_1": pd.DataFrame({"x": [1]})},
            artifacts_dir=Path("uploads") / "unit_test_artifacts_4",
            max_output_chars=1000,
            max_artifacts=1,
        )
        assert False, "sandbox should block dunder attribute access"
    except ca.ComplexAnalyticsSecurityError as exc:
        assert "Dunder attribute access blocked" in str(exc)


def test_complex_executor_timeout_returns_classified_error(monkeypatch):
    dataset = ResolvedTabularDataset(
        engine="duckdb_parquet",
        dataset_id="ds-timeout",
        dataset_version=1,
        dataset_provenance_id="prov-timeout",
        tables=[
            ResolvedTabularTable(
                table_name="sheet_1",
                sheet_name="Sheet1",
                row_count=3,
                columns=["x"],
                column_aliases={},
                table_version=1,
                provenance_id="tbl-prov",
                parquet_path=None,
            )
        ],
        catalog_path=None,
        sqlite_path=None,
    )

    monkeypatch.setattr(
        ca,
        "_collect_datasets_for_file",
        lambda file_obj: (dataset, {"sheet_1": pd.DataFrame({"x": [1, 2, 3]})}),
    )

    def slow_execute(**kwargs):  # noqa: ANN003
        _ = kwargs
        time.sleep(0.2)
        return {"status": "ok", "debug": {"executor_status": "success"}, "final_response": "ok", "sources": [], "artifacts": []}

    async def fake_codegen(**kwargs):  # noqa: ANN003
        _ = kwargs
        return "result = {'status': 'ok', 'metrics': {}, 'notes': [], 'artifacts': []}", {"code_source": "llm"}

    monkeypatch.setattr(ca, "_generate_complex_analysis_code", fake_codegen)
    monkeypatch.setattr(ca, "_execute_complex_analytics_sync", slow_execute)
    monkeypatch.setattr(ca.settings, "COMPLEX_ANALYTICS_TIMEOUT_SECONDS", 0.05)

    result = asyncio.run(
        ca.execute_complex_analytics_path(
            query="python pandas analysis",
            files=[SimpleNamespace(id="f1", file_type="xlsx", original_filename="x.xlsx", custom_metadata={})],
        )
    )
    assert result is not None
    assert result["status"] == "error"
    assert result["debug"]["executor_error_code"] == ca.COMPLEX_ANALYTICS_ERROR_TIMEOUT


def test_russian_report_is_localized_without_mojibake():
    report = ca._format_complex_analytics_answer(
        query="Сделай полный анализ таблицы и графики",
        table_name="sheet_1",
        metrics={
            "rows_total": 10,
            "columns_total": 2,
            "columns": ["request_id", "status"],
            "potential_process": "Likely an operational process dataset with records, dimensions, and process indicators.",
            "column_profile": [
                {
                    "column": "request_id",
                    "purpose_hint": "identifier/key column",
                    "non_null": 10,
                    "null_count": 0,
                    "unique_count": 10,
                    "sample_values": ["REQ-1", "REQ-2", "REQ-3"],
                }
            ],
            "numeric_summary": [],
            "datetime_summary": [],
            "categorical_summary": [],
        },
        notes=[],
        artifacts=[],
        executed_code="result = {}",
        include_code=False,
    )

    assert "## Полный аналитический отчет" in report
    assert "### 2) Контекст процесса" in report
    assert "Вероятно, это операционный процессный датасет" in report
    assert "идентификатор/ключ" in report
    assert "????" not in report


def test_complex_analytics_artifact_retention_cleanup(tmp_path: Path, monkeypatch):
    root = tmp_path / "complex_analytics"
    old_dir = root / "old_run"
    mid_dir = root / "mid_run"
    new_dir = root / "new_run"
    for path in (old_dir, mid_dir, new_dir):
        path.mkdir(parents=True, exist_ok=True)
        (path / "x.png").write_text("x", encoding="utf-8")

    now = time.time()
    os.utime(old_dir, (now - 10 * 3600, now - 10 * 3600))
    os.utime(mid_dir, (now - 5 * 3600, now - 5 * 3600))
    os.utime(new_dir, (now - 1 * 3600, now - 1 * 3600))

    monkeypatch.setattr(ca.settings, "COMPLEX_ANALYTICS_ARTIFACT_TTL_HOURS", 2)
    monkeypatch.setattr(ca.settings, "COMPLEX_ANALYTICS_ARTIFACT_MAX_RUN_DIRS", 1)

    stats = ca._cleanup_complex_analytics_artifacts(artifacts_root=root)
    assert isinstance(stats, dict)
    assert stats.get("deleted", 0) >= 2
    remaining = [p for p in root.iterdir() if p.is_dir()]
    assert len(remaining) <= 1


def test_codegen_uses_llm_generated_code_when_contract_is_valid(monkeypatch):
    captured = {}

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        policy_class = str(kwargs.get("policy_class") or "")
        if policy_class == "complex_analytics_plan":
            return {
                "response": """
{
  "analysis_goal": "dependency analysis",
  "required_artifacts": ["plots", "metrics"],
  "required_outputs": ["summary", "metrics", "insights", "artifacts"],
  "data_contract": {
    "required_inputs": ["datasets", "dataset_name", "file_ids"],
    "required_outputs": ["result", "artifacts", "insights", "metrics", "tables"]
  },
  "required_contract": {
    "expects_visualization": false,
    "expects_dependency": true,
    "expects_nlp": false
  },
  "python_generation_prompt": "Generate dependency-focused analysis code using datasets input only.",
  "should_generate_code": true
}
""",
                "model_route": "ollama",
                "provider_effective": "ollama",
            }
        return {
            "response": """
import matplotlib.pyplot as plt
df = datasets["sheet_1"].copy()
fig, ax = plt.subplots(figsize=(4, 3))
ax.plot([1, 2], [1, 2])
plot_path = save_plot(fig=fig, name="unit_chart.png")
plt.close(fig)
result = {
    "status": "ok",
    "table_name": "sheet_1",
    "metrics": {
        "rows_total": int(len(df)),
        "columns_total": int(len(df.columns)),
        "columns": [str(c) for c in df.columns],
        "insights": ["Custom dependency-focused analysis executed"]
    },
    "notes": [],
    "artifacts": [{"kind": "line", "path": plot_path}]
}
""",
            "model_route": "ollama",
            "provider_effective": "ollama",
        }

    monkeypatch.setattr(ca.llm_manager, "generate_response", fake_generate_response)
    monkeypatch.setattr(ca.settings, "COMPLEX_ANALYTICS_CODEGEN_ENABLED", True)
    monkeypatch.setattr(ca.settings, "COMPLEX_ANALYTICS_CODEGEN_FORCE_LOCAL", True)

    code, meta = asyncio.run(
        ca._generate_complex_analysis_code(
            query="analyze dependencies and build heatmap",
            primary_table_name="sheet_1",
            primary_frame=pd.DataFrame({"x": [1, 2], "y": [3, 4]}),
            model_source="local",
            provider_mode="explicit",
            model_name="llama3.2",
        )
    )

    assert "datasets[\"sheet_1\"]" in code
    assert meta.get("code_source") == "llm"
    assert meta.get("codegen_status") == "success"
    assert captured.get("model_source") == "ollama"
    assert captured.get("provider_mode") == "explicit"


def test_codegen_falls_back_to_template_when_generated_code_is_invalid(monkeypatch):
    async def fake_generate_response(**kwargs):  # noqa: ANN003
        policy_class = str(kwargs.get("policy_class") or "")
        if policy_class == "complex_analytics_plan":
            return {
                "response": """
{
  "analysis_goal": "simple analysis",
  "required_artifacts": [],
  "required_outputs": ["summary", "metrics"],
  "data_contract": {"required_inputs": ["datasets"], "required_outputs": ["result"]},
  "required_contract": {"expects_visualization": false, "expects_dependency": false, "expects_nlp": false},
  "python_generation_prompt": "Generate analysis code",
  "should_generate_code": true
}
"""
            }
        return {"response": "print('hello')"}

    monkeypatch.setattr(ca.llm_manager, "generate_response", fake_generate_response)
    monkeypatch.setattr(ca.settings, "COMPLEX_ANALYTICS_CODEGEN_ENABLED", True)

    code, meta = asyncio.run(
        ca._generate_complex_analysis_code(
            query="build dependency chart",
            primary_table_name="sheet_1",
            primary_frame=pd.DataFrame({"x": [1, 2, 3]}),
            model_source="local",
            provider_mode="explicit",
            model_name="llama3.2",
        )
    )

    assert code.lstrip().startswith("import pandas as pd")
    assert meta.get("code_source") == "template"
    assert meta.get("codegen_status") == "fallback"
    assert str(meta.get("codegen_error") or "")


def test_execute_path_returns_codegen_error_when_template_fallback_disabled(monkeypatch):
    dataset = ResolvedTabularDataset(
        engine="duckdb_parquet",
        dataset_id="ds-codegen",
        dataset_version=1,
        dataset_provenance_id="prov-codegen",
        tables=[
            ResolvedTabularTable(
                table_name="sheet_1",
                sheet_name="Sheet1",
                row_count=3,
                columns=["x"],
                column_aliases={},
                table_version=1,
                provenance_id="tbl-prov",
                parquet_path=None,
            )
        ],
        catalog_path=None,
        sqlite_path=None,
    )

    monkeypatch.setattr(
        ca,
        "_collect_datasets_for_file",
        lambda file_obj: (dataset, {"sheet_1": pd.DataFrame({"x": [1, 2, 3]})}),
    )

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {"response": "not-json-and-not-code"}

    monkeypatch.setattr(ca.llm_manager, "generate_response", fake_generate_response)
    monkeypatch.setattr(ca.settings, "COMPLEX_ANALYTICS_CODEGEN_ENABLED", True)
    monkeypatch.setattr(ca.settings, "COMPLEX_ANALYTICS_ALLOW_TEMPLATE_FALLBACK", False)

    result = asyncio.run(
        ca.execute_complex_analytics_path(
            query="run python dependency analytics",
            files=[SimpleNamespace(id="f1", file_type="xlsx", original_filename="x.xlsx", custom_metadata={})],
            model_source="local",
            provider_mode="explicit",
            model_name="llama3.2",
        )
    )
    assert result is not None
    assert result["status"] == "error"
    assert result["debug"]["executor_error_code"] == ca.COMPLEX_ANALYTICS_ERROR_CODEGEN


def test_execute_path_uses_template_fallback_when_enabled(monkeypatch):
    dataset = ResolvedTabularDataset(
        engine="duckdb_parquet",
        dataset_id="ds-template",
        dataset_version=1,
        dataset_provenance_id="prov-template",
        tables=[
            ResolvedTabularTable(
                table_name="sheet_1",
                sheet_name="Sheet1",
                row_count=3,
                columns=["x"],
                column_aliases={},
                table_version=1,
                provenance_id="tbl-prov",
                parquet_path=None,
            )
        ],
        catalog_path=None,
        sqlite_path=None,
    )

    monkeypatch.setattr(
        ca,
        "_collect_datasets_for_file",
        lambda file_obj: (dataset, {"sheet_1": pd.DataFrame({"x": [1, 2, 3]})}),
    )

    async def fake_codegen(**kwargs):  # noqa: ANN003
        _ = kwargs
        return "result = {'status': 'ok', 'metrics': {}, 'notes': [], 'artifacts': []}", {
            "code_source": "template",
            "codegen_status": "fallback",
            "codegen_error": "missing_visualization_contract",
        }

    def fake_execute_sync(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {
            "status": "ok",
            "final_response": "template result",
            "sources": [],
            "artifacts": [],
            "debug": {
                "complex_analytics": {
                    "metrics": {},
                    "notes": [],
                    "stdout": "",
                    "code_preview": "",
                    "response_status": "not_attempted",
                }
            },
        }

    async def fake_compose(**kwargs):  # noqa: ANN003
        _ = kwargs
        return "", {"response_status": "fallback", "response_error_code": "empty_response"}

    monkeypatch.setattr(ca, "_generate_complex_analysis_code", fake_codegen)
    monkeypatch.setattr(ca, "_execute_complex_analytics_sync", fake_execute_sync)
    monkeypatch.setattr(ca, "_compose_complex_analytics_response", fake_compose)
    monkeypatch.setattr(ca.settings, "COMPLEX_ANALYTICS_ALLOW_TEMPLATE_FALLBACK", True)

    result = asyncio.run(
        ca.execute_complex_analytics_path(
            query="run python analytics with charts",
            files=[SimpleNamespace(id="f1", file_type="xlsx", original_filename="x.xlsx", custom_metadata={})],
            model_source="local",
            provider_mode="explicit",
            model_name="llama3.2",
        )
    )
    assert result is not None
    assert result["status"] == "ok"
    assert result["final_response"] == "template result"


def test_broad_query_uses_local_formatter_without_compose_call(monkeypatch):
    dataset = ResolvedTabularDataset(
        engine="duckdb_parquet",
        dataset_id="ds-broad",
        dataset_version=1,
        dataset_provenance_id="prov-broad",
        tables=[
            ResolvedTabularTable(
                table_name="sheet_1",
                sheet_name="Sheet1",
                row_count=4,
                columns=["amount", "segment"],
                column_aliases={},
                table_version=1,
                provenance_id="tbl-prov",
                parquet_path=None,
            )
        ],
        catalog_path=None,
        sqlite_path=None,
    )

    monkeypatch.setattr(
        ca,
        "_collect_datasets_for_file",
        lambda file_obj: (dataset, {"sheet_1": pd.DataFrame({"amount": [1, 2, 3, 4], "segment": ["a", "b", "a", "c"]})}),
    )

    async def fake_codegen(**kwargs):  # noqa: ANN003
        _ = kwargs
        return "result = {'status': 'ok', 'metrics': {}, 'notes': [], 'artifacts': []}", {
            "code_source": "template",
            "codegen_status": "fallback",
            "codegen_error": "missing_visualization_contract",
        }

    def fake_execute_sync(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {
            "status": "ok",
            "final_response": "## Full Analytics Report\n### 1) Summary\n- Rows: 4",
            "sources": [],
            "artifacts": [],
            "debug": {
                "complex_analytics": {
                    "metrics": {},
                    "notes": [],
                    "stdout": "",
                    "code_preview": "",
                    "response_status": "not_attempted",
                }
            },
        }

    async def fail_compose(**kwargs):  # noqa: ANN003
        raise AssertionError(f"compose should not be called for broad query: {kwargs}")

    monkeypatch.setattr(ca, "_generate_complex_analysis_code", fake_codegen)
    monkeypatch.setattr(ca, "_execute_complex_analytics_sync", fake_execute_sync)
    monkeypatch.setattr(ca, "_compose_complex_analytics_response", fail_compose)
    monkeypatch.setattr(ca.settings, "COMPLEX_ANALYTICS_PREFER_LOCAL_COMPOSER_FOR_BROAD_QUERY", True)

    result = asyncio.run(
        ca.execute_complex_analytics_path(
            query="Answer as senior analyst and analyze this file fully with feature relationships and charts",
            files=[SimpleNamespace(id="f1", file_type="xlsx", original_filename="x.xlsx", custom_metadata={})],
            model_source="local",
            provider_mode="explicit",
            model_name="llama3.2",
        )
    )
    assert result is not None
    assert result["status"] == "ok"
    assert result["debug"]["complex_analytics"]["response_status"] == "fallback"
    assert result["debug"]["complex_analytics"]["response_error_code"] == "broad_query_local_formatter"

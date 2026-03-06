import asyncio
import json
import re
import uuid
import warnings
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.schemas.chat import ChatMessage
from app.observability.metrics import reset_metrics, snapshot_metrics
from app.services.chat.complex_analytics import executor as ca
from app.services.chat.complex_analytics import execute_complex_analytics_path
from app.services.chat.tabular_sql import execute_tabular_sql_path
from app.services.chat_orchestrator import ChatOrchestrator
from app.services.tabular.storage_adapter import SharedDuckDBParquetStorageAdapter


def _write_csv(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows), encoding="utf-8")


def _install_mock_complex_analytics_llm(monkeypatch):
    async def fake_generate_response(**kwargs):  # noqa: ANN003
        policy_class = str(kwargs.get("policy_class") or "")
        prompt = str(kwargs.get("prompt") or "")
        if policy_class == "complex_analytics_plan":
            prompt_lower = prompt.lower()
            expects_visualization = any(
                token in prompt_lower for token in ("heatmap", "chart", "plot", "граф", "диаграм", "визуал")
            )
            expects_dependency = any(token in prompt_lower for token in ("dependency", "correlation", "зависим", "коррел"))
            expects_nlp = any(token in prompt_lower for token in ("comment_text", "nlp", "коммент", "текст"))
            return {
                "response": json.dumps(
                    {
                        "analysis_goal": "Advanced tabular analysis with metrics and visuals",
                        "required_artifacts": ["plots", "metrics"],
                        "required_outputs": ["summary", "metrics", "insights", "artifacts"],
                        "data_contract": {
                            "required_inputs": ["datasets", "dataset_name", "file_ids"],
                            "required_outputs": ["result", "artifacts", "insights", "metrics", "tables"],
                        },
                        "required_contract": {
                            "expects_visualization": expects_visualization,
                            "expects_dependency": expects_dependency,
                            "expects_nlp": expects_nlp,
                        },
                        "python_generation_prompt": "Generate robust pandas analysis using datasets with charts when possible.",
                        "should_generate_code": True,
                    },
                    ensure_ascii=False,
                ),
                "model_route": "ollama",
                "provider_effective": "ollama",
            }
        if policy_class == "complex_analytics_codegen":
            return {
                "response": """
import pandas as pd
import matplotlib.pyplot as plt
import warnings

table_name = list(datasets.keys())[0]
df = datasets[table_name].copy()
result = {
    "status": "ok",
    "table_name": table_name,
    "metrics": {
        "rows_total": int(len(df)),
        "columns_total": int(len(df.columns)),
        "columns": [str(c) for c in df.columns],
        "insights": [],
        "numeric_summary": [],
        "categorical_summary": [],
        "datetime_summary": [],
    },
    "notes": [],
    "artifacts": [],
    "insights": [],
}

if "comment_text" in df.columns:
    tokens = (
        df["comment_text"]
        .astype(str)
        .str.lower()
        .str.replace(r"[^a-zа-я0-9\\s]", " ", regex=True)
        .str.split()
        .explode()
    )
    if tokens is not None:
        tokens = tokens[tokens.str.len() > 2]
    if tokens is not None and len(tokens) > 0:
        top_tokens = tokens.value_counts().head(10)
        result["metrics"]["comment_top_tokens"] = {str(k): int(v) for k, v in top_tokens.items()}

numeric_df = df.apply(pd.to_numeric, errors="coerce")
numeric_cols = [str(c) for c in numeric_df.columns if int(numeric_df[c].notna().sum()) >= max(2, int(len(df) * 0.3))]
for col in numeric_cols[:6]:
    clean = numeric_df[col].dropna()
    if len(clean) > 0:
        result["metrics"]["numeric_summary"].append(
            {"column": col, "min": float(clean.min()), "max": float(clean.max()), "mean": float(clean.mean())}
        )

figure_saved = False
if len(numeric_cols) >= 2:
    corr = numeric_df[numeric_cols].corr(numeric_only=True)
    if corr is not None and not corr.empty:
        fig, ax = plt.subplots(figsize=(6, 5))
        image = ax.imshow(corr.to_numpy(), cmap="RdBu_r")
        ax.figure.colorbar(image, ax=ax)
        ax.set_xticks(range(len(corr.columns)))
        ax.set_xticklabels([str(x) for x in corr.columns], rotation=45, ha="right")
        ax.set_yticks(range(len(corr.index)))
        ax.set_yticklabels([str(x) for x in corr.index])
        ax.set_title("Dependency heatmap")
        path = save_plot(fig=fig, name="dependency_heatmap.png")
        plt.close(fig)
        result["artifacts"].append({"kind": "heatmap", "path": path, "title": "Dependency heatmap"})
        result["metrics"]["insights"].append("Correlation dependency heatmap generated.")
        figure_saved = True

if (not figure_saved) and len(numeric_cols) >= 1:
    col = numeric_cols[0]
    clean = numeric_df[col].dropna()
    if len(clean) > 0:
        fig, ax = plt.subplots(figsize=(7, 4))
        clean.plot(kind="hist", bins=12, ax=ax, title=f"Distribution of {col}")
        path = save_plot(fig=fig, name="numeric_distribution.png")
        plt.close(fig)
        result["artifacts"].append({"kind": "histogram", "path": path, "column": col})
        result["metrics"]["insights"].append(f"Distribution chart generated for {col}.")
        figure_saved = True

if not figure_saved:
    for col in df.columns:
        values = df[col].dropna().astype(str).str.strip()
        values = values[values != ""]
        if len(values) == 0:
            continue
        top = values.value_counts().head(8)
        if len(top) <= 1:
            continue
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.bar([str(x) for x in top.index], [int(v) for v in top.values], color="#4c78a8")
        ax.tick_params(axis="x", labelrotation=45)
        ax.set_title(f"Top values in {col}")
        path = save_plot(fig=fig, name=f"top_values_{str(col)}.png")
        plt.close(fig)
        result["artifacts"].append({"kind": "categorical_bar", "path": path, "column": str(col)})
        result["metrics"]["insights"].append(f"Top-values chart generated for {col}.")
        figure_saved = True
        break

if not figure_saved:
    result["notes"].append("Visualization requested but no suitable columns were found for charting.")
""",
                "model_route": "ollama",
                "provider_effective": "ollama",
            }
        if policy_class == "complex_analytics_response":
            if re.search(r"[\u0400-\u04FF]", prompt):
                return {
                    "response": (
                        "Полный аналитический отчет\n"
                        "Контекст датасета: анализ выполнен.\n"
                        "Ключевые выводы: построены метрики и графики.\n"
                        "Графики: см. артефакты ниже."
                    ),
                    "model_route": "ollama",
                    "provider_effective": "ollama",
                }
            return {
                "response": (
                    "Full analytics report\n"
                    "Dataset context: analysis completed.\n"
                    "Key insights: metrics and visual artifacts were generated."
                ),
                "model_route": "ollama",
                "provider_effective": "ollama",
            }
        return {
            "response": "ok",
            "model_route": "ollama",
            "provider_effective": "ollama",
        }

    monkeypatch.setattr(ca.llm_manager, "generate_response", fake_generate_response)


def test_complex_analytics_happy_path_generates_metrics_and_artifact(tmp_path: Path, monkeypatch):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")
    _install_mock_complex_analytics_llm(monkeypatch)

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "comments.csv"
    _write_csv(
        csv_path,
        [
            "application_id,comment_time,comment_text,office",
            "a1,2026-01-01 10:00:00,Need urgent approval,EKB",
            "a2,2026-01-01 11:00:00,Delayed due to docs,MSK",
            "a3,2026-01-02 09:30:00,Client asked to resubmit,EKB",
            "a4,2026-01-03 12:15:00,All checks passed,SPB",
        ],
    )
    dataset = adapter.ingest(
        file_id="complex-1",
        file_path=csv_path,
        file_type="csv",
        source_filename="comments.csv",
    )
    assert dataset is not None

    file_obj = SimpleNamespace(
        id="complex-1",
        file_type="csv",
        original_filename="comments.csv",
        custom_metadata={"tabular_dataset": dataset},
    )

    result = asyncio.run(
        execute_complex_analytics_path(
            query="Use Python pandas and NLP on comment_text, then build a heatmap by office and comment_time.",
            files=[file_obj],
        )
    )
    assert result is not None
    assert result["status"] == "ok"
    assert result["debug"]["execution_route"] == "complex_analytics"
    assert result["debug"]["executor_status"] == "success"
    assert int(result["debug"]["artifacts_count"]) >= 1
    assert str(result.get("final_response") or "").strip()
    assert result["sources"]
    artifacts = result.get("artifacts") or []
    assert artifacts
    assert Path(str(artifacts[0]["path"])).exists()
    assert Path(str(artifacts[0]["path"])).is_absolute() is False
    assert str(artifacts[0].get("url") or "").startswith("/uploads/")
    assert "D:\\" not in result["final_response"]


def test_deterministic_sql_regression_still_works_for_simple_count(tmp_path: Path):
    pytest.importorskip("duckdb")

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "rows.csv"
    _write_csv(
        csv_path,
        [
            "city,amount",
            "ekb,10",
            "msk,20",
            "spb,30",
        ],
    )
    dataset = adapter.ingest(
        file_id="det-1",
        file_path=csv_path,
        file_type="csv",
        source_filename="rows.csv",
    )
    assert dataset is not None

    file_obj = SimpleNamespace(
        id="det-1",
        file_type="csv",
        original_filename="rows.csv",
        custom_metadata={"tabular_dataset": dataset},
    )
    result = asyncio.run(execute_tabular_sql_path(query="How many rows in the whole file?", files=[file_obj]))
    assert result is not None
    assert result["status"] == "ok"
    assert result["debug"]["retrieval_mode"] == "tabular_sql"
    assert result["debug"]["tabular_sql"]["policy_decision"]["allowed"] is True


def test_complex_analytics_emits_success_and_artifact_metrics(tmp_path: Path, monkeypatch):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")
    _install_mock_complex_analytics_llm(monkeypatch)
    reset_metrics()

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "metrics.csv"
    _write_csv(
        csv_path,
        [
            "application_id,comment_time,comment_text,office",
            "a1,2026-01-01 10:00:00,Need callback,EKB",
            "a2,2026-01-02 11:00:00,All good,MSK",
        ],
    )
    dataset = adapter.ingest(
        file_id="metrics-1",
        file_path=csv_path,
        file_type="csv",
        source_filename="metrics.csv",
    )
    assert dataset is not None
    file_obj = SimpleNamespace(
        id="metrics-1",
        file_type="csv",
        original_filename="metrics.csv",
        custom_metadata={"tabular_dataset": dataset},
    )

    result = asyncio.run(
        execute_complex_analytics_path(
            query="Run python pandas analysis and build chart",
            files=[file_obj],
        )
    )
    assert result is not None
    assert result["status"] == "ok"

    counters = snapshot_metrics().get("counters", {})
    assert any(key.startswith("complex_analytics_executor_success_total") for key in counters.keys())
    assert any(key.startswith("complex_analytics_artifacts_generated_total") for key in counters.keys())


def test_complex_analytics_generates_categorical_chart_when_no_numeric_columns(tmp_path: Path, monkeypatch):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")
    _install_mock_complex_analytics_llm(monkeypatch)

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "strings_only.csv"
    _write_csv(
        csv_path,
        [
            "office,status,comment_text",
            "EKB,approved,Need callback",
            "MSK,pending,Waiting docs",
            "EKB,pending,Escalated",
            "SPB,approved,Done",
            "EKB,pending,Follow-up",
        ],
    )
    dataset = adapter.ingest(
        file_id="cat-1",
        file_path=csv_path,
        file_type="csv",
        source_filename="strings_only.csv",
    )
    assert dataset is not None
    file_obj = SimpleNamespace(
        id="cat-1",
        file_type="csv",
        original_filename="strings_only.csv",
        custom_metadata={"tabular_dataset": dataset},
    )

    result = asyncio.run(
        execute_complex_analytics_path(
            query="Run python analysis and build charts for this file",
            files=[file_obj],
        )
    )
    assert result is not None
    assert result["status"] == "ok"
    assert int(result["debug"]["artifacts_count"]) >= 1
    assert any(str(a.get("kind")) in {"categorical_bar", "histogram", "heatmap"} for a in result.get("artifacts", []))
    assert any(str(a.get("url") or "").startswith("/uploads/") for a in result.get("artifacts", []))


def test_complex_analytics_datetime_parsing_does_not_emit_infer_format_warnings(tmp_path: Path, monkeypatch):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")
    _install_mock_complex_analytics_llm(monkeypatch)

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "requests.csv"
    _write_csv(
        csv_path,
        [
            "request_id,created_at,client_name,city,product,amount_rub,status,priority",
            "REQ-1,2026-01-05 12:00:00,Client 1,Moscow,Hosting,1000.5,new,high",
            "REQ-2,2026-01-06 13:30:00,Client 2,Kazan,VPN,2500.0,approved,medium",
            "REQ-3,2026-01-07 16:45:00,Client 3,SPB,CRM,1800.0,rejected,low",
        ],
    )
    dataset = adapter.ingest(
        file_id="warn-1",
        file_path=csv_path,
        file_type="csv",
        source_filename="requests.csv",
    )
    assert dataset is not None
    file_obj = SimpleNamespace(
        id="warn-1",
        file_type="csv",
        original_filename="requests.csv",
        custom_metadata={"tabular_dataset": dataset},
    )

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        result = asyncio.run(
            execute_complex_analytics_path(
                query="Сделай полный анализ и построй графики",
                files=[file_obj],
            )
        )

    assert result is not None
    assert result["status"] == "ok"
    assert not any("Could not infer format" in str(w.message) for w in captured)


def test_complex_analytics_uses_llm_codegen_for_dependency_request(tmp_path: Path, monkeypatch):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")

    captured_kwargs = {}

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        captured_kwargs.update(kwargs)
        policy_class = str(kwargs.get("policy_class") or "")
        if policy_class == "complex_analytics_plan":
            return {
                "response": """
{
  "analysis_goal": "dependency analysis",
  "required_artifacts": ["plots", "metrics"],
  "required_outputs": ["summary", "metrics", "insights", "artifacts"],
  "data_contract": {"required_inputs": ["datasets"], "required_outputs": ["result", "artifacts", "metrics"]},
  "required_contract": {"expects_visualization": true, "expects_dependency": true, "expects_nlp": false},
  "python_generation_prompt": "Generate dependency heatmap code.",
  "should_generate_code": true
}
""",
                "model_route": "ollama",
                "provider_effective": "ollama",
            }
        if policy_class == "complex_analytics_codegen":
            return {
                "response": """
import pandas as pd
import matplotlib.pyplot as plt
table_name = list(datasets.keys())[0]
df = datasets[table_name].copy()
numeric_df = df.apply(pd.to_numeric, errors="coerce")
numeric_cols = [str(c) for c in numeric_df.columns if int(numeric_df[c].notna().sum()) > 0]
if len(numeric_cols) >= 2:
    corr = numeric_df[numeric_cols].corr(numeric_only=True)
    fig, ax = plt.subplots(figsize=(6, 5))
    image = ax.imshow(corr.to_numpy(), cmap="RdBu_r")
    ax.figure.colorbar(image, ax=ax)
    ax.set_xticks(range(len(corr.columns)))
    ax.set_xticklabels([str(x) for x in corr.columns], rotation=45, ha="right")
    ax.set_yticks(range(len(corr.index)))
    ax.set_yticklabels([str(x) for x in corr.index])
    ax.set_title("Dependency heatmap")
    plot_path = save_plot(fig=fig, name="dependency_heatmap.png")
    plt.close(fig)
    artifacts = [{"kind": "heatmap", "path": plot_path, "title": "Dependency heatmap"}]
else:
    artifacts = []
result = {
    "status": "ok",
    "table_name": table_name,
    "metrics": {
        "rows_total": int(len(df)),
        "columns_total": int(len(df.columns)),
        "columns": [str(c) for c in df.columns],
        "insights": ["Dependencies analyzed via numeric correlation matrix"]
    },
    "notes": [],
    "artifacts": artifacts
}
""",
                "model_route": "ollama",
                "provider_effective": "ollama",
            }
        return {
            "response": "Dependency report",
            "model_route": "ollama",
            "provider_effective": "ollama",
        }

    monkeypatch.setattr(ca.llm_manager, "generate_response", fake_generate_response)
    monkeypatch.setattr(ca.settings, "COMPLEX_ANALYTICS_CODEGEN_ENABLED", True)
    monkeypatch.setattr(ca.settings, "COMPLEX_ANALYTICS_CODEGEN_FORCE_LOCAL", True)

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "dependency.csv"
    _write_csv(
        csv_path,
        [
            "a,b,c",
            "1,10,100",
            "2,20,80",
            "3,30,60",
            "4,40,40",
            "5,50,20",
        ],
    )
    dataset = adapter.ingest(
        file_id="dep-1",
        file_path=csv_path,
        file_type="csv",
        source_filename="dependency.csv",
    )
    assert dataset is not None
    file_obj = SimpleNamespace(
        id="dep-1",
        file_type="csv",
        original_filename="dependency.csv",
        custom_metadata={"tabular_dataset": dataset},
    )

    result = asyncio.run(
        execute_complex_analytics_path(
            query="Проведи анализ зависимостей и построй тепловую диаграмму",
            files=[file_obj],
            model_source="local",
            provider_mode="explicit",
            model_name="llama3.2",
        )
    )

    assert result is not None
    assert result["status"] == "ok"
    assert any(str(a.get("kind") or "") == "heatmap" for a in (result.get("artifacts") or []))
    assert result["debug"]["complex_analytics"]["code_source"] == "llm"
    assert result["debug"]["complex_analytics"]["codegen_auto_visual_patch_applied"] is False
    assert result["debug"]["complex_analytics"]["complex_analytics_codegen"]["auto_visual_patch_applied"] is False
    assert captured_kwargs.get("model_source") == "ollama"
    assert captured_kwargs.get("provider_mode") == "explicit"


def test_complex_analytics_auto_visual_patch_prevents_codegen_failure(tmp_path: Path, monkeypatch):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        policy_class = str(kwargs.get("policy_class") or "")
        if policy_class == "complex_analytics_plan":
            return {
                "response": """
{
  "analysis_goal": "full dataset analysis",
  "required_artifacts": ["plots", "metrics"],
  "required_outputs": ["summary", "metrics", "artifacts"],
  "data_contract": {"required_inputs": ["datasets"], "required_outputs": ["result"]},
  "required_contract": {"expects_visualization": true, "expects_dependency": false, "expects_nlp": false},
  "python_generation_prompt": "Generate full analytics report with distribution plots.",
  "should_generate_code": true
}
""",
                "model_route": "ollama",
                "provider_effective": "ollama",
            }
        if policy_class == "complex_analytics_codegen":
            return {
                "response": """
table_name = list(datasets.keys())[0]
df = datasets[table_name].copy()
result = {
    "status": "ok",
    "table_name": table_name,
    "metrics": {
        "rows_total": int(len(df)),
        "columns_total": int(len(df.columns)),
        "columns": [str(c) for c in df.columns]
    },
    "notes": [],
    "artifacts": []
}
""",
                "model_route": "ollama",
                "provider_effective": "ollama",
            }
        return {
            "response": "Full report generated",
            "model_route": "ollama",
            "provider_effective": "ollama",
        }

    monkeypatch.setattr(ca.llm_manager, "generate_response", fake_generate_response)

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "auto_visual.csv"
    _write_csv(
        csv_path,
        [
            "a,b,c",
            "1,10,x",
            "2,20,y",
            "3,30,z",
            "4,40,x",
        ],
    )
    dataset = adapter.ingest(
        file_id="auto-visual-1",
        file_path=csv_path,
        file_type="csv",
        source_filename="auto_visual.csv",
    )
    assert dataset is not None
    file_obj = SimpleNamespace(
        id="auto-visual-1",
        file_type="csv",
        original_filename="auto_visual.csv",
        custom_metadata={"tabular_dataset": dataset},
    )

    result = asyncio.run(
        execute_complex_analytics_path(
            query="Сделай полный анализ файла и построй красивые графики распределений",
            files=[file_obj],
            model_source="local",
            provider_mode="explicit",
            model_name="llama3.2",
        )
    )

    assert result is not None
    assert result["status"] == "ok"
    assert int(result["debug"]["artifacts_count"]) >= 1
    assert result["debug"]["complex_analytics"]["code_source"] == "llm"
    assert result["debug"]["complex_analytics"]["codegen_auto_visual_patch_applied"] is True
    assert result["debug"]["complex_analytics"]["complex_analytics_codegen"]["auto_visual_patch_applied"] is True


def test_complex_analytics_compose_quality_gate_falls_back_to_local_formatter(tmp_path: Path, monkeypatch):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")

    async def fake_generate_response(**kwargs):  # noqa: ANN003
        policy_class = str(kwargs.get("policy_class") or "")
        if policy_class == "complex_analytics_plan":
            return {
                "response": """
{
  "analysis_goal": "full analysis",
  "required_artifacts": ["plots", "metrics"],
  "required_outputs": ["summary", "metrics", "artifacts"],
  "data_contract": {"required_inputs": ["datasets"], "required_outputs": ["result"]},
  "required_contract": {"expects_visualization": true, "expects_dependency": false, "expects_nlp": false},
  "python_generation_prompt": "Generate full analytics with chart.",
  "should_generate_code": true
}
""",
                "model_route": "ollama",
                "provider_effective": "ollama",
            }
        if policy_class == "complex_analytics_codegen":
            return {
                "response": """
import pandas as pd
import matplotlib.pyplot as plt
table_name = list(datasets.keys())[0]
df = datasets[table_name].copy()
num = df.apply(pd.to_numeric, errors="coerce")
col = str(num.columns[0])
clean = num[col].dropna()
fig, ax = plt.subplots(figsize=(6, 3))
clean.plot(kind="hist", bins=10, ax=ax, title=f"Distribution of {col}")
plot_path = save_plot(fig=fig, name="quality_gate_hist.png")
plt.close(fig)
result = {
    "status": "ok",
    "table_name": table_name,
    "metrics": {
        "rows_total": int(len(df)),
        "columns_total": int(len(df.columns)),
        "columns": [str(c) for c in df.columns],
        "numeric_summary": [{"column": col, "count": int(len(clean))}],
        "datetime_summary": [],
        "categorical_summary": []
    },
    "notes": [],
    "artifacts": [{"kind": "histogram", "path": plot_path, "column": col}]
}
""",
                "model_route": "ollama",
                "provider_effective": "ollama",
            }
        if policy_class == "complex_analytics_response":
            return {
                "response": "Done.",
                "model_route": "ollama",
                "provider_effective": "ollama",
            }
        return {"response": "ok", "model_route": "ollama", "provider_effective": "ollama"}

    monkeypatch.setattr(ca.llm_manager, "generate_response", fake_generate_response)

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "compose_quality.csv"
    _write_csv(
        csv_path,
        [
            "a,b,c",
            "1,10,x",
            "2,20,y",
            "3,30,z",
            "4,40,x",
        ],
    )
    dataset = adapter.ingest(
        file_id="compose-quality-1",
        file_path=csv_path,
        file_type="csv",
        source_filename="compose_quality.csv",
    )
    assert dataset is not None
    file_obj = SimpleNamespace(
        id="compose-quality-1",
        file_type="csv",
        original_filename="compose_quality.csv",
        custom_metadata={"tabular_dataset": dataset},
    )

    result = asyncio.run(
        execute_complex_analytics_path(
            query="Analyze this file fully, include stats and charts",
            files=[file_obj],
            model_source="local",
            provider_mode="explicit",
            model_name="llama3.2",
        )
    )

    assert result is not None
    assert result["status"] == "ok"
    assert int(result["debug"]["artifacts_count"]) >= 1
    assert result["debug"]["complex_analytics"]["response_status"] == "fallback"
    assert result["debug"]["complex_analytics"]["response_error_code"] == "broad_query_local_formatter"
    final_response = str(result.get("final_response") or "")
    assert "## Full Analytics Report" in final_response
    assert "### 4) Metrics and Statistics" in final_response
    assert "### 7) Visualizations" in final_response
    assert "Request was processed" not in final_response


def test_explicit_local_with_complex_short_circuit_does_not_call_llm(monkeypatch):
    orchestrator = ChatOrchestrator()

    async def fake_prepare_context(*, chat_data, db, user_id):  # noqa: ARG001
        return {
            "conversation_id": uuid.uuid4(),
            "provider_source_selected_raw": "local",
            "provider_source_effective": "ollama",
            "provider_model_effective": "llama3.2",
            "provider_mode": "explicit",
            "final_prompt": "unused",
            "rag_used": False,
            "rag_debug": {
                "execution_route": "complex_analytics",
                "executor_attempted": True,
                "executor_status": "success",
                "executor_error_code": None,
                "artifacts_count": 1,
                "short_circuit_response": True,
                "short_circuit_response_text": "complex analytics result",
            },
            "context_docs": [],
            "rag_caveats": [],
            "rag_sources": ["comments.csv | complex_analytics"],
            "history_for_generation": [],
            "preferred_lang": "en",
        }

    async def fake_create_message(
        db,  # noqa: ARG001
        conversation_id,  # noqa: ARG001
        role,  # noqa: ARG001
        content,  # noqa: ARG001
        model_name,  # noqa: ARG001
        temperature,  # noqa: ARG001
        max_tokens,  # noqa: ARG001
        tokens_used=None,  # noqa: ARG001
        generation_time=None,  # noqa: ARG001
    ):
        return SimpleNamespace(id=uuid.uuid4())

    async def fail_generate_response(**kwargs):  # noqa: ANN003
        raise AssertionError(f"LLM should not be called: {kwargs}")

    monkeypatch.setattr(orchestrator, "_prepare_request_context", fake_prepare_context)
    monkeypatch.setattr("app.services.chat_orchestrator.crud_message.create_message", fake_create_message)
    monkeypatch.setattr("app.services.chat_orchestrator.llm_manager.generate_response", fail_generate_response)

    response = asyncio.run(
        orchestrator.chat(
            chat_data=ChatMessage(
                message="run python pandas analytics",
                model_source="local",
                provider_mode="explicit",
            ),
            db=object(),
            current_user=None,
        )
    )
    assert response.response == "complex analytics result"
    assert response.execution_route == "complex_analytics"
    assert response.executor_attempted is True
    assert response.executor_status == "success"
    assert response.artifacts_count == 1
    assert response.aihub_attempted is False

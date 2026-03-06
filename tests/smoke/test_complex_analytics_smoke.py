import asyncio
import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.chat import rag_prompt_builder as rag_builder
from app.services.chat import complex_analytics as ca
from app.services.tabular.storage_adapter import SharedDuckDBParquetStorageAdapter


def _write_csv(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows), encoding="utf-8")


def _install_mock_complex_analytics_llm(monkeypatch):
    async def fake_generate_response(**kwargs):  # noqa: ANN003
        policy_class = str(kwargs.get("policy_class") or "")
        if policy_class == "complex_analytics_plan":
            return {
                "response": json.dumps(
                    {
                        "analysis_goal": "smoke plan",
                        "required_artifacts": ["plots", "metrics"],
                        "required_outputs": ["summary", "metrics", "insights", "artifacts"],
                        "data_contract": {"required_inputs": ["datasets"], "required_outputs": ["result", "artifacts"]},
                        "required_contract": {"expects_visualization": True, "expects_dependency": True, "expects_nlp": True},
                        "python_generation_prompt": "Generate chart-focused code.",
                        "should_generate_code": True,
                    }
                ),
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
result = {
    "status": "ok",
    "table_name": table_name,
    "metrics": {
        "rows_total": int(len(df)),
        "columns_total": int(len(df.columns)),
        "columns": [str(c) for c in df.columns],
        "insights": ["Smoke dependency analysis finished"]
    },
    "notes": [],
    "artifacts": []
}
numeric_df = df.apply(pd.to_numeric, errors="coerce")
numeric_cols = [str(c) for c in numeric_df.columns if int(numeric_df[c].notna().sum()) > 0]
if len(numeric_cols) >= 1:
    col = numeric_cols[0]
    clean = numeric_df[col].dropna()
    if len(clean) > 0:
        fig, ax = plt.subplots(figsize=(7, 4))
        clean.plot(kind="hist", bins=10, ax=ax, title=f"Distribution of {col}")
        path = save_plot(fig=fig, name="smoke_distribution.png")
        plt.close(fig)
        result["artifacts"].append({"kind": "histogram", "path": path, "column": col})
if not result["artifacts"]:
    for col in df.columns:
        values = df[col].dropna().astype(str).str.strip()
        values = values[values != ""]
        if len(values) == 0:
            continue
        top = values.value_counts().head(8)
        if len(top) <= 1:
            continue
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.bar([str(x) for x in top.index], [int(v) for v in top.values], color="#4c78a8")
        ax.tick_params(axis="x", labelrotation=45)
        ax.set_title(f"Top values in {col}")
        path = save_plot(fig=fig, name=f"smoke_top_{str(col)}.png")
        plt.close(fig)
        result["artifacts"].append({"kind": "categorical_bar", "path": path, "column": str(col)})
        break
""",
                "model_route": "ollama",
                "provider_effective": "ollama",
            }
        if policy_class == "complex_analytics_response":
            return {
                "response": "Full analytics report\nCharts and metrics generated.",
                "model_route": "ollama",
                "provider_effective": "ollama",
            }
        return {"response": "ok", "model_route": "ollama", "provider_effective": "ollama"}

    monkeypatch.setattr(ca.llm_manager, "generate_response", fake_generate_response)


def test_complex_analytics_request_routes_to_executor_with_artifacts(tmp_path: Path, monkeypatch):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")
    _install_mock_complex_analytics_llm(monkeypatch)

    adapter = SharedDuckDBParquetStorageAdapter(
        dataset_root=tmp_path / "datasets",
        catalog_path=tmp_path / "catalog.duckdb",
    )
    csv_path = tmp_path / "smoke_comments.csv"
    _write_csv(
        csv_path,
        [
            "application_id,comment_time,comment_text,office",
            "a1,2026-02-01 10:00:00,Need callback,EKB",
            "a2,2026-02-02 11:00:00,All good,MSK",
            "a3,2026-02-03 09:00:00,Need docs,EKB",
        ],
    )
    dataset = adapter.ingest(
        file_id="smoke-1",
        file_path=csv_path,
        file_type="csv",
        source_filename="smoke_comments.csv",
    )
    assert dataset is not None
    file_obj = SimpleNamespace(
        id=uuid.uuid4(),
        file_type="csv",
        embedding_model="local:nomic-embed-text",
        chunks_count=6,
        is_processed="completed",
        original_filename="smoke_comments.csv",
        custom_metadata={"tabular_dataset": dataset},
    )

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [file_obj]

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=uuid.uuid4(),
            conversation_id=uuid.uuid4(),
            query="Run Python/pandas NLP on comment_text and generate heatmap by office and comment_time",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert context_docs == []
    assert rag_caveats == []
    assert str(final_prompt or "").strip()
    assert rag_debug["execution_route"] == "complex_analytics"
    assert rag_debug["executor_status"] == "success"
    assert int(rag_debug["artifacts_count"]) >= 1
    assert any(str(a.get("url") or "").startswith("/uploads/") for a in (rag_debug.get("artifacts") or []))
    assert rag_debug["short_circuit_response"] is True
    assert rag_sources

import asyncio
import uuid
import warnings
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.schemas.chat import ChatMessage
from app.observability.metrics import reset_metrics, snapshot_metrics
from app.services.chat import complex_analytics as ca
from app.services.chat.complex_analytics import execute_complex_analytics_path
from app.services.chat.tabular_sql import execute_tabular_sql_path
from app.services.chat_orchestrator import ChatOrchestrator
from app.services.tabular.storage_adapter import SharedDuckDBParquetStorageAdapter


def _write_csv(path: Path, rows: list[str]) -> None:
    path.write_text("\n".join(rows), encoding="utf-8")


def test_complex_analytics_happy_path_generates_metrics_and_artifact(tmp_path: Path):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")

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
    assert "Full Analytics Report" in result["final_response"]
    assert "```python" in result["final_response"]
    assert result["sources"]
    artifacts = result.get("artifacts") or []
    assert artifacts
    assert Path(str(artifacts[0]["path"])).exists()
    assert Path(str(artifacts[0]["path"])).is_absolute() is False
    assert str(artifacts[0].get("url") or "").startswith("/uploads/")
    assert "](/uploads/" in result["final_response"]
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


def test_complex_analytics_emits_success_and_artifact_metrics(tmp_path: Path):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")
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


def test_complex_analytics_generates_categorical_chart_when_no_numeric_columns(tmp_path: Path):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")

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


def test_complex_analytics_datetime_parsing_does_not_emit_infer_format_warnings(tmp_path: Path):
    pytest.importorskip("duckdb")
    pytest.importorskip("matplotlib")

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
        return {
            "response": """
import pandas as pd
import matplotlib.pyplot as plt
table_name = list(datasets.keys())[0]
df = datasets[table_name].copy()
numeric_df = df.apply(pd.to_numeric, errors="coerce").dropna()
if numeric_df.shape[1] >= 2 and len(numeric_df) > 1:
    corr = numeric_df.corr(numeric_only=True)
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
    assert captured_kwargs.get("model_source") == "local"
    assert captured_kwargs.get("provider_mode") == "explicit"


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

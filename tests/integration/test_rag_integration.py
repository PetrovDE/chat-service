import asyncio
import json
import re
import uuid
from types import SimpleNamespace

from app.domain.chat.query_planner import (
    INTENT_TABULAR_AGGREGATE,
    ROUTE_DETERMINISTIC_ANALYTICS,
    QueryPlanDecision,
)
from app.observability.metrics import reset_metrics, snapshot_metrics
from app.services.chat import rag_prompt_builder as rag_builder
from app.services.chat import full_file_analysis
from app.services.chat.full_file_analysis import build_full_file_map_reduce_prompt
from app.rag.retriever import rag_retriever


def test_mixed_embedding_groups_merge(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    files = [
        SimpleNamespace(id=uuid.uuid4(), embedding_model="local:nomic-embed-text:latest"),
        SimpleNamespace(id=uuid.uuid4(), embedding_model="aihub:arctic"),
    ]

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return files

    async def fake_query_rag(
        query,  # noqa: ARG001
        top_k=5,  # noqa: ARG001
        fetch_k=None,  # noqa: ARG001
        conversation_id=None,  # noqa: ARG001
        user_id=None,  # noqa: ARG001
        file_ids=None,
        embedding_mode="local",
        embedding_model=None,
        score_threshold=None,  # noqa: ARG001
        debug_return=False,  # noqa: ARG001
        rag_mode=None,  # noqa: ARG001
        **kwargs,  # noqa: ARG001
    ):
        return {
            "docs": [
                {
                    "content": f"chunk-{embedding_mode}",
                    "metadata": {"file_id": file_ids[0], "chunk_index": 0, "filename": "f.txt"},
                    "similarity_score": 0.9,
                    "distance": 0.1,
                }
            ],
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid", "embedding_model": embedding_model},
        }

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="test query",
            top_k=8,
            model_source="local",
        )
    )

    assert rag_used is True
    assert isinstance(final_prompt, str) and final_prompt
    assert len(context_docs) >= 2
    assert rag_debug["mixed_embeddings"] is True
    assert rag_debug["group_count"] == 2
    assert isinstance(rag_caveats, list)
    assert isinstance(rag_sources, list)
    # Regression guard for SSE start payload serialization.
    json.dumps(rag_debug)


def test_full_file_truncation_flag(monkeypatch):
    rows = []
    for i in range(900):
        rows.append(
            {
                "id": f"id_{i}",
                "content": f"text-{i}",
                "metadata": {"file_id": "f1", "chunk_index": i, "filename": "big.txt"},
            }
        )

    monkeypatch.setattr(rag_retriever.vectorstore, "get_by_filter", lambda filter_dict=None, limit_per_collection=1000: rows)  # noqa: ARG005

    result = asyncio.run(
        rag_retriever.query_rag(
            "analyze",
            user_id="u1",
            conversation_id="c1",
            rag_mode="full_file",
            debug_return=True,
        )
    )

    debug = result["debug"]
    assert debug["retrieval_mode"] == "full_file"
    assert debug["full_file_limit_hit"] is True
    assert debug["full_file_max_chunks"] == 800


def test_mixed_group_partial_failure_fallback(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_a = SimpleNamespace(id=uuid.uuid4(), embedding_model="local:nomic-embed-text:latest")
    file_b = SimpleNamespace(id=uuid.uuid4(), embedding_model="aihub:arctic")

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [file_a, file_b]

    async def fake_query_rag(
        query,  # noqa: ARG001
        top_k=5,  # noqa: ARG001
        fetch_k=None,  # noqa: ARG001
        conversation_id=None,  # noqa: ARG001
        user_id=None,  # noqa: ARG001
        file_ids=None,
        embedding_mode="local",  # noqa: ARG001
        embedding_model=None,  # noqa: ARG001
        score_threshold=None,  # noqa: ARG001
        debug_return=False,  # noqa: ARG001
        rag_mode=None,  # noqa: ARG001
        **kwargs,  # noqa: ARG001
    ):
        if str(file_b.id) in (file_ids or []):
            raise RuntimeError("provider temporary failure")
        return {
            "docs": [
                {
                    "content": "survived-group",
                    "metadata": {"file_id": str(file_a.id), "chunk_index": 0, "filename": "ok.txt"},
                    "similarity_score": 0.8,
                    "distance": 0.2,
                }
            ],
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"},
        }

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="test query",
            top_k=8,
            model_source="local",
        )
    )

    assert rag_used is True
    assert context_docs
    assert "survived-group" in final_prompt
    assert rag_debug["mixed_embeddings"] is True
    assert isinstance(rag_caveats, list)
    assert isinstance(rag_sources, list)


def test_full_file_prompt_preserves_all_retrieved_chunks(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    total_chunks = 60

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [
            SimpleNamespace(
                id=file_id,
                embedding_model="local:nomic-embed-text:latest",
                chunks_count=total_chunks,
                is_processed="completed",
                original_filename="table.xlsx",
            )
        ]

    async def fake_query_rag(
        query,  # noqa: ARG001
        top_k=5,  # noqa: ARG001
        fetch_k=None,  # noqa: ARG001
        conversation_id=None,  # noqa: ARG001
        user_id=None,  # noqa: ARG001
        file_ids=None,  # noqa: ARG001
        embedding_mode="local",  # noqa: ARG001
        embedding_model=None,  # noqa: ARG001
        score_threshold=None,  # noqa: ARG001
        debug_return=False,  # noqa: ARG001
        rag_mode=None,  # noqa: ARG001
        **kwargs,  # noqa: ARG001
    ):
        docs = []
        for idx in range(total_chunks):
            row_start = idx * 5 + 1
            row_end = min((idx + 1) * 5, 300)
            docs.append(
                {
                    "content": f"chunk-{idx}",
                    "metadata": {
                        "file_id": str(file_id),
                        "chunk_index": idx,
                        "filename": "table.xlsx",
                        "sheet_name": "Sheet1",
                        "row_start": row_start,
                        "row_end": row_end,
                        "total_rows": 300,
                    },
                    "similarity_score": 1.0,
                    "distance": 0.0,
                }
            )
        return {
            "docs": docs,
            "debug": {"intent": "analyze_full_file", "retrieval_mode": "full_file"},
        }

    async def fake_map_reduce(**kwargs):
        docs = kwargs.get("context_documents") or []
        return "full-file prompt", {"enabled": True, "truncated_batches": False, "covered_chunks": len(docs)}

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)
    monkeypatch.setattr(rag_builder, "build_full_file_map_reduce_prompt", fake_map_reduce)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="analyze full file",
            top_k=8,
            model_source="local",
            rag_mode="full_file",
        )
    )

    assert rag_used is True
    assert final_prompt == "full-file prompt"
    assert len(context_docs) == total_chunks
    assert rag_debug["coverage"]["complete"] is True
    assert rag_debug["retrieved_chunks_total"] == total_chunks
    assert rag_debug["truncated"] is False
    assert not rag_caveats
    assert rag_sources
    assert rag_sources[0] == "table.xlsx | sheet=Sheet1 | rows=1-300"


def test_full_file_row_coverage_debug_fields(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [
            SimpleNamespace(
                id=file_id,
                embedding_model="local:nomic-embed-text:latest",
                chunks_count=4,
                is_processed="completed",
                original_filename="table.xlsx",
            )
        ]

    async def fake_query_rag(
        query,  # noqa: ARG001
        top_k=5,  # noqa: ARG001
        fetch_k=None,  # noqa: ARG001
        conversation_id=None,  # noqa: ARG001
        user_id=None,  # noqa: ARG001
        file_ids=None,  # noqa: ARG001
        embedding_mode="local",  # noqa: ARG001
        embedding_model=None,  # noqa: ARG001
        score_threshold=None,  # noqa: ARG001
        debug_return=False,  # noqa: ARG001
        rag_mode=None,  # noqa: ARG001
        **kwargs,  # noqa: ARG001
    ):
        docs = []
        for idx in range(4):
            docs.append(
                {
                    "content": f"chunk-{idx}",
                    "metadata": {
                        "file_id": str(file_id),
                        "chunk_index": idx,
                        "filename": "table.xlsx",
                        "sheet_name": "Sheet1",
                        "row_start": idx * 25 + 1,
                        "row_end": (idx + 1) * 25,
                        "total_rows": 100,
                    },
                    "similarity_score": 1.0,
                    "distance": 0.0,
                }
            )
        return {
            "docs": docs,
            "debug": {"intent": "analyze_full_file", "retrieval_mode": "full_file"},
        }

    async def fake_map_reduce(**kwargs):
        return "full-file prompt", {
            "enabled": True,
            "truncated_batches": False,
            "rows_used_map_total": 100,
            "rows_used_reduce_total": 95,
            "batch_diagnostics": [
                {
                    "batch_index": 1,
                    "batch_rows_start_end": [{"sheet_name": "Sheet1", "row_start": 1, "row_end": 100}],
                    "batch_input_chars": 2048,
                    "batch_output_chars": 512,
                }
            ],
        }

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)
    monkeypatch.setattr(rag_builder, "build_full_file_map_reduce_prompt", fake_map_reduce)

    _, rag_used, rag_debug, context_docs, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="analyze full file",
            top_k=8,
            model_source="local",
            rag_mode="full_file",
        )
    )

    assert rag_used is True
    assert len(context_docs) == 4
    assert rag_debug["rows_expected_total"] == 100
    assert rag_debug["rows_retrieved_total"] == 100
    assert rag_debug["rows_used_map_total"] == 100
    assert rag_debug["rows_used_reduce_total"] == 95
    assert rag_debug["row_coverage_ratio"] == 0.95
    assert rag_debug["full_file_map_reduce"]["batch_diagnostics"][0]["batch_input_chars"] == 2048


def test_tabular_intent_routes_to_sql_path(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [
            SimpleNamespace(
                id=file_id,
                embedding_model="local:nomic-embed-text:latest",
                file_type="xlsx",
                chunks_count=12,
                is_processed="completed",
                original_filename="table.xlsx",
                custom_metadata={
                    "tabular_dataset": {
                        "dataset_id": "ds-1",
                        "dataset_version": 1,
                        "dataset_provenance_id": "prov-1",
                        "tables": [
                            {
                                "table_name": "sheet_1",
                                "sheet_name": "Sheet1",
                                "row_count": 308,
                                "columns": ["city", "amount"],
                                "column_aliases": {},
                            }
                        ],
                    }
                },
            )
        ]

    async def fake_tabular_sql_path(*, query, files):  # noqa: ARG001
        return {
            "prompt_context": "Deterministic tabular SQL result (source of truth): sql=SELECT COUNT(*) FROM sheet_1 result=308",
            "debug": {"retrieval_mode": "tabular_sql", "intent": "tabular_aggregate", "deterministic_path": True},
            "sources": ["table.xlsx | table=sheet_1 | sql"],
            "rows_expected_total": 308,
            "rows_retrieved_total": 308,
            "rows_used_map_total": 308,
            "rows_used_reduce_total": 308,
            "row_coverage_ratio": 1.0,
        }

    async def fail_query_rag(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("query_rag should not be used for deterministic tabular aggregate path")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder, "execute_tabular_sql_path", fake_tabular_sql_path)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="Сколько всего строк в таблице?",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is True
    assert context_docs == []
    assert rag_caveats == []
    assert rag_sources == ["table.xlsx | table=sheet_1 | sql"]
    assert rag_debug["retrieval_mode"] == "tabular_sql"
    assert rag_debug["deterministic_path"] is True
    assert rag_debug["row_coverage_ratio"] == 1.0
    assert "Deterministic tabular SQL result" in final_prompt


def test_combined_tabular_route_uses_semantic_prefetch_before_sql(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_a = uuid.uuid4()
    file_b = uuid.uuid4()

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [
            SimpleNamespace(
                id=file_a,
                embedding_model="local:nomic-embed-text:latest",
                file_type="xlsx",
                chunks_count=14,
                is_processed="completed",
                original_filename="north.xlsx",
                active_processing=SimpleNamespace(id=uuid.uuid4()),
                custom_metadata={
                    "tabular_dataset": {
                        "dataset_id": "ds-a",
                        "dataset_version": 1,
                        "dataset_provenance_id": "prov-a",
                        "tables": [
                            {
                                "table_name": "north_sheet",
                                "sheet_name": "North",
                                "row_count": 120,
                                "columns": ["region", "amount"],
                                "column_aliases": {},
                            }
                        ],
                    }
                },
            ),
            SimpleNamespace(
                id=file_b,
                embedding_model="local:nomic-embed-text:latest",
                file_type="xlsx",
                chunks_count=9,
                is_processed="completed",
                original_filename="south.xlsx",
                active_processing=SimpleNamespace(id=uuid.uuid4()),
                custom_metadata={
                    "tabular_dataset": {
                        "dataset_id": "ds-b",
                        "dataset_version": 1,
                        "dataset_provenance_id": "prov-b",
                        "tables": [
                            {
                                "table_name": "south_sheet",
                                "sheet_name": "South",
                                "row_count": 80,
                                "columns": ["region", "amount"],
                                "column_aliases": {},
                            }
                        ],
                    }
                },
            ),
        ]

    async def fake_query_rag(**kwargs):  # noqa: ANN003
        assert kwargs.get("chunk_types") == ["file_summary", "sheet_summary", "row_group", "column_summary"]
        return {
            "docs": [
                {
                    "content": "North sheet summary says region North has active records",
                    "metadata": {
                        "file_id": str(file_a),
                        "filename": "north.xlsx",
                        "sheet_name": "North",
                        "chunk_type": "sheet_summary",
                    },
                    "similarity_score": 0.97,
                }
            ],
            "debug": {"retrieval_mode": "hybrid", "intent": "fact_lookup"},
        }

    async def fake_tabular_sql_path(*, query, files):  # noqa: ARG001
        assert "[combined_scope_sheet=North]" in query
        assert len(files) == 1
        assert str(files[0].id) == str(file_a)
        return {
            "status": "ok",
            "prompt_context": "Deterministic tabular SQL result (source of truth): sql=SELECT COUNT(*) ... result=42",
            "debug": {"retrieval_mode": "tabular_sql", "intent": "tabular_aggregate"},
            "sources": ["north.xlsx | table=north_sheet | sql"],
            "rows_expected_total": 120,
            "rows_retrieved_total": 42,
            "rows_used_map_total": 42,
            "rows_used_reduce_total": 42,
            "row_coverage_ratio": 0.35,
        }

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)
    monkeypatch.setattr(rag_builder, "execute_tabular_sql_path", fake_tabular_sql_path)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="На каком листе есть region North и сколько там записей?",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is True
    assert rag_caveats == []
    assert rag_sources == ["north.xlsx | table=north_sheet | sql"]
    assert context_docs
    assert rag_debug["strategy_mode"] == "combined"
    assert rag_debug["retrieval_mode"] == "tabular_combined"
    assert rag_debug["combined_scope"]["selected_sheet"] == "North"
    assert "Deterministic tabular SQL result" in final_prompt


def test_tabular_sql_error_returns_clarification_without_narrative_fallback(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [
            SimpleNamespace(
                id=file_id,
                embedding_model="local:nomic-embed-text:latest",
                file_type="xlsx",
                chunks_count=8,
                is_processed="completed",
                original_filename="table.xlsx",
                custom_metadata={
                    "tabular_dataset": {
                        "dataset_id": "ds-1",
                        "dataset_version": 1,
                        "dataset_provenance_id": "prov-1",
                        "tables": [
                            {
                                "table_name": "sheet_1",
                                "sheet_name": "Sheet1",
                                "row_count": 50,
                                "columns": ["city", "amount"],
                                "column_aliases": {},
                            }
                        ],
                    }
                },
            )
        ]

    async def fake_tabular_sql_path(*, query, files):  # noqa: ARG001
        return {
            "status": "error",
            "clarification_prompt": "Deterministic SQL execution timed out. Please narrow the filter and retry.",
            "debug": {
                "retrieval_mode": "tabular_sql",
                "intent": "tabular_aggregate",
                "deterministic_path": True,
                "deterministic_error": {"code": "sql_timeout", "category": "timeout"},
            },
            "sources": [],
            "rows_expected_total": 0,
            "rows_retrieved_total": 0,
            "rows_used_map_total": 0,
            "rows_used_reduce_total": 0,
            "row_coverage_ratio": 0.0,
        }

    async def fail_query_rag(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("query_rag should not be used when deterministic SQL path returns classified error")

    def force_deterministic_planner(*, query, files):  # noqa: ARG001
        return QueryPlanDecision(
            route=ROUTE_DETERMINISTIC_ANALYTICS,
            intent=INTENT_TABULAR_AGGREGATE,
            strategy_mode="analytical",
            confidence=0.95,
            requires_clarification=False,
            reason_codes=["test_force_deterministic"],
        )

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder, "execute_tabular_sql_path", fake_tabular_sql_path)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="Сколько всего строк в таблице?",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=force_deterministic_planner,
        )
    )

    assert rag_used is False
    assert context_docs == []
    assert rag_caveats == []
    assert rag_sources == []
    assert "не удалось выполнить детерминированный sql-запрос" in final_prompt.lower()
    assert rag_debug["requires_clarification"] is True
    assert rag_debug["deterministic_error"]["code"] == "sql_timeout"
    assert rag_debug["rag_mode_effective"] == "tabular_sql_error"


def test_tabular_sql_invalid_payload_returns_clarification_without_narrative_fallback(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [
            SimpleNamespace(
                id=file_id,
                embedding_model="local:nomic-embed-text:latest",
                file_type="xlsx",
                chunks_count=8,
                is_processed="completed",
                original_filename="table.xlsx",
                custom_metadata={
                    "tabular_dataset": {
                        "dataset_id": "ds-1",
                        "dataset_version": 1,
                        "dataset_provenance_id": "prov-1",
                        "tables": [
                            {
                                "table_name": "sheet_1",
                                "sheet_name": "Sheet1",
                                "row_count": 50,
                                "columns": ["city", "amount"],
                                "column_aliases": {},
                            }
                        ],
                    }
                },
            )
        ]

    async def invalid_tabular_sql_path(*, query, files):  # noqa: ARG001
        return "INVALID_PAYLOAD"

    async def fail_query_rag(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("query_rag should not be used when deterministic SQL payload is invalid")

    def force_deterministic_planner(*, query, files):  # noqa: ARG001
        return QueryPlanDecision(
            route=ROUTE_DETERMINISTIC_ANALYTICS,
            intent=INTENT_TABULAR_AGGREGATE,
            strategy_mode="analytical",
            confidence=0.95,
            requires_clarification=False,
            reason_codes=["test_force_deterministic"],
        )

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder, "execute_tabular_sql_path", invalid_tabular_sql_path)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="Сколько всего строк в таблице?",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=force_deterministic_planner,
        )
    )

    assert rag_used is False
    assert context_docs == []
    assert rag_caveats == []
    assert rag_sources == []
    assert "невалидный payload" in final_prompt.lower()
    assert rag_debug["requires_clarification"] is True
    assert rag_debug["executor_error_code"] == "invalid_executor_payload"
    assert rag_debug["rag_mode_effective"] == "tabular_sql_invalid_payload"


def test_metric_critical_ambiguous_query_returns_clarification(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [
            SimpleNamespace(
                id=file_id,
                embedding_model="local:nomic-embed-text:latest",
                file_type="xlsx",
                chunks_count=12,
                is_processed="completed",
                original_filename="table.xlsx",
                custom_metadata={
                    "tabular_dataset": {
                        "dataset_id": "ds-1",
                        "dataset_version": 1,
                        "dataset_provenance_id": "prov-1",
                        "tables": [
                            {
                                "table_name": "sheet_1",
                                "sheet_name": "Sheet1",
                                "row_count": 100,
                                "columns": ["region", "revenue"],
                                "column_aliases": {"revenue": "Выручка"},
                            }
                        ],
                    }
                },
            )
        ]

    async def fail_tabular_sql_path(*, query, files):  # noqa: ARG001
        raise AssertionError("execute_tabular_sql_path should not run for ambiguous metric-critical query")

    async def fail_query_rag(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("query_rag should not run for ambiguous metric-critical query")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder, "execute_tabular_sql_path", fail_tabular_sql_path)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="Какая средняя?",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert context_docs == []
    assert rag_caveats == []
    assert rag_sources == []
    assert "Уточните" in final_prompt
    assert rag_debug["requires_clarification"] is True
    assert rag_debug["planner_decision"]["route"] == "deterministic_analytics"
    assert "metric_critical_ambiguous" in rag_debug["planner_decision"]["reason_codes"]


def test_narrative_retrieval_runtime_error_surfaces_explicit_debug(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [
            SimpleNamespace(
                id=file_id,
                embedding_model="local:nomic-embed-text:latest",
                file_type="txt",
                chunks_count=10,
                is_processed="completed",
                original_filename="notes.txt",
                custom_metadata={},
            )
        ]

    async def fail_query_rag(**kwargs):  # noqa: ANN003
        raise RuntimeError("vector store unavailable")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="Что написано в notes.txt?",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert context_docs == []
    assert rag_caveats == []
    assert rag_sources == []
    assert "ошибка внутреннего контура retrieval" in final_prompt.lower()
    assert rag_debug["retrieval_mode"] == "narrative_error"
    assert rag_debug["executor_error_code"] == "retrieval_runtime_error"
    assert rag_debug["requires_clarification"] is True


def test_narrative_all_group_failures_surface_explicit_debug(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_a = uuid.uuid4()
    file_b = uuid.uuid4()

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [
            SimpleNamespace(
                id=file_a,
                embedding_model="local:nomic-embed-text:latest",
                file_type="txt",
                chunks_count=5,
                is_processed="completed",
                original_filename="a.txt",
                custom_metadata={},
            ),
            SimpleNamespace(
                id=file_b,
                embedding_model="aihub:arctic",
                file_type="txt",
                chunks_count=5,
                is_processed="completed",
                original_filename="b.txt",
                custom_metadata={},
            ),
        ]

    async def fail_query_rag(**kwargs):  # noqa: ANN003
        raise RuntimeError("retrieval backend outage")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="Что есть в файлах?",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert context_docs == []
    assert rag_caveats == []
    assert rag_sources == []
    assert "retrieval failed" in final_prompt.lower()
    assert rag_debug["retrieval_mode"] == "narrative_error"
    assert rag_debug["executor_error_code"] == "retrieval_runtime_error"
    assert rag_debug["requires_clarification"] is True


def test_query_language_policy_applied_without_user():
    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=None,
            conversation_id=uuid.uuid4(),
            query="Сделай краткий отчет",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert "Respond strictly in Russian" in final_prompt
    assert rag_debug is None
    assert context_docs == []
    assert rag_caveats == []
    assert rag_sources == []


def test_build_rag_prompt_skips_files_without_active_ready_processing(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [
            SimpleNamespace(
                id=file_id,
                extension="xlsx",
                chunks_count=10,
                active_processing=None,
                original_filename="table.xlsx",
                custom_metadata={},
            )
        ]

    async def fail_query_rag(**kwargs):  # noqa: ANN003
        raise AssertionError("query_rag should not be called for files without active processing")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    final_prompt, rag_used, rag_debug, context_docs, rag_caveats, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="Сколько записей в таблице?",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert isinstance(final_prompt, str) and final_prompt
    assert rag_used is False
    assert isinstance(rag_debug, dict)
    assert rag_debug["retrieval_mode"] == "no_context_files"
    assert rag_debug["requires_clarification"] is True
    assert rag_debug["fallback_type"] == "no_context"
    assert context_docs == []
    assert rag_caveats == []
    assert rag_sources == []


def test_full_file_small_context_uses_direct_strategy(monkeypatch):
    docs = []
    for idx in range(16):
        docs.append(
            {
                "content": f"Row {idx + 1}: val={idx}",
                "metadata": {
                    "file_id": "f1",
                    "chunk_index": idx,
                    "filename": "sheet.xlsx",
                    "sheet_name": "Sheet1",
                    "row_start": idx * 20 + 1,
                    "row_end": (idx + 1) * 20,
                    "total_rows": 320,
                },
            }
        )

    monkeypatch.setattr(full_file_analysis.settings, "FULL_FILE_DIRECT_CONTEXT_MAX_CHUNKS", 24)
    monkeypatch.setattr(full_file_analysis.settings, "FULL_FILE_DIRECT_CONTEXT_MAX_CHARS", 50000)

    prompt, meta = asyncio.run(
        build_full_file_map_reduce_prompt(
            query="Сделай полный анализ по всем строкам",
            context_documents=docs,
            preferred_lang="ru",
            model_source="local",
            model_name="llama",
            prompt_max_chars=None,
        )
    )

    assert meta["enabled"] is True
    assert meta["strategy"] == "direct_context"
    assert meta["covered_chunks"] == 16
    assert meta["batch_diagnostics"]
    assert meta["batch_diagnostics"][0]["batch_rows_start_end"]
    assert meta["batch_diagnostics"][0]["batch_input_chars"] > 0
    assert "Full retrieved context" in prompt
    assert "chunk=15" in prompt


def test_full_file_map_reduce_structured_preserves_ranges(monkeypatch):
    docs = []
    for idx, (row_start, row_end) in enumerate([(1, 50), (51, 100), (101, 150), (151, 200)]):
        docs.append(
            {
                "content": f"Row range {row_start}-{row_end}",
                "metadata": {
                    "file_id": "f1",
                    "chunk_index": idx,
                    "filename": "sheet.xlsx",
                    "sheet_name": "Sheet1",
                    "row_start": row_start,
                    "row_end": row_end,
                    "total_rows": 200,
                },
            }
        )

    async def fake_generate_response(**kwargs):
        prompt = kwargs.get("prompt") or ""
        ranges = re.findall(r"rows=(\d+)-(\d+)", prompt)
        row_ranges = [
            {
                "file_key": "f1",
                "sheet_name": "Sheet1",
                "row_start": int(start),
                "row_end": int(end),
            }
            for start, end in ranges
        ]
        payload = {
            "facts": ["batch fact"],
            "aggregates": ["sum=100"],
            "row_ranges_covered": row_ranges,
            "missing_data": [],
        }
        return {"response": json.dumps(payload)}

    monkeypatch.setattr(full_file_analysis.settings, "FULL_FILE_DIRECT_CONTEXT_MAX_CHUNKS", 1)
    monkeypatch.setattr(full_file_analysis.settings, "FULL_FILE_DIRECT_CONTEXT_MAX_CHARS", 1000)
    monkeypatch.setattr(full_file_analysis.settings, "FULL_FILE_MAP_BATCH_MAX_DOCS", 2)
    monkeypatch.setattr(full_file_analysis.settings, "FULL_FILE_MAP_BATCH_MAX_CHARS", 10000)
    monkeypatch.setattr(full_file_analysis.settings, "FULL_FILE_REDUCE_CONTEXT_MAX_CHARS", 12000)
    monkeypatch.setattr(full_file_analysis.settings, "FULL_FILE_MAP_MAX_TOKENS", 900)
    monkeypatch.setattr(full_file_analysis.llm_manager, "generate_response", fake_generate_response)

    prompt, meta = asyncio.run(
        build_full_file_map_reduce_prompt(
            query="Сделай анализ по всем строкам",
            context_documents=docs,
            preferred_lang="ru",
            model_source="local",
            model_name="llama",
            prompt_max_chars=None,
        )
    )

    assert meta["strategy"] == "structured_map_reduce"
    assert meta["rows_used_map_total"] == 200
    assert meta["rows_used_reduce_total"] == 200
    assert meta["structured_reduce"]["row_ranges_covered"]
    assert "Structured reduce JSON" in prompt


def test_rag_prompt_emits_coverage_slo_metrics(monkeypatch):
    reset_metrics()
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_files(db, conversation_id, user_id):  # noqa: ARG001
        return [
            SimpleNamespace(
                id=file_id,
                embedding_model="local:nomic-embed-text:latest",
                file_type="xlsx",
                chunks_count=10,
                is_processed="completed",
                original_filename="sheet.xlsx",
                custom_metadata={},
            )
        ]

    async def fake_query_rag(**kwargs):  # noqa: ANN003
        _ = kwargs
        return {
            "docs": [
                {
                    "content": "row 1-20",
                    "metadata": {
                        "file_id": str(file_id),
                        "chunk_index": 0,
                        "filename": "sheet.xlsx",
                        "sheet_name": "Sheet1",
                        "row_start": 1,
                        "row_end": 20,
                        "total_rows": 100,
                    },
                    "similarity_score": 0.9,
                },
                {
                    "content": "row 21-40",
                    "metadata": {
                        "file_id": str(file_id),
                        "chunk_index": 1,
                        "filename": "sheet.xlsx",
                        "sheet_name": "Sheet1",
                        "row_start": 21,
                        "row_end": 40,
                        "total_rows": 100,
                    },
                    "similarity_score": 0.8,
                },
            ],
            "debug": {"intent": "analyze_full_file", "retrieval_mode": "full_file"},
        }

    async def fake_map_reduce(**kwargs):  # noqa: ANN003
        _ = kwargs
        return "full-file prompt", {"enabled": True, "truncated_batches": False}

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    final_prompt, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="Сделай полный анализ",
            top_k=8,
            model_source="local",
            rag_mode="full_file",
            full_file_prompt_builder=fake_map_reduce,
        )
    )

    assert rag_used is True
    assert final_prompt == "full-file prompt"
    assert rag_debug["coverage"]["ratio"] == 0.2
    assert rag_debug["row_coverage_ratio"] == 0.4

    snap = snapshot_metrics()
    counters = snap["counters"]
    gauges = snap["gauges"]
    assert any(
        "llama_service_retrieval_coverage_events_total" in key
        and "retrieval_mode=full_file" in key
        for key in counters
    )
    assert any(
        "llama_service_tabular_row_coverage_events_total" in key
        and "retrieval_mode=full_file" in key
        for key in counters
    )
    assert any(
        "llama_service_retrieval_coverage_ratio" in key
        and "retrieval_mode=full_file" in key
        for key in gauges
    )


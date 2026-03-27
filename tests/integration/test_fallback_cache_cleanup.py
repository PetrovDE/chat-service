import asyncio
import uuid
from types import SimpleNamespace

from app.domain.chat.query_planner import (
    INTENT_TABULAR_AGGREGATE,
    ROUTE_DETERMINISTIC_ANALYTICS,
    QueryPlanDecision,
)
from app.services.chat import rag_prompt_builder as rag_builder
from app.services.chat.context import (
    build_rag_conversation_memory,
    should_include_assistant_history_for_generation,
)
from app.services.chat.controlled_debug import CACHE_KEY_VERSION, build_cache_observability
from app.services.chat.orchestrator_helpers import (
    build_rag_debug_payload as build_orchestrator_rag_debug_payload,
)
from app.services.chat_orchestrator import ChatOrchestrator
from app.schemas import ChatMessage


def _force_deterministic_planner(*, query, files):  # noqa: ARG001
    return QueryPlanDecision(
        route=ROUTE_DETERMINISTIC_ANALYTICS,
        intent=INTENT_TABULAR_AGGREGATE,
        strategy_mode="analytical",
        confidence=0.95,
        requires_clarification=False,
        reason_codes=["test_force_deterministic"],
    )


def test_overview_then_narrow_followup_does_not_reuse_assistant_overview_in_structured_route():
    history = [
        {"role": "user", "content": "Сделай обзор таблицы"},
        {"role": "assistant", "content": "Вот подробный overview на 20 пунктов"},
        {"role": "user", "content": "Сколько записей по региону North?"},
    ]
    include_assistant = should_include_assistant_history_for_generation(
        {
            "retrieval_mode": "tabular_sql",
            "selected_route": "aggregation",
            "fallback_type": "none",
            "requires_clarification": False,
        }
    )
    memory = build_rag_conversation_memory(history, max_messages=6, include_assistant=include_assistant)
    assert include_assistant is False
    assert memory
    assert all(str(item.get("role")) != "assistant" for item in memory)
    assert any("Сколько записей" in str(item.get("content")) for item in memory)


def test_cross_chat_same_filename_does_not_reuse_old_not_found_fallback(monkeypatch):
    user_id = uuid.uuid4()
    first_chat = uuid.uuid4()
    second_chat = uuid.uuid4()
    file_id = uuid.uuid4()
    ready_file = SimpleNamespace(
        id=file_id,
        embedding_model="local:nomic-embed-text:latest",
        chunks_count=3,
        original_filename="report.xlsx",
        stored_filename=f"{file_id}_report.xlsx",
        custom_metadata={"display_name": "report.xlsx"},
    )
    state = {"calls": 0}

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    async def fake_get_user_ready_files_for_resolution(db, user_id, limit=300):  # noqa: ARG001
        state["calls"] += 1
        if state["calls"] == 1:
            return []
        return [ready_file]

    async def fake_add_file_to_conversation(db, file_id, conversation_id, attached_by_user_id=None):  # noqa: ARG001
        return SimpleNamespace(file_id=file_id, chat_id=conversation_id)

    async def fake_query_rag(**kwargs):  # noqa: ANN003
        selected_ids = kwargs.get("file_ids") or []
        return {
            "docs": [
                {
                    "content": "rows=460",
                    "metadata": {"file_id": selected_ids[0], "filename": "report.xlsx", "chunk_index": 0},
                    "similarity_score": 0.98,
                }
            ],
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"},
        }

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(
        rag_builder.crud_file,
        "get_user_ready_files_for_resolution",
        fake_get_user_ready_files_for_resolution,
    )
    monkeypatch.setattr(rag_builder.crud_file, "add_file_to_conversation", fake_add_file_to_conversation)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    prompt_first, rag_used_first, rag_debug_first, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=first_chat,
            query="Покажи report.xlsx",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )
    prompt_second, rag_used_second, rag_debug_second, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=second_chat,
            query="Покажи report.xlsx",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used_first is False
    assert "Не нашёл файл" in prompt_first
    assert rag_debug_first["fallback_type"] == "unresolved_file_not_found"
    assert rag_used_second is True
    assert "rows=460" in prompt_second
    assert rag_debug_second["file_resolution_status"] == "resolved_unique"
    assert rag_debug_second["fallback_type"] == "none"
    assert rag_debug_first["cache_key"] != rag_debug_second["cache_key"]


def test_found_file_with_empty_retrieval_returns_file_aware_controlled_fallback(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    attached_file = SimpleNamespace(
        id=file_id,
        embedding_model="local:nomic-embed-text:latest",
        file_type="txt",
        chunks_count=4,
        original_filename="notes.txt",
        custom_metadata={},
    )

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [attached_file]

    async def fake_query_rag(**kwargs):  # noqa: ANN003
        return {"docs": [], "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"}}

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    prompt, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="Что в файле notes.txt про KPI?",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert "доступных файлах" in prompt
    assert "Не нашёл файл" not in prompt
    assert rag_debug["file_resolution_status"] == "conversation_match"
    assert str(file_id) in rag_debug["resolved_file_ids"]
    assert rag_debug["fallback_type"] == "retrieval_empty"


def test_narrative_retrieval_path_emits_stage4_observability_fields(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    processing_id = uuid.uuid4()
    attached_file = SimpleNamespace(
        id=file_id,
        embedding_model="local:nomic-embed-text:latest",
        file_type="txt",
        chunks_count=4,
        original_filename="notes.txt",
        stored_filename=f"{file_id}_notes.txt",
        active_processing=SimpleNamespace(id=processing_id, status="ready"),
        custom_metadata={"upload_id": "upload-1", "document_id": "document-1"},
    )

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [attached_file]

    async def fake_query_rag(**kwargs):  # noqa: ANN003
        assert kwargs.get("processing_ids") == [str(processing_id)]
        return {
            "docs": [
                {
                    "content": "Customer churn increased in Q3 by 3%.",
                    "metadata": {
                        "file_id": str(file_id),
                        "filename": "notes.txt",
                        "chunk_index": 0,
                        "sheet_name": "Overview",
                    },
                    "similarity_score": 0.94,
                },
                {
                    "content": "Retention plan includes monthly cohort analysis.",
                    "metadata": {
                        "file_id": str(file_id),
                        "filename": "notes.txt",
                        "chunk_index": 1,
                        "sheet_name": "Overview",
                    },
                    "similarity_score": 0.89,
                },
            ],
            "debug": {
                "intent": "fact_lookup",
                "retrieval_mode": "hybrid",
                "where": {
                    "file_id": {"$in": [str(file_id)]},
                    "processing_id": {"$in": [str(processing_id)]},
                    "sheet_name": {"$in": ["Overview"]},
                },
                "top_k": int(kwargs.get("top_k", 0) or 0),
                "fetch_k": 24,
                "returned_count": 2,
            },
        }

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    prompt, rag_used, rag_debug, context_docs, _, rag_sources = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="What does notes.txt say about churn?",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is True
    assert "churn" in prompt.lower()
    assert len(context_docs) == 2
    assert rag_debug["retrieval_mode"] == "hybrid"

    payload = build_orchestrator_rag_debug_payload(
        rag_debug=rag_debug,
        context_docs=context_docs,
        rag_sources=rag_sources,
        llm_tokens_used=77,
        provider_debug=None,
    )
    assert payload is not None

    assert payload["file_resolution_status"] == "conversation_match"
    assert payload["resolved_file_ids"] == [str(file_id)]
    assert payload["upload_id"] == "upload-1"
    assert payload["document_id"] == "document-1"
    assert payload["retrieval_filters"]["file_id"] == {"$in": [str(file_id)]}
    assert payload["applied_filters"] == payload["retrieval_filters"]
    assert payload["retrieval_scope"]["file_ids"] == [str(file_id)]
    assert payload["retrieval_scope"]["processing_ids"] == [str(processing_id)]
    assert payload["retrieval_scope"]["sheet_names"] == ["Overview"]
    assert payload["top_chunks_total"] == 2
    assert len(payload["top_chunks"]) == 2
    assert payload["top_chunks"][0]["file_id"] == str(file_id)
    assert payload["top_chunks"][0]["score"] > 0.0
    assert payload["avg_similarity"] > 0.0
    assert payload["source_count"] == len(rag_sources)
    assert payload["context_chars"] > 0
    assert payload["context_tokens"] > 0
    assert payload["retrieval_skipped"] is False
    assert payload["retrieval_skip_reason"] == "none"
    assert payload["debug_sections"]["retrieval"]["retrieval_scope"]["file_ids"] == [str(file_id)]
    assert payload["debug_sections"]["retrieval"]["retrieval_skipped"] is False


def test_russian_controlled_fallbacks_stay_russian(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    tabular_file = SimpleNamespace(
        id=file_id,
        embedding_model="local:nomic-embed-text:latest",
        file_type="xlsx",
        chunks_count=20,
        original_filename="employees.xlsx",
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
                        "columns": ["name", "city"],
                        "column_aliases": {},
                    }
                ],
            }
        },
    )

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [tabular_file]

    async def fake_missing_column_tabular_sql(*, query, files):  # noqa: ARG001
        return {
            "status": "error",
            "clarification_prompt": "В таблице нет колонки для запроса (`дата рождения`).",
            "debug": {
                "retrieval_mode": "tabular_sql",
                "intent": "tabular_missing_column",
                "selected_route": "unsupported_missing_column",
                "detected_intent": "chart",
                "fallback_type": "unsupported_missing_column",
                "fallback_reason": "missing_required_columns",
            },
            "sources": [],
            "rows_expected_total": 50,
            "rows_retrieved_total": 0,
            "rows_used_map_total": 0,
            "rows_used_reduce_total": 0,
            "row_coverage_ratio": 0.0,
        }

    async def fail_query_rag(**kwargs):  # noqa: ANN003
        raise AssertionError("query_rag should not be called for missing-column deterministic fallback")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)
    monkeypatch.setattr(rag_builder, "execute_tabular_sql_path", fake_missing_column_tabular_sql)

    prompt, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="Построй chart по month of birth",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=_force_deterministic_planner,
        )
    )

    assert rag_used is False
    assert "В таблице нет колонки" in prompt
    assert rag_debug["detected_language"] == "ru"
    assert rag_debug["response_language"] == "ru"
    assert rag_debug["selected_route"] == "unsupported_missing_column"


def test_no_context_files_controlled_fallback_preserves_russian_language(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)

    prompt, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="Покажи данные по таблице",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert "нет готовых файлов" in prompt.lower()
    assert isinstance(rag_debug, dict)
    assert rag_debug["file_resolution_status"] == "no_context_files"
    assert rag_debug["fallback_type"] == "no_context"
    assert rag_debug["detected_language"] == "ru"
    assert rag_debug["response_language"] == "ru"
    assert rag_debug["selected_route"] == "no_context"


def test_cache_key_includes_route_language_and_file_resolution_dimensions():
    base = build_cache_observability(
        user_id="u1",
        conversation_id="c1",
        query="How many rows?",
        resolved_file_ids=["f1"],
        file_resolution_status="conversation_match",
        detected_language="en",
        selected_route="aggregation",
        detected_intent="aggregation",
    )
    changed_route = build_cache_observability(
        user_id="u1",
        conversation_id="c1",
        query="How many rows?",
        resolved_file_ids=["f1"],
        file_resolution_status="conversation_match",
        detected_language="en",
        selected_route="filtering",
        detected_intent="aggregation",
    )
    changed_language = build_cache_observability(
        user_id="u1",
        conversation_id="c1",
        query="How many rows?",
        resolved_file_ids=["f1"],
        file_resolution_status="conversation_match",
        detected_language="ru",
        selected_route="aggregation",
        detected_intent="aggregation",
    )
    changed_files = build_cache_observability(
        user_id="u1",
        conversation_id="c1",
        query="How many rows?",
        resolved_file_ids=["f2"],
        file_resolution_status="conversation_match",
        detected_language="en",
        selected_route="aggregation",
        detected_intent="aggregation",
    )

    assert base["cache_key_version"] == CACHE_KEY_VERSION
    assert base["cache_miss"] is False
    assert base["cache_hit"] is False
    assert base["cache_supported"] is False
    assert base["cache_active"] is False
    assert base["cache_status"] == "inactive"
    assert base["cache_key"] != changed_route["cache_key"]
    assert base["cache_key"] != changed_language["cache_key"]
    assert base["cache_key"] != changed_files["cache_key"]


def test_unsupported_missing_column_is_preserved_and_not_replaced_by_generic_no_context(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    tabular_file = SimpleNamespace(
        id=file_id,
        embedding_model="local:nomic-embed-text:latest",
        file_type="xlsx",
        chunks_count=20,
        original_filename="employees.xlsx",
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
                        "columns": ["name", "city"],
                        "column_aliases": {},
                    }
                ],
            }
        },
    )

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [tabular_file]

    async def fake_missing_column_tabular_sql(*, query, files):  # noqa: ARG001
        return {
            "status": "error",
            "clarification_prompt": "В таблице нет колонки для запроса (`дата рождения`).",
            "debug": {
                "retrieval_mode": "tabular_sql",
                "intent": "tabular_missing_column",
                "selected_route": "unsupported_missing_column",
                "detected_intent": "chart",
                "fallback_type": "unsupported_missing_column",
                "fallback_reason": "missing_required_columns",
            },
            "sources": [],
            "rows_expected_total": 50,
            "rows_retrieved_total": 0,
            "rows_used_map_total": 0,
            "rows_used_reduce_total": 0,
            "row_coverage_ratio": 0.0,
        }

    async def fail_query_rag(**kwargs):  # noqa: ANN003
        raise AssertionError("query_rag should not be used for unsupported_missing_column path")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)
    monkeypatch.setattr(rag_builder, "execute_tabular_sql_path", fake_missing_column_tabular_sql)

    prompt, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="Построй chart по month of birth",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=_force_deterministic_planner,
        )
    )

    assert rag_used is False
    assert "В таблице нет колонки" in prompt
    assert "нет готовых файлов" not in prompt.lower()
    assert rag_debug["selected_route"] == "unsupported_missing_column"
    assert rag_debug["fallback_type"] == "unsupported_missing_column"


def test_nonstream_orchestrator_runtime_error_returns_controlled_fallback(monkeypatch):
    orchestrator = ChatOrchestrator()
    conversation_id = uuid.uuid4()

    async def fake_prepare_context(*, chat_data, db, user_id):  # noqa: ARG001
        return {
            "conversation_id": conversation_id,
            "provider_source_selected_raw": "local",
            "provider_source_effective": "ollama",
            "provider_model_effective": "llama3.2",
            "provider_mode": "explicit",
            "final_prompt": "ignored",
            "rag_used": True,
            "rag_debug": {
                "execution_route": "narrative",
                "retrieval_mode": "tabular_sql",
                "selected_route": "aggregation",
                "detected_intent": "tabular_aggregate",
                "file_ids": ["f1"],
            },
            "context_docs": [],
            "rag_caveats": [],
            "rag_sources": ["table.xlsx | table=sheet_1 | sql"],
            "history_for_generation": [],
            "preferred_lang": "ru",
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
        raise RuntimeError("provider outage")

    monkeypatch.setattr(orchestrator, "_prepare_request_context", fake_prepare_context)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.crud_message.create_message", fake_create_message)
    monkeypatch.setattr("app.services.chat.orchestrator_runtime.llm_manager.generate_response", fail_generate_response)

    result = asyncio.run(
        orchestrator.chat(
            chat_data=ChatMessage(
                message="Сколько строк?",
                model_source="local",
                provider_mode="explicit",
                rag_debug=True,
            ),
            db=object(),
            current_user=None,
        )
    )

    assert "ошибка внутреннего runtime" in result.response.lower()
    assert isinstance(result.rag_debug, dict)
    assert result.rag_debug["fallback_type"] == "orchestrator_runtime_error"
    assert result.rag_debug["fallback_reason"] == "runtime_exception"

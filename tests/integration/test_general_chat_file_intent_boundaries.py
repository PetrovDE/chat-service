import asyncio
import uuid
from types import SimpleNamespace

import pytest

import app.domain.chat.query_planner as planner_module
from app.domain.chat.query_planner import (
    INTENT_NARRATIVE_RETRIEVAL,
    ROUTE_NARRATIVE_RETRIEVAL,
    QueryPlanDecision,
)
from app.services.chat import rag_prompt_builder as rag_builder
from app.services.chat.tabular_query_parser import parse_tabular_query


def _ready_active_processing() -> SimpleNamespace:
    return SimpleNamespace(id=uuid.uuid4(), status="ready")


def _attached_ready_file(*, file_id: uuid.UUID, extension: str = "xlsx") -> SimpleNamespace:
    return SimpleNamespace(
        id=file_id,
        file_type=extension,
        extension=extension,
        chunks_count=5,
        original_filename=f"dataset.{extension}",
        custom_metadata={},
        active_processing=_ready_active_processing(),
    )


def _narrative_planner_decision() -> QueryPlanDecision:
    return QueryPlanDecision(
        route=ROUTE_NARRATIVE_RETRIEVAL,
        intent=INTENT_NARRATIVE_RETRIEVAL,
        strategy_mode="semantic",
        confidence=0.8,
        requires_clarification=False,
        reason_codes=["test_narrative_route"],
    )


def test_no_files_russian_greeting_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u043f\u0440\u0438\u0432\u0435\u0442",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"


def test_no_files_python_chart_code_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)

    final_prompt, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u043d\u0430\u043f\u0438\u0448\u0438 \u043a\u043e\u0434 \u0434\u043b\u044f \u0432\u044b\u0432\u043e\u0434\u0430 \u0433\u0440\u0430\u0444\u0438\u043a\u043e\u0432 \u043d\u0430 python",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert isinstance(final_prompt, str) and final_prompt
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"


def test_no_files_what_is_pandas_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u0447\u0442\u043e \u0442\u0430\u043a\u043e\u0435 pandas",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["fallback_type"] == "none"


def test_attached_file_python_chart_code_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id, extension="xlsx")]

    def fail_query_planner(**kwargs):  # noqa: ANN003
        raise AssertionError("query_planner should not run for general_chat route")

    async def fail_query_rag(**kwargs):  # noqa: ANN003
        raise AssertionError("query_rag should not run for general_chat route")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u043d\u0430\u043f\u0438\u0448\u0438 \u043a\u043e\u0434 \u0434\u043b\u044f \u0432\u044b\u0432\u043e\u0434\u0430 \u0433\u0440\u0430\u0444\u0438\u043a\u043e\u0432 \u043d\u0430 python",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fail_query_planner,
        )
    )

    assert rag_used is False
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"


def test_attached_file_what_is_pandas_routes_to_general_chat(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id, extension="xlsx")]

    def fail_query_planner(**kwargs):  # noqa: ANN003
        raise AssertionError("query_planner should not run for general_chat route")

    async def fail_query_rag(**kwargs):  # noqa: ANN003
        raise AssertionError("query_rag should not run for general_chat route")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u0447\u0442\u043e \u0442\u0430\u043a\u043e\u0435 pandas",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fail_query_planner,
        )
    )

    assert rag_used is False
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"


def test_attached_file_schema_question_routes_file_aware(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    planner_called = {"value": False}

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id, extension="xlsx")]

    async def fake_query_rag(**kwargs):  # noqa: ANN003
        ids = kwargs.get("file_ids") or []
        return {
            "docs": [
                {
                    "content": "columns: amount_rub, city, month",
                    "metadata": {"file_id": ids[0], "filename": "dataset.xlsx", "chunk_index": 0},
                    "similarity_score": 0.98,
                }
            ],
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"},
        }

    def fake_query_planner(**kwargs):  # noqa: ANN003
        planner_called["value"] = True
        return _narrative_planner_decision()

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u043a\u0430\u043a\u0438\u0435 \u0441\u0442\u043e\u043b\u0431\u0446\u044b \u0432 \u0444\u0430\u0439\u043b\u0435",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fake_query_planner,
        )
    )

    assert planner_called["value"] is True
    assert rag_used is True
    assert rag_debug["selected_route"] != "general_chat"
    assert rag_debug["retrieval_mode"] != "assistant_direct"
    assert rag_debug["fallback_type"] != "no_context"


def test_attached_file_temporal_chart_routes_file_aware(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    planner_called = {"value": False}

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id, extension="xlsx")]

    async def fake_query_rag(**kwargs):  # noqa: ANN003
        ids = kwargs.get("file_ids") or []
        return {
            "docs": [
                {
                    "content": "monthly aggregation context",
                    "metadata": {"file_id": ids[0], "filename": "dataset.xlsx", "chunk_index": 0},
                    "similarity_score": 0.97,
                }
            ],
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"},
        }

    def fake_query_planner(**kwargs):  # noqa: ANN003
        planner_called["value"] = True
        return _narrative_planner_decision()

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="\u043f\u043e\u0441\u0442\u0440\u043e\u0439 \u0433\u0440\u0430\u0444\u0438\u043a \u043f\u043e \u043c\u0435\u0441\u044f\u0446\u0430\u043c",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fake_query_planner,
        )
    )

    assert planner_called["value"] is True
    assert rag_used is True
    assert rag_debug["selected_route"] != "general_chat"
    assert rag_debug["retrieval_mode"] != "assistant_direct"
    assert rag_debug["fallback_type"] != "no_context"


def test_schema_followup_detail_question_reuses_file_context(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    planner_called = {"value": False}

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id, extension="xlsx")]

    async def fake_query_rag(**kwargs):  # noqa: ANN003
        ids = kwargs.get("file_ids") or []
        return {
            "docs": [
                {
                    "content": "Column details: city (text), status (category), amount_rub (numeric).",
                    "metadata": {"file_id": ids[0], "filename": "dataset.xlsx", "chunk_index": 0},
                    "similarity_score": 0.99,
                }
            ],
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"},
        }

    def fake_query_planner(**kwargs):  # noqa: ANN003
        planner_called["value"] = True
        return _narrative_planner_decision()

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="show full description for each column",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fake_query_planner,
            conversation_history=[
                {"role": "user", "content": "what columns are in the file"},
                {"role": "assistant", "content": "I can list the schema and examples."},
            ],
        )
    )

    assert planner_called["value"] is True
    assert rag_used is True
    assert rag_debug["followup_context_used"] is True
    assert rag_debug["prior_tabular_intent_reused"] is True
    assert rag_debug["selected_route"] != "general_chat"
    assert rag_debug["retrieval_mode"] != "assistant_direct"


def test_attached_file_arbitrary_column_question_routes_file_aware(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    planner_called = {"value": False}

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return [_attached_ready_file(file_id=file_id, extension="xlsx")]

    async def fake_query_rag(**kwargs):  # noqa: ANN003
        ids = kwargs.get("file_ids") or []
        return {
            "docs": [
                {
                    "content": "calc_need_spravka indicates whether supporting documents are required.",
                    "metadata": {"file_id": ids[0], "filename": "dataset.xlsx", "chunk_index": 1},
                    "similarity_score": 0.98,
                }
            ],
            "debug": {"intent": "fact_lookup", "retrieval_mode": "hybrid"},
        }

    def fake_query_planner(**kwargs):  # noqa: ANN003
        planner_called["value"] = True
        return _narrative_planner_decision()

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fake_query_rag)

    _, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="explain column calc_need_spravka",
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=fake_query_planner,
        )
    )

    assert planner_called["value"] is True
    assert rag_used is True
    assert rag_debug["selected_route"] != "general_chat"
    assert rag_debug["retrieval_mode"] != "assistant_direct"
    assert rag_debug["fallback_type"] != "no_context"


def test_no_file_refusal_for_generic_python_chart_how_to(monkeypatch):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        return []

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)

    final_prompt, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query="how to build a bar chart in python",
            top_k=8,
            model_source="local",
            rag_mode="auto",
        )
    )

    assert rag_used is False
    assert rag_debug["detected_intent"] == "general_chat"
    assert rag_debug["selected_route"] == "general_chat"
    assert rag_debug["retrieval_mode"] == "assistant_direct"
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"
    assert "There are no ready files in this chat" not in final_prompt
    assert "Attach a file to this chat" not in final_prompt


@pytest.mark.parametrize(
    ("query", "case_id"),
    [
        ("what fields and data in test_requests_460_rows.xlsx file?", "en_explicit_filename_schema"),
        ("get test_requests_460_rows.xlsx and show me the data there", "en_explicit_filename_data_there"),
        ("can you read the file .xlsx and get me information about data there", "en_single_file_data_there"),
        ("what columns are in the file?", "en_columns_in_file"),
        (
            "\u043a\u0430\u043a\u0438\u0435 \u0434\u0430\u043d\u043d\u044b\u0435 \u0432 \u0444\u0430\u0439\u043b\u0435 "
            "test_requests_460_rows.xlsx \u0438 \u043a\u0430\u043a\u0438\u0435 \u0441\u0442\u043e\u043b\u0431\u0446\u044b",
            "ru_schema_parity",
        ),
    ],
    ids=[
        "en_explicit_filename_schema",
        "en_explicit_filename_data_there",
        "en_single_file_data_there",
        "en_columns_in_file",
        "ru_schema_parity",
    ],
)
def test_attached_ready_tabular_schema_queries_do_not_fall_to_narrative_empty_retrieval(
    monkeypatch,
    query: str,
    case_id: str,  # noqa: ARG001
):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    planner_calls = {"count": 0}
    executed_queries = []

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        file_obj = _attached_ready_file(file_id=file_id, extension="xlsx")
        file_obj.original_filename = "test_requests_460_rows.xlsx"
        file_obj.stored_filename = "test_requests_460_rows.xlsx"
        return [file_obj]

    dataset = SimpleNamespace(
        tables=[
            SimpleNamespace(
                table_name="sheet_1",
                sheet_name="Sheet1",
                columns=["office", "request_date", "status"],
                column_aliases={},
            )
        ]
    )

    def fake_resolve_tabular_dataset(file_obj):  # noqa: ARG001
        return dataset

    def tracking_query_planner(*, query, files):  # noqa: ANN003
        planner_calls["count"] += 1
        return planner_module.plan_query(query=query, files=files)

    async def fake_tabular_sql_executor(*, query, files):  # noqa: ANN003
        executed_queries.append(query)
        _ = files
        return {
            "status": "ok",
            "prompt_context": "Deterministic schema/file summary context (source of truth).",
            "debug": {
                "retrieval_mode": "tabular_sql",
                "intent": "tabular_profile",
                "detected_intent": "schema_question",
                "selected_route": "schema_question",
                "tabular_sql": {
                    "schema_payload": {
                        "file_name": "test_requests_460_rows.xlsx",
                        "tables_total": 1,
                        "rows_total": 460,
                        "columns": ["office", "request_date", "status"],
                        "selected_scope": {"scope_label": "sheet=Sheet1"},
                    }
                },
            },
            "sources": ["test_requests_460_rows.xlsx | schema"],
            "rows_expected_total": 460,
            "rows_retrieved_total": 460,
            "rows_used_map_total": 460,
            "rows_used_reduce_total": 460,
            "row_coverage_ratio": 1.0,
        }

    async def fail_query_rag(**kwargs):  # noqa: ANN003
        raise AssertionError(f"query_rag must not run for schema/profile path: {kwargs.get('query')}")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(planner_module, "resolve_tabular_dataset", fake_resolve_tabular_dataset)
    monkeypatch.setattr(rag_builder, "execute_tabular_sql_path", fake_tabular_sql_executor)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    final_prompt, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query=query,
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=tracking_query_planner,
        )
    )

    assert planner_calls["count"] == 1
    assert executed_queries
    assert rag_used is True
    assert isinstance(final_prompt, str) and final_prompt
    assert rag_debug["retrieval_mode"] == "tabular_sql"
    assert rag_debug["selected_route"] == "schema_question"
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"
    assert rag_debug["retrieval_mode"] != "narrative_no_retrieval"
    assert rag_debug.get("controlled_response_state") != "no_retrieval"


@pytest.mark.parametrize(
    ("query", "expected_route", "history"),
    [
        (
            "\u043f\u043e\u043a\u0430\u0436\u0438 \u043f\u043e\u043b\u043d\u043e\u0435 \u043e\u043f\u0438\u0441\u0430\u043d\u0438\u0435 \u043f\u043e \u043a\u0430\u0436\u0434\u043e\u043c\u0443 \u0441\u0442\u043e\u043b\u0431\u0446\u0443",
            "overview",
            [
                {"role": "user", "content": "\u043a\u0430\u043a\u0438\u0435 \u0441\u0442\u043e\u043b\u0431\u0446\u044b \u0432 \u0444\u0430\u0439\u043b\u0435?"},
                {"role": "assistant", "content": "office, status, calc_need_spravka"},
            ],
        ),
        (
            "\u043f\u043e \u043a\u0430\u043a\u043e\u043c\u0443 office \u0431\u043e\u043b\u044c\u0448\u0435 \u0432\u0441\u0435\u0433\u043e \u0437\u0430\u043f\u0438\u0441\u0435\u0439?",
            "aggregation",
            [
                {"role": "user", "content": "\u043a\u0430\u043a\u0438\u0435 \u0441\u0442\u043e\u043b\u0431\u0446\u044b \u0432 \u0444\u0430\u0439\u043b\u0435?"},
                {"role": "assistant", "content": "office, status, calc_need_spravka"},
            ],
        ),
        (
            "\u0434\u0430\u0439 \u0433\u0440\u0430\u0444\u0438\u043a \u0440\u0430\u0441\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0438\u044f \u0437\u0430\u043f\u0438\u0441\u0435\u0439 \u043f\u043e office",
            "chart",
            [
                {"role": "user", "content": "\u043a\u0430\u043a\u0438\u0435 \u0441\u0442\u043e\u043b\u0431\u0446\u044b \u0432 \u0444\u0430\u0439\u043b\u0435?"},
                {"role": "assistant", "content": "office, status, calc_need_spravka"},
            ],
        ),
        (
            "\u043f\u043e \u043a\u0430\u043a\u043e\u043c\u0443 office \u0431\u043e\u043b\u044c\u0448\u0435 \u0432\u0441\u0435\u0433\u043e \u0437\u0430\u043f\u0438\u0441\u0435\u0439? "
            "\u0434\u0430\u0439 \u0433\u0440\u0430\u0444\u0438\u043a \u0440\u0430\u0441\u043f\u0440\u0435\u0434\u0435\u043b\u0435\u043d\u0438\u044f \u0437\u0430\u043f\u0438\u0441\u0435\u0439 \u043f\u043e office",
            "chart",
            [
                {"role": "user", "content": "\u043a\u0430\u043a\u0438\u0435 \u0441\u0442\u043e\u043b\u0431\u0446\u044b \u0432 \u0444\u0430\u0439\u043b\u0435?"},
                {"role": "assistant", "content": "office, status, calc_need_spravka"},
            ],
        ),
    ],
    ids=[
        "schema_followup_full_description",
        "schema_to_aggregation_followup",
        "chart_distribution_followup",
        "combined_aggregation_and_chart_followup",
    ],
)
def test_attached_ready_tabular_followups_do_not_downgrade_to_narrative(
    monkeypatch,
    query: str,
    expected_route: str,
    history: list[dict[str, str]],
):
    user_id = uuid.uuid4()
    conversation_id = uuid.uuid4()
    file_id = uuid.uuid4()
    planner_calls = {"count": 0}
    executed_queries: list[str] = []

    async def fake_get_conversation_files(db, conversation_id, user_id):  # noqa: ARG001
        file_obj = _attached_ready_file(file_id=file_id, extension="xlsx")
        file_obj.original_filename = "test_requests_460_rows.xlsx"
        file_obj.stored_filename = "test_requests_460_rows.xlsx"
        return [file_obj]

    dataset = SimpleNamespace(
        tables=[
            SimpleNamespace(
                table_name="sheet_1",
                sheet_name="Sheet1",
                row_count=460,
                columns=["office", "status", "calc_need_spravka"],
                column_aliases={},
            )
        ]
    )

    def fake_resolve_tabular_dataset(file_obj):  # noqa: ARG001
        return dataset

    def tracking_query_planner(*, query, files):  # noqa: ANN003
        planner_calls["count"] += 1
        return planner_module.plan_query(query=query, files=files)

    async def fake_tabular_sql_executor(*, query, files):  # noqa: ANN003
        _ = files
        executed_queries.append(query)
        parsed = parse_tabular_query(query)
        assert parsed.route == expected_route
        tabular_debug: dict[str, object] = {
            "result": '[["office_a", 120], ["office_b", 100]]',
            "operation": "count",
            "group_by_column": "office",
            "metric_column": None,
        }
        debug_payload: dict[str, object] = {
            "retrieval_mode": "tabular_sql",
            "intent": "tabular_profile" if expected_route == "overview" else "tabular_aggregate",
            "detected_intent": expected_route,
            "selected_route": expected_route,
            "tabular_sql": tabular_debug,
        }
        if expected_route == "chart":
            debug_payload.update(
                {
                    "chart_rendered": True,
                    "chart_artifact_available": True,
                    "chart_artifact_exists": True,
                    "chart_fallback_reason": "none",
                    "requested_chart_field": "office",
                    "matched_chart_field": "office",
                }
            )
        return {
            "status": "ok",
            "prompt_context": "Deterministic tabular payload context.",
            "debug": debug_payload,
            "sources": ["test_requests_460_rows.xlsx | deterministic"],
            "rows_expected_total": 460,
            "rows_retrieved_total": 460,
            "rows_used_map_total": 460,
            "rows_used_reduce_total": 460,
            "row_coverage_ratio": 1.0,
            "artifacts": [],
        }

    async def fail_query_rag(**kwargs):  # noqa: ANN003
        raise AssertionError(f"query_rag must not run for deterministic follow-up route: {kwargs.get('query')}")

    monkeypatch.setattr(rag_builder.crud_file, "get_conversation_files", fake_get_conversation_files)
    monkeypatch.setattr(planner_module, "resolve_tabular_dataset", fake_resolve_tabular_dataset)
    monkeypatch.setattr(rag_builder, "execute_tabular_sql_path", fake_tabular_sql_executor)
    monkeypatch.setattr(rag_builder.rag_retriever, "query_rag", fail_query_rag)

    final_prompt, rag_used, rag_debug, _, _, _ = asyncio.run(
        rag_builder.build_rag_prompt(
            db=None,
            user_id=user_id,
            conversation_id=conversation_id,
            query=query,
            top_k=8,
            model_source="local",
            rag_mode="auto",
            query_planner=tracking_query_planner,
            conversation_history=history,
        )
    )

    assert planner_calls["count"] == 1
    assert executed_queries
    assert rag_used is True
    assert isinstance(final_prompt, str) and final_prompt
    assert rag_debug["retrieval_mode"] == "tabular_sql"
    assert rag_debug["selected_route"] == expected_route
    assert rag_debug["fallback_type"] == "none"
    assert rag_debug["fallback_reason"] == "none"
    assert rag_debug["retrieval_mode"] != "narrative_no_retrieval"
    assert rag_debug.get("controlled_response_state") != "no_retrieval"

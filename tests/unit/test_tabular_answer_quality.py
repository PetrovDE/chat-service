from __future__ import annotations

import uuid
from types import SimpleNamespace

from app.domain.chat.query_planner import QueryPlanDecision
from app.services.chat.tabular_answer_shaper import extract_chart_highlights
from app.services.chat.tabular_deterministic_result import build_tabular_success_route_result
from app.services.chat.tabular_response_composer import (
    build_chart_response_text,
    build_missing_column_message,
    build_scope_clarification_message,
)
from app.services.chat.controlled_response_composer import build_no_retrieval_message


def _planner_decision() -> QueryPlanDecision:
    return QueryPlanDecision(
        route="deterministic_analytics",
        intent="tabular_aggregate",
        strategy_mode="analytical",
        confidence=0.95,
        requires_clarification=False,
        reason_codes=["tabular_dataset_available"],
    )


def test_chart_answer_includes_source_field_and_useful_highlights() -> None:
    message = build_chart_response_text(
        preferred_lang="en",
        column_label="Status Code",
        chart_rendered=True,
        chart_artifact_available=True,
        chart_fallback_reason="none",
        result_text='[["200", 10], ["500", 2], ["404", 1]]',
        source_scope="orders.xlsx | sheet=Orders | table=orders",
    )

    assert "Chart ready: distribution of 'Status Code'." in message
    assert "Used data: orders.xlsx | sheet=Orders | table=orders." in message
    assert "Top bucket: `200` (10, 76.9% of total)." in message
    assert "Shape: concentrated." in message
    assert "Key contrast: `200` exceeds `500` by 8 (61.5% points)." in message


def test_chart_answer_does_not_claim_success_when_artifact_is_missing() -> None:
    message = build_chart_response_text(
        preferred_lang="en",
        column_label="Status Code",
        chart_rendered=True,
        chart_artifact_available=False,
        chart_fallback_reason="artifact_not_accessible",
        result_text='[["200", 10], ["500", 2]]',
        source_scope="orders.xlsx | sheet=Orders | table=orders",
    )

    assert "could not be delivered" in message.lower()
    assert "chart ready" not in message.lower()
    assert "reason=artifact_not_accessible" in message
    assert "Top bucket: `200` (10, 83.3% of total)." in message


def test_chart_highlights_dominant_category_and_concentration_quality() -> None:
    highlights = extract_chart_highlights(result_text='[["A", 90], ["B", 5], ["C", 5]]', max_items=3)

    assert len(highlights) <= 3
    assert "Top bucket: `A` (90, 90.0% of total)." in highlights[0]
    assert any("Shape: concentrated." in item for item in highlights)
    assert any("Key contrast: `A` exceeds `B` by 85 (85.0% points)." in item for item in highlights)


def test_chart_highlights_balanced_distribution_quality() -> None:
    highlights = extract_chart_highlights(result_text='[["A", 26], ["B", 25], ["C", 24], ["D", 25]]', max_items=3)

    assert len(highlights) <= 3
    assert "Top bucket: `A` (26, 26.0%); `B` is close at 25.0%." in highlights[0]
    assert any("Shape: flat across leading buckets" in item for item in highlights)
    assert not any("Shape: concentrated." in item for item in highlights)


def test_chart_highlights_long_tail_top_n_concentration_quality() -> None:
    highlights = extract_chart_highlights(
        result_text='[["A", 60], ["B", 20], ["C", 8], ["D", 4], ["E", 3], ["F", 2], ["G", 1], ["H", 1], ["I", 1]]',
        max_items=3,
    )

    assert len(highlights) <= 3
    assert any("Shape: long-tail." in item for item in highlights)
    assert any("Top 3 buckets account for 88.0%" in item for item in highlights)


def test_chart_highlights_time_series_trend_wording_quality() -> None:
    highlights = extract_chart_highlights(
        result_text='[["2024-01", 80], ["2024-02", 100], ["2024-03", 120], ["2024-04", 140]]',
        max_items=3,
    )

    assert len(highlights) <= 3
    assert any("Trend: increasing over time" in item for item in highlights)
    assert any("2024-01" in item and "2024-04" in item for item in highlights)


def test_chart_highlights_do_not_overclaim_weak_time_signal() -> None:
    highlights = extract_chart_highlights(
        result_text='[["2024-01", 100], ["2024-02", 101], ["2024-03", 99], ["2024-04", 100]]',
        max_items=3,
    )

    assert len(highlights) <= 3
    assert any("Trend: no clear directional trend across time buckets." in item for item in highlights)
    assert not any("Trend: increasing over time" in item for item in highlights)
    assert not any("Trend: decreasing over time" in item for item in highlights)


def test_chart_highlights_remain_concise() -> None:
    highlights = extract_chart_highlights(
        result_text='[["A", 50], ["B", 30], ["C", 20], ["D", 10], ["E", 5], ["F", 3]]',
        max_items=3,
    )

    assert 1 <= len(highlights) <= 3


def test_missing_column_answer_prioritizes_alternatives_and_next_question() -> None:
    message = build_missing_column_message(
        preferred_lang="en",
        requested_fields=["revenue"],
        alternatives=["amount_total", "amount_rub", "status"],
        ambiguous=False,
    )

    assert "available options" in message.lower()
    assert message.find("amount_total") < message.find("amount_rub")
    assert "best next question" in message.lower()


def test_scope_ambiguity_answer_is_concise_and_actionable() -> None:
    message = build_scope_clarification_message(
        preferred_lang="en",
        scope_kind="file",
        scope_options=["north.xlsx", "south.xlsx"],
    )

    assert "multiple possible file matches" in message.lower()
    assert "north.xlsx" in message
    assert "south.xlsx" in message
    assert "best next question" in message.lower()


def test_no_retrieval_answer_explains_limit_and_next_step() -> None:
    message = build_no_retrieval_message(preferred_lang="en")

    assert "No relevant chunks were found" in message
    assert "cannot answer confidently" in message
    assert "Best next question" in message


def test_aggregation_prompt_guidance_requests_direct_answer_then_interpretation() -> None:
    planner = _planner_decision()
    planner_payload = planner.as_dict()
    tabular_sql_result = {
        "status": "ok",
        "prompt_context": (
            "Deterministic tabular SQL result (source of truth):\n"
            "table=orders\n"
            "sql=SELECT COUNT(*) FROM orders\n"
            "result=[[308]]"
        ),
        "debug": {
            "selected_route": "aggregation",
            "tabular_sql": {
                "result": "[[308]]",
            },
        },
        "sources": ["orders.xlsx | table=orders | dataset_v=1 | table_v=1 | sql"],
        "rows_expected_total": 308,
        "rows_retrieved_total": 308,
        "rows_used_map_total": 308,
        "rows_used_reduce_total": 308,
        "row_coverage_ratio": 1.0,
    }

    prompt, _rag_used, _rag_debug, _docs, _caveats, _sources = build_tabular_success_route_result(
        query="How many rows are in orders?",
        user_id=uuid.uuid4(),
        conversation_id=uuid.uuid4(),
        files=[SimpleNamespace(id="f-1")],
        planner_decision=planner,
        planner_decision_payload=planner_payload,
        expected_chunks_total=0,
        rag_mode="auto",
        top_k=8,
        preferred_lang="en",
        is_combined_intent=False,
        tabular_sql_result=tabular_sql_result,
        processing_ids_by_file={},
        combined_context_docs=[],
        combined_debug={},
    )

    assert "Start with the direct answer in the first sentence" in prompt
    assert "For aggregation/table results, provide the direct value first" in prompt
    assert "Do not include headings named Answer/Limitations/Sources" in prompt
    assert "Deterministic direct answer candidate: 308." in prompt


def test_schema_summary_prompt_guidance_stays_concise_and_relevant() -> None:
    planner = _planner_decision()
    planner_payload = planner.as_dict()
    tabular_sql_result = {
        "status": "ok",
        "prompt_context": "Deterministic table schema (source of truth): ...",
        "debug": {
            "selected_route": "schema_question",
            "tabular_sql": {
                "schema_payload": {
                    "table_name": "orders",
                    "row_count": 308,
                    "columns": [
                        "order_id",
                        "created_at",
                        "status",
                        "city",
                        "amount_total",
                        "discount_rate",
                        "currency",
                    ],
                    "file_name": "orders.xlsx",
                    "summary_statement": "Single table file with rows=308 and columns=7.",
                    "tables_total": 1,
                    "rows_total": 308,
                    "selected_scope": {
                        "table_name": "orders",
                        "sheet_name": "Orders",
                        "scope_label": "sheet Orders (table orders)",
                    },
                    "tables": [
                        {
                            "table_name": "orders",
                            "sheet_name": "Orders",
                            "scope_label": "sheet Orders (table orders)",
                            "row_count": 308,
                            "columns_total": 7,
                            "relevant_fields": [
                                {"name": "created_at", "reasons": ["date/time values"]},
                                {"name": "amount_total", "reasons": ["numeric values"]},
                                {"name": "status", "reasons": ["low cardinality"]},
                                {"name": "city", "reasons": ["sample values available"]},
                            ],
                        }
                    ],
                    "next_question_suggestions": [
                        "In sheet Orders (table orders), show amount_total by created_at.",
                        "List all columns if you want a complete schema dump.",
                    ],
                },
            },
        },
        "sources": ["orders.xlsx | table=orders | dataset_v=1 | table_v=1 | schema"],
        "rows_expected_total": 308,
        "rows_retrieved_total": 308,
        "rows_used_map_total": 308,
        "rows_used_reduce_total": 308,
        "row_coverage_ratio": 1.0,
    }

    prompt, _rag_used, _rag_debug, _docs, _caveats, _sources = build_tabular_success_route_result(
        query="What data is available in this file?",
        user_id=uuid.uuid4(),
        conversation_id=uuid.uuid4(),
        files=[SimpleNamespace(id="f-1")],
        planner_decision=planner,
        planner_decision_payload=planner_payload,
        expected_chunks_total=0,
        rag_mode="auto",
        top_k=8,
        preferred_lang="en",
        is_combined_intent=False,
        tabular_sql_result=tabular_sql_result,
        processing_ids_by_file={},
        combined_context_docs=[],
        combined_debug={},
    )

    assert "For schema/file summary requests" in prompt
    assert "Most relevant analysis fields (max 6)" in prompt
    assert "Suggested next questions:" in prompt
    assert "open with a one-sentence first impression" in prompt
    assert "do not guess one silently" in prompt
    assert "Keep schema summaries concise" in prompt


def test_schema_summary_guidance_limits_field_dump_and_keeps_priority() -> None:
    planner = _planner_decision()
    planner_payload = planner.as_dict()
    relevant_fields = [
        {"name": f"field_{index}", "reasons": ["schema column"]}
        for index in range(1, 11)
    ]
    tabular_sql_result = {
        "status": "ok",
        "prompt_context": "Deterministic schema/file summary context (source of truth): ...",
        "debug": {
            "selected_route": "schema_question",
            "tabular_sql": {
                "schema_payload": {
                    "file_name": "wide.xlsx",
                    "summary_statement": "Single table file with rows=120 and columns=20.",
                    "tables_total": 1,
                    "rows_total": 120,
                    "tables": [
                        {
                            "table_name": "wide",
                            "sheet_name": "Sheet1",
                            "scope_label": "sheet Sheet1 (table wide)",
                            "row_count": 120,
                            "columns_total": 20,
                            "relevant_fields": relevant_fields,
                        }
                    ],
                    "next_question_suggestions": ["Use sheet Sheet1 (table wide) and list its key columns."],
                }
            },
        },
        "sources": ["wide.xlsx | table=wide | dataset_v=1 | table_v=1 | schema"],
        "rows_expected_total": 120,
        "rows_retrieved_total": 120,
        "rows_used_map_total": 120,
        "rows_used_reduce_total": 120,
        "row_coverage_ratio": 1.0,
    }

    prompt, _rag_used, _rag_debug, _docs, _caveats, _sources = build_tabular_success_route_result(
        query="which fields are important?",
        user_id=uuid.uuid4(),
        conversation_id=uuid.uuid4(),
        files=[SimpleNamespace(id="f-wide")],
        planner_decision=planner,
        planner_decision_payload=planner_payload,
        expected_chunks_total=0,
        rag_mode="auto",
        top_k=8,
        preferred_lang="en",
        is_combined_intent=False,
        tabular_sql_result=tabular_sql_result,
        processing_ids_by_file={},
        combined_context_docs=[],
        combined_debug={},
    )

    assert "field_1 (schema column)" in prompt
    assert "field_6 (schema column)" in prompt
    assert "field_7 (schema column)" not in prompt


def test_schema_summary_guidance_for_multi_sheet_scope_includes_preview() -> None:
    planner = _planner_decision()
    planner_payload = planner.as_dict()
    tabular_sql_result = {
        "status": "ok",
        "prompt_context": "Deterministic schema/file summary context (source of truth): ...",
        "debug": {
            "selected_route": "schema_question",
            "tabular_sql": {
                "schema_payload": {
                    "file_name": "regions.xlsx",
                    "summary_statement": "Workbook with 2 table(s)/sheet(s), total rows=240.",
                    "tables_total": 2,
                    "rows_total": 240,
                    "tables": [
                        {
                            "table_name": "north_sheet",
                            "sheet_name": "North",
                            "scope_label": "sheet North (table north_sheet)",
                            "row_count": 120,
                            "columns_total": 4,
                            "relevant_fields": [{"name": "amount_rub", "reasons": ["numeric values"]}],
                        },
                        {
                            "table_name": "south_sheet",
                            "sheet_name": "South",
                            "scope_label": "sheet South (table south_sheet)",
                            "row_count": 120,
                            "columns_total": 4,
                            "relevant_fields": [{"name": "amount_rub", "reasons": ["numeric values"]}],
                        },
                    ],
                    "next_question_suggestions": [
                        "Use sheet North (table north_sheet) and list its key columns.",
                        "Compare row counts between sheet North (table north_sheet) and sheet South (table south_sheet).",
                    ],
                }
            },
        },
        "sources": ["regions.xlsx | dataset_v=1 | schema_multi_table"],
        "rows_expected_total": 240,
        "rows_retrieved_total": 240,
        "rows_used_map_total": 240,
        "rows_used_reduce_total": 240,
        "row_coverage_ratio": 1.0,
    }

    prompt, _rag_used, _rag_debug, _docs, _caveats, _sources = build_tabular_success_route_result(
        query="what tables are available?",
        user_id=uuid.uuid4(),
        conversation_id=uuid.uuid4(),
        files=[SimpleNamespace(id="f-regions")],
        planner_decision=planner,
        planner_decision_payload=planner_payload,
        expected_chunks_total=0,
        rag_mode="auto",
        top_k=8,
        preferred_lang="en",
        is_combined_intent=False,
        tabular_sql_result=tabular_sql_result,
        processing_ids_by_file={},
        combined_context_docs=[],
        combined_debug={},
    )

    assert "Tables/sheets total: 2" in prompt
    assert "sheet North (table north_sheet), rows=120" in prompt
    assert "sheet South (table south_sheet), rows=120" in prompt

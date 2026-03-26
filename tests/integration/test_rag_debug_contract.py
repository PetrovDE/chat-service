from app.services.chat.postprocess import append_caveats_and_sources
from app.services.chat.sources_debug import (
    build_coverage_sources,
    build_sources_list,
    build_standard_rag_debug_payload,
)
from app.services.llm.providers import aihub as aihub_module


def test_standard_rag_debug_payload_contains_filters_and_top_chunks():
    rag_debug = {
        "filters": {"file_id": {"$in": ["f1"]}},
        "returned_count": 1,
        "retrieval_mode": "hybrid",
    }
    context_docs = [
        {
            "content": "Quarterly revenue is 120M with 12% growth year over year.",
            "metadata": {
                "file_id": "f1",
                "chunk_index": 3,
                "filename": "report.xlsx",
            },
            "similarity_score": 0.91,
        }
    ]

    payload = build_standard_rag_debug_payload(
        rag_debug=rag_debug,
        context_docs=context_docs,
        rag_sources=["report.xlsx | chunk=3"],
        llm_tokens_used=222,
        max_items=8,
    )

    assert payload["filters"] == {"file_id": {"$in": ["f1"]}}
    assert payload["retrieval_hits"] == 1
    assert payload["top_chunks"]
    first = payload["top_chunks"][0]
    assert first["file_id"] == "f1"
    assert first["doc_id"] == "f1"
    assert first["chunk_id"] == "f1_3"
    assert isinstance(first["preview"], str) and first["preview"]
    assert payload["top_chunks_limit"] == 8
    assert payload["top_chunks_total"] == 1
    assert payload["retrieved_chunks_total"] == 1
    assert payload["retrieval_hits_count"] == 1
    assert payload["retrieval_filters"] == {"file_id": {"$in": ["f1"]}}
    assert payload["retrieval_path"] == "vector"
    assert payload["structured_path_used"] is False
    assert payload["debug_contract_version"] == "rag_debug_v1"
    assert isinstance(payload["top_similarity_scores"], list)


def test_sources_and_top_chunks_include_row_ranges():
    docs = [
        {
            "content": "Row details",
            "metadata": {
                "file_id": "f2",
                "chunk_index": 7,
                "filename": "table.xlsx",
                "sheet_name": "Sheet1",
                "row_start": 281,
                "row_end": 308,
                "total_rows": 308,
            },
            "similarity_score": 1.0,
        }
    ]

    sources = build_sources_list(docs, max_items=8)
    payload = build_standard_rag_debug_payload(
        rag_debug={"retrieval_mode": "full_file", "returned_count": 1},
        context_docs=docs,
        rag_sources=sources,
        llm_tokens_used=10,
        max_items=8,
    )

    assert sources[0] == "table.xlsx | sheet=Sheet1 | chunk=7 | rows=281-308"
    assert payload["top_chunks"][0]["row_start"] == 281
    assert payload["top_chunks"][0]["row_end"] == 308
    assert payload["top_chunks"][0]["total_rows"] == 308
    assert payload["top_chunks"][0]["chunk_type"] is None


def test_coverage_sources_merge_row_ranges():
    docs = [
        {
            "content": "part1",
            "metadata": {"filename": "sales.xlsx", "sheet_name": "Data", "row_start": 1, "row_end": 40, "chunk_index": 0},
        },
        {
            "content": "part2",
            "metadata": {"filename": "sales.xlsx", "sheet_name": "Data", "row_start": 41, "row_end": 80, "chunk_index": 1},
        },
        {
            "content": "part3",
            "metadata": {"filename": "sales.xlsx", "sheet_name": "Data", "row_start": 120, "row_end": 140, "chunk_index": 2},
        },
    ]

    lines = build_coverage_sources(docs, max_items=8)
    assert lines
    assert lines[0] == "sales.xlsx | sheet=Data | rows=1-80, 120-140"


def test_append_caveats_and_sources_localizes_english_titles():
    answer = "Revenue increased."
    merged = append_caveats_and_sources(
        answer,
        caveats=[],
        sources=["sales.xlsx | rows=1-100"],
        preferred_lang="en",
    )
    assert "### Limitations/Missing Data" in merged
    assert "### Sources (short)" in merged


def test_aihub_prompt_truncation_debug_visible(monkeypatch):
    monkeypatch.setattr(aihub_module.settings, "AIHUB_MAX_PROMPT_CHARS", 2100)
    prompt = "x" * 2600
    _messages, provider_debug = aihub_module.aihub_provider._prepare_messages(
        conversation_history=None,
        prompt=prompt,
        prompt_max_chars=None,
    )

    payload = build_standard_rag_debug_payload(
        rag_debug={"retrieval_mode": "full_file"},
        context_docs=[],
        rag_sources=[],
        llm_tokens_used=0,
        provider_debug=provider_debug,
        max_items=8,
    )

    assert payload["prompt_chars_before"] == 2600
    assert payload["prompt_chars_after"] == 2100
    assert payload["prompt_truncated"] is True
    assert payload["prompt_chars_requested"] == 2100
    assert payload["prompt_chars_configured"] == 2100
    assert payload["prompt_chars_limit"] == 2100


def test_aihub_prompt_debug_exposes_requested_and_effective_limits(monkeypatch):
    monkeypatch.setattr(aihub_module.settings, "AIHUB_MAX_PROMPT_CHARS", 50000)
    prompt = "x" * 64000
    _messages, provider_debug = aihub_module.aihub_provider._prepare_messages(
        conversation_history=None,
        prompt=prompt,
        prompt_max_chars=500000,
    )

    payload = build_standard_rag_debug_payload(
        rag_debug={"retrieval_mode": "full_file"},
        context_docs=[],
        rag_sources=[],
        llm_tokens_used=0,
        provider_debug=provider_debug,
        max_items=8,
    )

    assert payload["prompt_chars_requested"] == 500000
    assert payload["prompt_chars_configured"] == 50000
    assert payload["prompt_chars_limit"] == 50000
    assert payload["prompt_chars_before"] == 64000
    assert payload["prompt_chars_after"] == 50000
    assert payload["prompt_truncated"] is True


def test_structured_retrieval_debug_path_flag():
    payload = build_standard_rag_debug_payload(
        rag_debug={"retrieval_mode": "tabular_sql", "returned_count": 0},
        context_docs=[],
        rag_sources=[],
        llm_tokens_used=0,
        max_items=8,
    )
    assert payload["retrieval_path"] == "structured"
    assert payload["structured_path_used"] is True


def test_rag_debug_contract_stable_defaults_include_file_route_language_and_cache_fields():
    payload = build_standard_rag_debug_payload(
        rag_debug={"retrieval_mode": "hybrid"},
        context_docs=[],
        rag_sources=[],
        llm_tokens_used=None,
        max_items=8,
    )

    assert payload["selected_route"] == "unknown"
    assert payload["detected_intent"] == "unknown"
    assert payload["file_resolution_status"] == "not_requested"
    assert payload["resolved_file_ids"] == []
    assert payload["resolved_file_names"] == []
    assert payload["matched_columns"] == []
    assert payload["unmatched_requested_fields"] == []
    assert payload["fallback_type"] == "none"
    assert payload["fallback_reason"] == "none"
    assert payload["detected_language"] == "ru"
    assert payload["response_language"] == "ru"
    assert payload["cache_hit"] is False
    assert payload["cache_miss"] is True
    assert payload["cache_key_version"] == "unknown"
    assert payload["chart_artifact_available"] is False
    assert payload["chart_artifact_exists"] is False
    assert payload["requested_time_grain"] is None
    assert payload["source_datetime_field"] is None
    assert payload["derived_temporal_dimension"] is None
    assert payload["temporal_plan_status"] == "not_requested"
    assert payload["planner_mode"] == "deterministic"
    assert payload["analytic_plan_version"] == "none"
    assert payload["analytic_plan_json"] == {}
    assert payload["plan_validation_status"] == "not_attempted"
    assert payload["sql_generation_mode"] == "deterministic"
    assert payload["sql_validation_status"] == "not_attempted"
    assert payload["post_execution_validation_status"] == "not_attempted"
    assert payload["repair_iteration_index"] == 0
    assert payload["repair_iteration_count"] == 0
    assert payload["repair_failure_reason"] == "none"
    assert payload["clarification_triggered_after_retries"] is False
    assert payload["final_execution_mode"] == "unknown"
    assert payload["final_selected_route"] == "unknown"
    assert payload["followup_context_used"] is False
    assert payload["prior_tabular_intent_reused"] is False
    assert "debug_sections" in payload
    assert payload["debug_sections"]["files"]["file_resolution_status"] == "not_requested"
    assert payload["debug_sections"]["chart"]["chart_artifact_available"] is False
    assert payload["debug_sections"]["retrieval"]["retrieval_hits_count"] == 0
    assert payload["debug_sections"]["tabular"]["temporal_plan_status"] == "not_requested"
    assert payload["debug_sections"]["continuity"]["followup_context_used"] is False
    assert payload["debug_sections"]["planner"]["planner_mode"] == "deterministic"
    assert payload["debug_sections"]["planner"]["plan_validation_status"] == "not_attempted"


def test_rag_debug_contract_file_aware_tabular_fields_are_preserved():
    rag_debug = {
        "retrieval_mode": "tabular_sql",
        "execution_route": "tabular_sql",
        "selected_route": "unsupported_missing_column",
        "detected_intent": "chart",
        "fallback_type": "unsupported_missing_column",
        "fallback_reason": "missing_required_columns",
        "file_resolution_status": "resolved_unique",
        "resolved_file_ids": ["f-1"],
        "resolved_file_names": ["employees.xlsx"],
        "matched_columns": ["city"],
        "unmatched_requested_fields": ["birth_date"],
        "detected_language": "ru",
        "response_language": "ru",
        "cache_hit": False,
        "cache_miss": True,
        "cache_key_version": "v2-route-lang-fileaware",
        "cache_key": "abc123",
        "followup_context_used": True,
        "prior_tabular_intent_reused": True,
    }
    payload = build_standard_rag_debug_payload(
        rag_debug=rag_debug,
        context_docs=[],
        rag_sources=[],
        llm_tokens_used=None,
        max_items=8,
    )

    assert payload["selected_route"] == "unsupported_missing_column"
    assert payload["detected_intent"] == "chart"
    assert payload["file_resolution_status"] == "resolved_unique"
    assert payload["resolved_file_ids"] == ["f-1"]
    assert payload["resolved_file_names"] == ["employees.xlsx"]
    assert payload["matched_columns"] == ["city"]
    assert payload["unmatched_requested_fields"] == ["birth_date"]
    assert payload["fallback_type"] == "unsupported_missing_column"
    assert payload["fallback_reason"] == "missing_required_columns"
    assert payload["response_language"] == "ru"
    assert payload["cache_key"] == "abc123"
    assert payload["debug_sections"]["tabular"]["unmatched_requested_fields"] == ["birth_date"]
    assert payload["debug_sections"]["continuity"]["followup_context_used"] is True
    assert payload["debug_sections"]["continuity"]["prior_tabular_intent_reused"] is True


def test_rag_debug_contract_includes_temporal_fields():
    payload = build_standard_rag_debug_payload(
        rag_debug={
            "retrieval_mode": "tabular_sql",
            "selected_route": "trend",
            "requested_time_grain": "month",
            "source_datetime_field": "created_at",
            "derived_temporal_dimension": "month(created_at)",
            "temporal_plan_status": "resolved",
            "temporal_aggregation_plan": {
                "requested_time_grain": "month",
                "source_datetime_field": "created_at",
                "derived_grouping_dimension": "month(created_at)",
                "operation": "sum",
                "measure_column": "amount_rub",
                "status": "ready",
                "fallback_reason": "none",
            },
        },
        context_docs=[],
        rag_sources=[],
        llm_tokens_used=0,
        max_items=8,
    )

    assert payload["requested_time_grain"] == "month"
    assert payload["source_datetime_field"] == "created_at"
    assert payload["derived_temporal_dimension"] == "month(created_at)"
    assert payload["temporal_plan_status"] == "resolved"
    assert payload["debug_sections"]["tabular"]["requested_time_grain"] == "month"
    assert payload["debug_sections"]["tabular"]["source_datetime_field"] == "created_at"


def test_rag_debug_contract_includes_schema_match_and_chart_fields():
    rag_debug = {
        "retrieval_mode": "tabular_sql",
        "requested_field_text": "status code",
        "candidate_columns": ["status_code", "status_name"],
        "scored_candidates": [
            {"column": "status_code", "score": 0.91},
            {"column": "status_name", "score": 0.88},
        ],
        "matched_column": "status_code",
        "match_score": 0.91,
        "match_strategy": "alias_match",
        "chart_spec_generated": True,
        "chart_rendered": False,
        "chart_artifact_available": False,
        "chart_artifact_exists": False,
        "chart_artifact_path": None,
        "chart_artifact_id": None,
    }
    payload = build_standard_rag_debug_payload(
        rag_debug=rag_debug,
        context_docs=[],
        rag_sources=[],
        llm_tokens_used=0,
        max_items=8,
    )

    assert payload["requested_field_text"] == "status code"
    assert payload["candidate_columns"] == ["status_code", "status_name"]
    assert payload["matched_column"] == "status_code"
    assert payload["match_strategy"] == "alias_match"
    assert payload["chart_spec_generated"] is True
    assert payload["chart_rendered"] is False
    assert payload["chart_artifact_available"] is False
    assert payload["chart_artifact_exists"] is False
    assert payload["debug_sections"]["chart"]["chart_artifact_available"] is False
    assert payload["debug_sections"]["chart"]["chart_artifact_exists"] is False


def test_rag_debug_contract_includes_scope_section_without_breaking_existing_sections():
    rag_debug = {
        "retrieval_mode": "tabular_sql",
        "scope_selection_status": "selected",
        "scope_selected_file_id": "f-1",
        "scope_selected_file_name": "north.xlsx",
        "scope_selected_table_name": "north_sheet",
        "scope_selected_sheet_name": "North",
        "scope_file_candidates": [{"file_id": "f-1", "score": 1.2}],
        "table_scope_candidates": [{"table_name": "north_sheet", "score": 1.1}],
    }
    payload = build_standard_rag_debug_payload(
        rag_debug=rag_debug,
        context_docs=[],
        rag_sources=[],
        llm_tokens_used=0,
        max_items=8,
    )

    assert payload["debug_sections"]["routing"]["retrieval_mode"] == "tabular_sql"
    assert payload["debug_sections"]["files"]["file_resolution_status"] == "not_requested"
    assert payload["debug_sections"]["scope"]["scope_selection_status"] == "selected"
    assert payload["debug_sections"]["scope"]["selected_file_name"] == "north.xlsx"
    assert payload["debug_sections"]["scope"]["selected_sheet_name"] == "North"
    assert payload["debug_sections"]["scope"]["selected_table_name"] == "north_sheet"


def test_rag_debug_contract_extracts_embedding_and_collection_namespace_details():
    docs = [
        {
            "content": "row payload",
            "metadata": {
                "file_id": "f1",
                "filename": "sales.xlsx",
                "chunk_index": 0,
                "collection": "documents_1536d",
                "namespace": "documents",
                "embedding_dimension": 1536,
            },
            "similarity_score": 0.97,
        }
    ]
    payload = build_standard_rag_debug_payload(
        rag_debug={"retrieval_mode": "hybrid", "embedding_mode": "aihub", "embedding_model": "qwen3-emb"},
        context_docs=docs,
        rag_sources=["sales.xlsx | chunk=0"],
        llm_tokens_used=321,
        max_items=8,
    )

    assert payload["embedding_provider"] == "aihub"
    assert payload["embedding_model"] == "qwen3-emb"
    assert payload["embedding_dimension"] == 1536
    assert payload["embedding_details_available"] is True
    assert payload["retrieval_collections"] == ["documents_1536d"]
    assert payload["retrieval_namespaces"] == ["documents"]
    assert payload["collection"] == "documents_1536d"
    assert payload["namespace"] == "documents"

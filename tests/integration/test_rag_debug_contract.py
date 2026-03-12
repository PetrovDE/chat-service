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
    assert payload["retrieval_path"] == "vector"
    assert payload["structured_path_used"] is False
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

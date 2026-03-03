from app.services import chat_orchestrator as chat_service


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

    payload = chat_service._build_standard_rag_debug_payload(
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
    assert first["doc_id"] == "f1_3"
    assert first["chunk_id"] == "f1_3"
    assert isinstance(first["preview"], str) and first["preview"]
    assert payload["top_chunks_limit"] == 8
    assert payload["top_chunks_total"] == 1
    assert payload["retrieved_chunks_total"] == 1


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

    sources = chat_service._build_sources_list(docs, max_items=8)
    payload = chat_service._build_standard_rag_debug_payload(
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

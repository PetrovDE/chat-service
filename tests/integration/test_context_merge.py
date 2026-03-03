from app.services.chat.context import merge_context_docs


def test_merge_context_docs_does_not_collapse_spreadsheet_chunks_with_same_prefix():
    prefix = "======================================================================\nEXCEL | SHEET: Sheet1\n======================================================================\nColumns: A, B, C\n"
    docs = [
        {
            "content": prefix + "Rows: 1-20 / 320\nRow 1: A: foo | B: 1",
            "metadata": {
                "file_id": "f1",
                "chunk_id": "f1_0",
                "sheet_name": "Sheet1",
                "row_start": 1,
                "row_end": 20,
            },
            "similarity_score": 0.9,
        },
        {
            "content": prefix + "Rows: 21-40 / 320\nRow 21: A: bar | B: 2",
            "metadata": {
                "file_id": "f1",
                "chunk_id": "f1_1",
                "sheet_name": "Sheet1",
                "row_start": 21,
                "row_end": 40,
            },
            "similarity_score": 0.8,
        },
    ]

    merged = merge_context_docs(docs, max_docs=64, sort_by_score=False)
    assert len(merged) == 2


def test_merge_context_docs_dedups_exact_same_chunk():
    doc = {
        "content": "same chunk content",
        "metadata": {
            "file_id": "f1",
            "chunk_id": "f1_3",
            "chunk_index": 3,
            "sheet_name": "Sheet1",
            "row_start": 61,
            "row_end": 80,
        },
        "similarity_score": 0.9,
    }

    merged = merge_context_docs([doc, dict(doc)], max_docs=64, sort_by_score=False)
    assert len(merged) == 1

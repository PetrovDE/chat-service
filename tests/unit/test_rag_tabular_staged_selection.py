from app.rag.retriever import RAGRetriever


def _row(*, row_id: str, chunk_type: str, score: float, sheet: str = "S1", row_start: int = 1, row_end: int = 50):
    return {
        "id": row_id,
        "content": f"{chunk_type}-{row_id}",
        "metadata": {
            "file_id": "f1",
            "file_type": "xlsx",
            "chunk_type": chunk_type,
            "sheet_name": sheet,
            "row_start": row_start,
            "row_end": row_end,
        },
        "hybrid_score": score,
    }


def test_staged_tabular_selection_prioritizes_summaries():
    retriever = RAGRetriever()
    rows = [
        _row(row_id="rg1", chunk_type="row_group", score=0.9, row_start=1, row_end=50),
        _row(row_id="rg2", chunk_type="row_group", score=0.89, row_start=51, row_end=100),
        _row(row_id="rg3", chunk_type="row_group", score=0.88, row_start=101, row_end=150),
        _row(row_id="s1", chunk_type="sheet_summary", score=0.87, row_start=0, row_end=0),
        _row(row_id="f1", chunk_type="file_summary", score=0.86, row_start=0, row_end=0),
    ]

    selected = retriever._staged_tabular_selection(rows, top_k=3)
    chunk_types = [str((item.get("metadata") or {}).get("chunk_type")) for item in selected]
    assert "file_summary" in chunk_types
    assert "sheet_summary" in chunk_types
    assert len(selected) == 3


def test_staged_tabular_selection_limits_row_groups_per_sheet():
    retriever = RAGRetriever()
    rows = [
        _row(row_id="f1", chunk_type="file_summary", score=0.99, row_start=0, row_end=0),
        _row(row_id="s1", chunk_type="sheet_summary", score=0.98, row_start=0, row_end=0),
        _row(row_id="rg1", chunk_type="row_group", score=0.97, row_start=1, row_end=50),
        _row(row_id="rg2", chunk_type="row_group", score=0.96, row_start=51, row_end=100),
        _row(row_id="rg3", chunk_type="row_group", score=0.95, row_start=101, row_end=150),
        _row(row_id="rg4", chunk_type="row_group", score=0.94, row_start=151, row_end=200),
    ]

    selected = retriever._staged_tabular_selection(rows, top_k=5)
    row_groups = [item for item in selected if (item.get("metadata") or {}).get("chunk_type") == "row_group"]
    assert len(row_groups) <= 2

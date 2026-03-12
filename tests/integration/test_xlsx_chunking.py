import asyncio
from pathlib import Path

import pandas as pd

from app.rag.document_loader import DocumentLoader
from app.rag import document_loader as loader_module


def test_xlsx_wide_sheet_chunking_adaptive(monkeypatch, tmp_path: Path):
    rows = 308
    dense_cols = {f"dense_{i}": [f"v{i}_{r}" for r in range(rows)] for i in range(12)}
    sparse_cols = {
        f"sparse_{i}": [f"s{i}_{r}" if (r % 20 == 0) else "" for r in range(rows)]
        for i in range(28)
    }
    data = {**dense_cols, **sparse_cols}
    df = pd.DataFrame(data)

    xlsx_path = tmp_path / "wide.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")

    monkeypatch.setattr(loader_module.settings, "XLSX_CHUNK_MAX_CHARS", 1400)
    monkeypatch.setattr(loader_module.settings, "XLSX_CHUNK_MAX_ROWS", 80)
    monkeypatch.setattr(loader_module.settings, "XLSX_MAX_COLUMNS_PER_CHUNK", 8)

    loader = DocumentLoader()
    docs = asyncio.run(loader.load_excel(str(xlsx_path), metadata={"file_id": "f1"}))
    row_groups = [d for d in docs if d.metadata.get("chunk_type") == "row_group"]
    sheet_summaries = [d for d in docs if d.metadata.get("chunk_type") == "sheet_summary"]
    file_summaries = [d for d in docs if d.metadata.get("chunk_type") == "file_summary"]

    assert docs
    assert file_summaries
    assert sheet_summaries
    assert len(row_groups) >= 6
    assert row_groups[0].metadata["row_start"] == 1
    assert row_groups[-1].metadata["row_end"] == rows
    assert all(len(d.page_content) <= 1650 for d in row_groups)
    assert all("====" not in d.page_content for d in row_groups)

    for doc in row_groups:
        assert len(doc.metadata["columns"]) <= 8
        assert len(doc.metadata["columns_all"]) == 40
        assert doc.metadata["columns_pruned"] is True


def test_xlsx_long_cell_not_lossy_truncated(monkeypatch, tmp_path: Path):
    long_text = "A" * 5000
    df = pd.DataFrame({"name": ["item-1"], "payload": [long_text]})

    xlsx_path = tmp_path / "long_cell.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")

    monkeypatch.setattr(loader_module.settings, "XLSX_CHUNK_MAX_CHARS", 1200)
    monkeypatch.setattr(loader_module.settings, "XLSX_CHUNK_MAX_ROWS", 20)
    monkeypatch.setattr(loader_module.settings, "XLSX_MAX_COLUMNS_PER_CHUNK", 0)
    monkeypatch.setattr(loader_module.settings, "XLSX_CELL_MAX_CHARS", 0)

    loader = DocumentLoader()
    docs = asyncio.run(loader.load_excel(str(xlsx_path), metadata={"file_id": "f1"}))
    combined = "\n".join(d.page_content for d in docs)
    row_groups = [d for d in docs if d.metadata.get("chunk_type") == "row_group"]

    assert docs
    assert row_groups
    assert any("Row 1:" in d.page_content for d in row_groups)
    assert "..." in combined
    assert long_text[:800] in combined


def test_tabular_doc_cap_keeps_summaries_and_limits_row_groups(monkeypatch, tmp_path: Path):
    rows = 5000
    cols = 150
    data = {f"c_{i}": [f"v_{i}_{r}" for r in range(rows)] for i in range(cols)}
    df = pd.DataFrame(data)
    csv_path = tmp_path / "huge.csv"
    df.to_csv(csv_path, index=False)

    monkeypatch.setattr(loader_module.settings, "TABULAR_MAX_EMBEDDING_DOCS", 64)
    monkeypatch.setattr(loader_module.settings, "XLSX_MAX_COLUMNS_PER_CHUNK", 16)
    monkeypatch.setattr(loader_module.settings, "XLSX_CHUNK_MAX_ROWS", 200)

    loader = DocumentLoader()
    docs = asyncio.run(loader.load_csv(str(csv_path), metadata={"file_id": "f_huge"}))
    file_summaries = [d for d in docs if d.metadata.get("chunk_type") == "file_summary"]
    sheet_summaries = [d for d in docs if d.metadata.get("chunk_type") == "sheet_summary"]
    row_groups = [d for d in docs if d.metadata.get("chunk_type") == "row_group"]

    assert file_summaries
    assert sheet_summaries
    assert row_groups
    assert len(docs) <= 64

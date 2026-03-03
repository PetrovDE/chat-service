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

    assert docs
    assert len(docs) >= 8
    assert docs[0].metadata["row_start"] == 1
    assert docs[-1].metadata["row_end"] == rows
    assert all(len(d.page_content) <= 1650 for d in docs)
    assert all("====" not in d.page_content for d in docs)

    for doc in docs:
        assert len(doc.metadata["columns"]) <= 8
        assert len(doc.metadata["columns_all"]) == 40
        assert doc.metadata["columns_pruned"] is True

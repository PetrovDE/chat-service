from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.services.tabular.parsing import read_csv_with_detection, read_excel_sheets


def test_csv_parsing_detects_encoding_delimiter_and_header(tmp_path: Path):
    csv_path = tmp_path / "clients_cp1251.csv"
    rows = [
        "клиент;категория;сумма",
        "ООО Альфа;A;1200",
        "ООО Бета;B;800",
    ]
    csv_path.write_bytes("\n".join(rows).encode("cp1251"))

    df, meta = read_csv_with_detection(csv_path)

    assert meta.encoding in {"cp1251", "latin-1"}
    assert meta.delimiter == ";"
    assert meta.header_detected is True
    assert meta.row_count == 2
    assert meta.column_count == 3
    assert meta.columns[:2] == ["клиент", "категория"]
    assert "сумма" in meta.inferred_types
    assert df.iloc[0]["клиент"] == "ООО Альфа"


def test_csv_parsing_handles_no_header_files(tmp_path: Path):
    csv_path = tmp_path / "rows_no_header.csv"
    csv_path.write_text("10|north|active\n20|south|pending\n", encoding="utf-8")

    df, meta = read_csv_with_detection(csv_path)

    assert meta.delimiter == "|"
    assert meta.header_detected is False
    assert meta.columns == ["col_1", "col_2", "col_3"]
    assert meta.row_count == 2
    assert df.iloc[1]["col_2"] == "south"


def test_xlsx_multi_sheet_parsing_returns_all_non_empty_sheets(tmp_path: Path):
    xlsx_path = tmp_path / "multi_sheet.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        pd.DataFrame({"city": ["ekb", "msk"], "amount": [10, 20]}).to_excel(
            writer, index=False, sheet_name="Sales"
        )
        pd.DataFrame({"region": ["ural"], "manager": ["ivan"]}).to_excel(
            writer, index=False, sheet_name="Owners"
        )
        pd.DataFrame({"empty": ["", ""]}).to_excel(writer, index=False, sheet_name="EmptyRows")

    parsed = read_excel_sheets(xlsx_path)
    names = [name for name, _ in parsed]

    assert "Sales" in names
    assert "Owners" in names
    assert "EmptyRows" not in names

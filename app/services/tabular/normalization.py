from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import re
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class NormalizedTabularTable:
    table_name: str
    sheet_name: str
    row_count: int
    columns: List[str]
    column_aliases: Dict[str, str]
    dataframe: object


def safe_sql_identifier(name: str, fallback: str) -> str:
    raw = (name or "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9_]+", "_", raw).strip("_")
    if not cleaned:
        cleaned = fallback
    if cleaned[0].isdigit():
        cleaned = f"t_{cleaned}"
    return cleaned


def normalize_dataframe_columns(df) -> Tuple[List[str], Dict[str, str]]:
    seen: Dict[str, int] = {}
    final_columns: List[str] = []
    aliases: Dict[str, str] = {}

    for idx, raw_col in enumerate(df.columns):
        original = str(raw_col or "").strip() or f"col_{idx + 1}"
        base = safe_sql_identifier(original, fallback=f"col_{idx + 1}")
        suffix = seen.get(base, 0)
        seen[base] = suffix + 1
        normalized = base if suffix == 0 else f"{base}_{suffix + 1}"
        final_columns.append(normalized)
        aliases[normalized] = original

    df.columns = final_columns
    return final_columns, aliases


def _read_csv(file_path: Path):
    import pandas as pd

    attempts = [
        {"encoding": "utf-8"},
        {"encoding": "utf-8-sig"},
        {"encoding": "cp1251"},
        {"encoding": "latin-1"},
    ]
    last_error: Optional[Exception] = None
    for attempt in attempts:
        try:
            return pd.read_csv(
                str(file_path),
                dtype=str,
                keep_default_na=False,
                engine="python",
                sep=None,
                on_bad_lines="skip",
                **attempt,
            )
        except Exception as exc:
            last_error = exc
            continue
    raise ValueError(f"Failed to read CSV file: {file_path}. Last error: {last_error}")


def _read_excel_sheet(file_path: Path, sheet_name: str):
    import pandas as pd

    return pd.read_excel(
        str(file_path),
        sheet_name=sheet_name,
        dtype=str,
        keep_default_na=False,
    )


def load_normalized_tables(file_path: Path, file_type: str) -> List[NormalizedTabularTable]:
    file_type = (file_type or "").lower().strip()
    if file_type not in {"csv", "xlsx", "xls"}:
        return []

    tables: List[NormalizedTabularTable] = []

    if file_type == "csv":
        df = _read_csv(file_path)
        if df is None or df.empty:
            return []
        columns, aliases = normalize_dataframe_columns(df)
        table_name = safe_sql_identifier("csv_data", fallback="csv_data")
        tables.append(
            NormalizedTabularTable(
                table_name=table_name,
                sheet_name="CSV",
                row_count=int(len(df)),
                columns=columns,
                column_aliases=aliases,
                dataframe=df,
            )
        )
        return tables

    import pandas as pd

    excel = pd.ExcelFile(str(file_path))
    for idx, sheet in enumerate(excel.sheet_names, start=1):
        try:
            df = _read_excel_sheet(file_path, sheet_name=sheet)
        except Exception:
            logger.warning("Tabular runtime: failed to read sheet '%s'", sheet, exc_info=True)
            continue
        if df is None or df.empty:
            continue
        columns, aliases = normalize_dataframe_columns(df)
        table_name = safe_sql_identifier(f"sheet_{idx}_{sheet}", fallback=f"sheet_{idx}")
        tables.append(
            NormalizedTabularTable(
                table_name=table_name,
                sheet_name=str(sheet),
                row_count=int(len(df)),
                columns=columns,
                column_aliases=aliases,
                dataframe=df,
            )
        )

    return tables


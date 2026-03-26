from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import re
from typing import Any, Dict, List, Tuple

from app.services.tabular.column_metadata_contract import (
    TABULAR_COLUMN_METADATA_CONTRACT_VERSION,
    build_dataframe_column_metadata,
)
from app.services.tabular.parsing import read_csv_with_detection, read_excel_sheets

logger = logging.getLogger(__name__)


@dataclass
class NormalizedTabularTable:
    table_name: str
    sheet_name: str
    row_count: int
    columns: List[str]
    column_aliases: Dict[str, str]
    column_metadata: Dict[str, Dict[str, Any]]
    column_metadata_contract_version: str
    column_metadata_stats: Dict[str, Any]
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


def build_column_metadata(df, *, columns: List[str], aliases: Dict[str, str]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    return build_dataframe_column_metadata(
        df=df,
        columns=columns,
        aliases=aliases,
    )


def _read_csv(file_path: Path):
    df, _meta = read_csv_with_detection(file_path, forced_delimiter=None)
    return df


def _read_tsv(file_path: Path):
    df, _meta = read_csv_with_detection(file_path, forced_delimiter="\t")
    return df


def _read_excel_sheet(file_path: Path, sheet_name: str):
    tables = dict(read_excel_sheets(file_path))
    if sheet_name not in tables:
        raise ValueError(f"Excel sheet not found: {sheet_name}")
    return tables[sheet_name]


def _normalize_dataframe(df):
    import pandas as pd

    if df is None:
        return pd.DataFrame()
    work = df.copy()
    work.columns = [str(col or "").strip() or f"col_{idx + 1}" for idx, col in enumerate(work.columns)]
    work = work.fillna("")
    for col in work.columns:
        work[col] = work[col].astype(str).map(lambda value: value.strip())
    if not work.empty:
        work = work.loc[(work != "").any(axis=1)]
    if not work.empty:
        non_empty_cols = [col for col in work.columns if bool((work[col] != "").any())]
        work = work[non_empty_cols]
    return work.reset_index(drop=True)


def load_normalized_tables(file_path: Path, file_type: str) -> List[NormalizedTabularTable]:
    file_type = (file_type or "").lower().strip()
    if file_type not in {"csv", "tsv", "xlsx", "xls"}:
        return []

    tables: List[NormalizedTabularTable] = []

    if file_type in {"csv", "tsv"}:
        df = _read_csv(file_path) if file_type == "csv" else _read_tsv(file_path)
        df = _normalize_dataframe(df)
        if df is None or df.empty:
            return []
        columns, aliases = normalize_dataframe_columns(df)
        column_metadata, column_metadata_stats = build_column_metadata(df, columns=columns, aliases=aliases)
        table_fallback = "csv_data" if file_type == "csv" else "tsv_data"
        table_name = safe_sql_identifier(table_fallback, fallback=table_fallback)
        tables.append(
            NormalizedTabularTable(
                table_name=table_name,
                sheet_name="CSV" if file_type == "csv" else "TSV",
                row_count=int(len(df)),
                columns=columns,
                column_aliases=aliases,
                column_metadata=column_metadata,
                column_metadata_contract_version=TABULAR_COLUMN_METADATA_CONTRACT_VERSION,
                column_metadata_stats=column_metadata_stats,
                dataframe=df,
            )
        )
        return tables

    excel_tables = read_excel_sheets(file_path)
    for idx, (sheet, df) in enumerate(excel_tables, start=1):
        df = _normalize_dataframe(df)
        if df is None or df.empty:
            continue
        columns, aliases = normalize_dataframe_columns(df)
        column_metadata, column_metadata_stats = build_column_metadata(df, columns=columns, aliases=aliases)
        table_name = safe_sql_identifier(f"sheet_{idx}_{sheet}", fallback=f"sheet_{idx}")
        tables.append(
            NormalizedTabularTable(
                table_name=table_name,
                sheet_name=str(sheet),
                row_count=int(len(df)),
                columns=columns,
                column_aliases=aliases,
                column_metadata=column_metadata,
                column_metadata_contract_version=TABULAR_COLUMN_METADATA_CONTRACT_VERSION,
                column_metadata_stats=column_metadata_stats,
                dataframe=df,
            )
        )

    return tables

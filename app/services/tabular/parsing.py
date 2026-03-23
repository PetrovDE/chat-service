from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


_DEFAULT_CSV_ENCODINGS: Tuple[str, ...] = ("utf-8", "utf-8-sig", "cp1251", "latin-1")
_DEFAULT_DELIMITERS: Tuple[str, ...] = (",", ";", "\t", "|")


@dataclass(frozen=True)
class CSVReadMetadata:
    encoding: str
    delimiter: str
    header_detected: bool
    row_count: int
    column_count: int
    columns: List[str]
    inferred_types: Dict[str, str]
    preview_rows: List[Dict[str, str]]


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


def infer_series_kind(series) -> str:
    non_empty: List[str] = [str(v).strip() for v in series.tolist() if str(v).strip()]
    if not non_empty:
        return "empty"
    sample = non_empty[:200]
    bool_count = 0
    int_count = 0
    float_count = 0
    datetime_like_count = 0
    for value in sample:
        lowered = value.lower()
        if lowered in {"true", "false", "yes", "no", "y", "n", "0", "1"}:
            bool_count += 1
            continue
        try:
            int(value)
            int_count += 1
            continue
        except Exception:
            pass
        try:
            float(value.replace(",", "."))
            float_count += 1
            continue
        except Exception:
            pass
        if "-" in value or "/" in value or ":" in value:
            datetime_like_count += 1
    total = max(1, len(sample))
    if bool_count / total >= 0.8:
        return "boolean"
    if int_count / total >= 0.8:
        return "integer"
    if (int_count + float_count) / total >= 0.8:
        return "number"
    if datetime_like_count / total >= 0.6:
        return "datetime_like"
    return "text"


def infer_column_types(df) -> Dict[str, str]:
    return {str(col): infer_series_kind(df[col]) for col in list(df.columns)}


def dataframe_preview_rows(df, *, max_rows: int = 5) -> List[Dict[str, str]]:
    if df is None or df.empty:
        return []
    out: List[Dict[str, str]] = []
    for _, row in df.head(max_rows).iterrows():
        item: Dict[str, str] = {}
        for col in df.columns:
            item[str(col)] = str(row.get(col, "") or "")
        out.append(item)
    return out


def _decode_sample(
    *,
    raw: bytes,
    encodings: Iterable[str],
) -> Tuple[str, str]:
    for encoding in encodings:
        try:
            text = raw.decode(encoding)
            return encoding, text
        except Exception:
            continue
    return "latin-1", raw.decode("latin-1", errors="ignore")


def detect_csv_encoding_and_delimiter(
    file_path: Path,
    *,
    candidate_encodings: Iterable[str] = _DEFAULT_CSV_ENCODINGS,
    delimiters: Iterable[str] = _DEFAULT_DELIMITERS,
) -> Tuple[str, str, bool]:
    raw = Path(file_path).read_bytes()[:64 * 1024]
    encoding, sample_text = _decode_sample(raw=raw, encodings=candidate_encodings)
    delimiter = ","
    header_detected = True
    if sample_text.strip():
        try:
            sniffed = csv.Sniffer().sniff(sample_text, delimiters="".join(delimiters))
            delimiter = str(getattr(sniffed, "delimiter", ",") or ",")
        except Exception:
            # Fallback: pick delimiter with max occurrences on first non-empty line.
            lines = [line for line in sample_text.splitlines() if line.strip()]
            first_line = lines[0] if lines else ""
            if first_line:
                ranked = sorted(
                    ((first_line.count(dlm), dlm) for dlm in delimiters),
                    key=lambda pair: pair[0],
                    reverse=True,
                )
                if ranked and ranked[0][0] > 0:
                    delimiter = ranked[0][1]
        try:
            header_detected = bool(csv.Sniffer().has_header(sample_text))
        except Exception:
            header_detected = True
    return encoding, delimiter, header_detected


def read_csv_with_detection(
    file_path: Path,
    *,
    forced_delimiter: Optional[str] = None,
    candidate_encodings: Iterable[str] = _DEFAULT_CSV_ENCODINGS,
) -> Tuple[Any, CSVReadMetadata]:
    import pandas as pd

    encoding, detected_delimiter, header_detected = detect_csv_encoding_and_delimiter(
        Path(file_path),
        candidate_encodings=candidate_encodings,
    )
    delimiter = str(forced_delimiter or detected_delimiter or ",")
    header = 0 if header_detected else None
    try:
        df = pd.read_csv(
            str(file_path),
            dtype=str,
            keep_default_na=False,
            engine="python",
            sep=delimiter,
            on_bad_lines="skip",
            encoding=encoding,
            header=header,
        )
    except Exception:
        # Last fallback for badly encoded files.
        df = pd.read_csv(
            str(file_path),
            dtype=str,
            keep_default_na=False,
            engine="python",
            sep=delimiter,
            on_bad_lines="skip",
            encoding="latin-1",
            header=header,
        )
        encoding = "latin-1"

    if header is None:
        df.columns = [f"col_{idx + 1}" for idx in range(len(df.columns))]
    df = _normalize_dataframe(df)
    columns = [str(col) for col in list(df.columns)]
    metadata = CSVReadMetadata(
        encoding=encoding,
        delimiter=delimiter,
        header_detected=bool(header_detected),
        row_count=int(len(df)),
        column_count=int(len(columns)),
        columns=columns,
        inferred_types=infer_column_types(df) if columns else {},
        preview_rows=dataframe_preview_rows(df, max_rows=5),
    )
    return df, metadata


def read_excel_sheets(file_path: Path) -> List[Tuple[str, Any]]:
    import pandas as pd

    result: List[Tuple[str, Any]] = []
    excel = pd.ExcelFile(str(file_path))
    for sheet_name in excel.sheet_names:
        df = pd.read_excel(
            str(file_path),
            sheet_name=sheet_name,
            dtype=str,
            keep_default_na=False,
        )
        normalized = _normalize_dataframe(df)
        if normalized is None or normalized.empty:
            continue
        result.append((str(sheet_name), normalized))
    return result

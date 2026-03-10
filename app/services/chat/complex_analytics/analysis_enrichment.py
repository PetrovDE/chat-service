from __future__ import annotations

from typing import Any, Dict, List


def infer_column_purpose(column_name: str) -> str:
    c = str(column_name or "").strip().lower()
    if not c:
        return "attribute used for segmentation/analysis"
    if c.endswith("_id") or c == "id" or "uuid" in c:
        return "identifier/key column"
    if "date" in c or "time" in c or c.endswith("_at"):
        return "time/event timestamp"
    if "comment" in c or "text" in c or "descr" in c or "message" in c:
        return "free-text/narrative field"
    if "office" in c or "region" in c or "city" in c or "branch" in c:
        return "organizational or location dimension"
    if "status" in c or "stage" in c or "state" in c:
        return "process state dimension"
    if "amount" in c or "sum" in c or "price" in c or "cost" in c or "total" in c or "revenue" in c:
        return "financial metric field"
    if "count" in c or "qty" in c or "volume" in c:
        return "volume/count metric field"
    return "attribute used for segmentation/analysis"


def infer_process_context(columns: List[str]) -> str:
    cols = {str(c).strip().lower() for c in columns if str(c).strip()}
    if {"application_id", "comment_time", "comment_text", "office"}.issubset(cols):
        return "Application review / processing workflow with distributed offices and analyst comments."
    if ("order_id" in cols or "invoice_id" in cols) and ("status" in cols or "stage" in cols):
        return "Order-to-cash / document processing workflow."
    if ("ticket_id" in cols or "incident_id" in cols) and ("status" in cols or "comment_text" in cols):
        return "Support or incident management workflow."
    return "Likely an operational process dataset with records, dimensions, and process indicators."


def _build_column_profile(frame: Any, *, max_columns: int = 80) -> List[Dict[str, Any]]:
    profiles: List[Dict[str, Any]] = []
    columns = list(getattr(frame, "columns", []))[:max_columns]
    rows_total = int(len(frame))
    for column in columns:
        series = frame[column]
        clean = series.dropna()
        profile = {
            "column": str(column),
            "dtype": str(getattr(series, "dtype", "")),
            "purpose_hint": infer_column_purpose(str(column)),
            "non_null": int(clean.shape[0]),
            "null_count": int(rows_total - int(clean.shape[0])),
            "unique_count": int(clean.nunique(dropna=True)),
            "sample_values": [str(v) for v in clean.astype(str).head(3).tolist()],
        }
        profiles.append(profile)
    return profiles


def _build_numeric_summary(frame: Any, *, max_columns: int = 20) -> List[Dict[str, Any]]:
    import pandas as pd

    summary: List[Dict[str, Any]] = []
    columns = list(getattr(frame, "columns", []))[:max_columns]
    rows_total = max(1, int(len(frame)))
    for column in columns:
        series = pd.to_numeric(frame[column], errors="coerce").dropna()
        if int(series.shape[0]) < max(3, int(rows_total * 0.1)):
            continue
        summary.append(
            {
                "column": str(column),
                "count": int(series.shape[0]),
                "min": float(series.min()),
                "max": float(series.max()),
                "mean": float(series.mean()),
                "median": float(series.median()),
            }
        )
    return summary


def _build_datetime_summary(frame: Any, *, max_columns: int = 20) -> List[Dict[str, Any]]:
    import pandas as pd
    import warnings

    summary: List[Dict[str, Any]] = []
    columns = list(getattr(frame, "columns", []))[:max_columns]
    rows_total = max(1, int(len(frame)))
    for column in columns:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=UserWarning,
                message=r"Could not infer format, so each element will be parsed individually.*",
            )
            series = pd.to_datetime(frame[column], errors="coerce")
        clean = series.dropna()
        if int(clean.shape[0]) < max(3, int(rows_total * 0.1)):
            continue
        summary.append(
            {
                "column": str(column),
                "min": str(clean.min()),
                "max": str(clean.max()),
            }
        )
    return summary


def _build_categorical_summary(frame: Any, *, max_columns: int = 30) -> List[Dict[str, Any]]:
    summary: List[Dict[str, Any]] = []
    columns = list(getattr(frame, "columns", []))[:max_columns]
    for column in columns:
        series = frame[column].dropna().astype(str).str.strip()
        series = series[series != ""]
        if int(series.shape[0]) == 0:
            continue
        unique_count = int(series.nunique(dropna=True))
        if unique_count <= 1 or unique_count > 60:
            continue
        top_values = series.value_counts().head(8)
        summary.append(
            {
                "column": str(column),
                "top_values": {str(k): int(v) for k, v in top_values.items()},
            }
        )
    return summary


def _build_relationship_findings(frame: Any, *, max_pairs: int = 8) -> List[Dict[str, Any]]:
    import pandas as pd

    findings: List[Dict[str, Any]] = []
    numeric_columns: List[str] = []
    for column in list(getattr(frame, "columns", []))[:40]:
        numeric = pd.to_numeric(frame[column], errors="coerce")
        if int(numeric.notna().sum()) >= 3:
            numeric_columns.append(str(column))
    if len(numeric_columns) < 2:
        return findings

    corr_input = frame[numeric_columns].apply(pd.to_numeric, errors="coerce")
    corr_matrix = corr_input.corr(numeric_only=True)
    if corr_matrix is None or corr_matrix.empty:
        return findings

    pairs = []
    cols = list(corr_matrix.columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            value = corr_matrix.iloc[i, j]
            if pd.notna(value):
                pairs.append((abs(float(value)), float(value), str(cols[i]), str(cols[j])))
    pairs = sorted(pairs, reverse=True)[:max_pairs]
    for _, value, left, right in pairs:
        findings.append(
            {
                "feature_a": left,
                "feature_b": right,
                "correlation": round(float(value), 4),
            }
        )
    return findings


def enrich_metrics_from_dataframe(*, metrics: Dict[str, Any], frame: Any) -> Dict[str, Any]:
    enriched = dict(metrics or {})
    if frame is None:
        return enriched

    columns = [str(c) for c in list(getattr(frame, "columns", []))]
    rows_total = int(len(frame))
    enriched.setdefault("rows_total", rows_total)
    enriched.setdefault("columns_total", int(len(columns)))
    enriched.setdefault("columns", columns)
    if not str(enriched.get("potential_process") or "").strip():
        enriched["potential_process"] = infer_process_context(columns)

    if not isinstance(enriched.get("column_profile"), list) or not enriched.get("column_profile"):
        enriched["column_profile"] = _build_column_profile(frame)
    if not isinstance(enriched.get("numeric_summary"), list) or not enriched.get("numeric_summary"):
        enriched["numeric_summary"] = _build_numeric_summary(frame)
    if not isinstance(enriched.get("datetime_summary"), list):
        enriched["datetime_summary"] = _build_datetime_summary(frame)
    if not isinstance(enriched.get("categorical_summary"), list) or not enriched.get("categorical_summary"):
        enriched["categorical_summary"] = _build_categorical_summary(frame)
    if not isinstance(enriched.get("relationship_findings"), list):
        enriched["relationship_findings"] = _build_relationship_findings(frame)
    if not enriched.get("relationship_findings"):
        enriched["relationship_findings"] = _build_relationship_findings(frame)

    insights = enriched.get("insights") if isinstance(enriched.get("insights"), list) else []
    if not insights:
        insights = []
    summary_line = f"Dataset rows={rows_total}, columns={len(columns)}."
    if summary_line not in insights:
        insights.append(summary_line)
    numeric_count = int(len(enriched.get("numeric_summary") or []))
    rel_count = int(len(enriched.get("relationship_findings") or []))
    coverage_line = (
        f"Detected numeric columns={numeric_count}, relationship pairs={rel_count}, "
        f"categorical summaries={len(enriched.get('categorical_summary') or [])}."
    )
    if coverage_line not in insights:
        insights.append(coverage_line)
    enriched["insights"] = insights[:16]
    return enriched


# Compatibility aliases.
_infer_column_purpose = infer_column_purpose
_infer_process_context = infer_process_context
_enrich_metrics_from_dataframe = enrich_metrics_from_dataframe

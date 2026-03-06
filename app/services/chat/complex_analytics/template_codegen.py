from __future__ import annotations

import json

from .planner import is_dependency_query


def build_complex_analysis_code(*, query: str, primary_table_name: str) -> str:
    q_lower = (query or "").lower()
    needs_visual = True
    needs_nlp = any(
        token in q_lower
        for token in (
            "nlp",
            "comment_text",
            "text",
            "коммент",
            "текст",
            "token",
        )
    )
    needs_dependency = is_dependency_query(q_lower)

    return f"""
import pandas as pd
import numpy as np
import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import re
import warnings
from datetime import datetime
try:
    import seaborn as sns
except Exception:
    sns = None

df = datasets[{json.dumps(primary_table_name)}].copy()
result = {{
    "status": "ok",
    "table_name": {json.dumps(primary_table_name)},
    "metrics": {{}},
    "notes": [],
    "artifacts": [],
    "insights": []
}}

result["metrics"]["rows_total"] = int(len(df))
result["metrics"]["columns_total"] = int(len(df.columns))
result["metrics"]["columns"] = [str(c) for c in df.columns]
result["metrics"]["insights"] = []

con = duckdb.connect(database=":memory:")
con.register("df", df)
duck_rows = con.execute("SELECT COUNT(*) AS cnt FROM df").fetchone()[0]
result["metrics"]["rows_total_duckdb"] = int(duck_rows or 0)
con.close()

def to_datetime_silent(series):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        warnings.filterwarnings(
            "ignore",
            message=r"Could not infer format, so each element will be parsed individually.*",
        )
        try:
            return pd.to_datetime(series, errors="coerce")
        except TypeError:
            return pd.to_datetime(series, errors="coerce")

def infer_column_purpose(column_name):
    c = str(column_name).lower()
    if "id" in c:
        return "identifier/key column"
    if "date" in c or "time" in c:
        return "time/event timestamp"
    if "comment" in c or "text" in c or "descr" in c:
        return "free-text/narrative field"
    if "office" in c or "region" in c or "city" in c or "branch" in c:
        return "organizational or location dimension"
    if "status" in c or "stage" in c:
        return "process state dimension"
    if "amount" in c or "sum" in c or "price" in c or "cost" in c or "total" in c:
        return "financial metric field"
    if "count" in c or "qty" in c:
        return "volume/count metric field"
    return "attribute used for segmentation/analysis"

def infer_process_context(columns):
    cols = set([str(c).lower() for c in columns])
    if set(["application_id", "comment_time", "comment_text", "office"]).issubset(cols):
        return "Application review / processing workflow with distributed offices and analyst comments."
    if ("order_id" in cols or "invoice_id" in cols) and ("status" in cols or "stage" in cols):
        return "Order-to-cash / document processing workflow."
    if ("ticket_id" in cols or "incident_id" in cols) and ("status" in cols or "comment_text" in cols):
        return "Support or incident management workflow."
    return "Likely an operational process dataset with records, dimensions, and process indicators."

column_profiles = []
numeric_summaries = []
datetime_summaries = []
categorical_summaries = []

rows_total = int(len(df))
for col in df.columns:
    series = df[col]
    non_null = int(series.notna().sum())
    null_count = int(rows_total - non_null)
    unique_count = int(series.nunique(dropna=True))
    sample_values = [str(v) for v in series.dropna().astype(str).head(3).tolist()]
    purpose_hint = infer_column_purpose(col)

    profile = {{
        "column": str(col),
        "dtype": str(series.dtype),
        "purpose_hint": purpose_hint,
        "non_null": non_null,
        "null_count": null_count,
        "unique_count": unique_count,
        "sample_values": sample_values,
    }}

    numeric_series = pd.to_numeric(series, errors="coerce")
    numeric_non_null = int(numeric_series.notna().sum())
    if numeric_non_null > 0 and numeric_non_null >= max(3, int(rows_total * 0.1)):
        profile["numeric_detected"] = True
        num_clean = numeric_series.dropna()
        if len(num_clean) > 0:
            numeric_summaries.append(
                {{
                    "column": str(col),
                    "count": int(len(num_clean)),
                    "min": float(num_clean.min()),
                    "max": float(num_clean.max()),
                    "mean": float(num_clean.mean()),
                    "median": float(num_clean.median()),
                }}
            )

    dt_series = to_datetime_silent(series)
    dt_non_null = int(dt_series.notna().sum())
    if dt_non_null > 0 and dt_non_null >= max(3, int(rows_total * 0.1)):
        profile["datetime_detected"] = True
        dt_clean = dt_series.dropna()
        if len(dt_clean) > 0:
            datetime_summaries.append(
                {{
                    "column": str(col),
                    "min": str(dt_clean.min()),
                    "max": str(dt_clean.max()),
                }}
            )

    if unique_count > 1 and unique_count <= 40:
        ser = series.dropna().astype(str).str.strip()
        ser = ser[ser != ""]
        if len(ser) > 0:
            top_counts = ser.value_counts().head(8)
            categorical_summaries.append(
                {{
                    "column": str(col),
                    "top_values": {{str(k): int(v) for k, v in top_counts.items()}},
                }}
            )

    column_profiles.append(profile)

result["metrics"]["column_profile"] = column_profiles
result["metrics"]["numeric_summary"] = numeric_summaries
result["metrics"]["datetime_summary"] = datetime_summaries
result["metrics"]["categorical_summary"] = categorical_summaries
result["metrics"]["potential_process"] = infer_process_context(df.columns)
result["metrics"]["insights"].append(f"Dataset rows={{int(len(df))}}, columns={{int(len(df.columns))}}")
result["metrics"]["insights"].append(f"Detected numeric columns={{len(numeric_summaries)}}, datetime columns={{len(datetime_summaries)}}")

if "application_id" in df.columns:
    result["metrics"]["application_id_unique"] = int(df["application_id"].nunique(dropna=True))

if "office" in df.columns:
    office = df["office"].astype(str).str.strip()
    office = office[office != ""]
    office_counts = office.value_counts().head(10)
    if len(office_counts) > 0:
        result["metrics"]["office_top"] = {{str(k): int(v) for k, v in office_counts.items()}}

if "comment_time" in df.columns:
    ts = to_datetime_silent(df["comment_time"])
    valid_ts = ts.dropna()
    if len(valid_ts) > 0:
        result["metrics"]["comment_time_min"] = str(valid_ts.min())
        result["metrics"]["comment_time_max"] = str(valid_ts.max())
    else:
        result["notes"].append("comment_time exists but could not be parsed to datetime")

if {str(needs_nlp)} and "comment_text" in df.columns:
    txt = df["comment_text"].astype(str).str.lower()
    txt = txt.str.replace(r"[^a-zа-я0-9\\s]", " ", regex=True)
    tokens = txt.str.split().explode()
    if tokens is not None:
        tokens = tokens[tokens.str.len() > 2]
    if tokens is not None and len(tokens) > 0:
        top_tokens = tokens.value_counts().head(20)
        result["metrics"]["comment_top_tokens"] = {{str(k): int(v) for k, v in top_tokens.items()}}
    else:
        result["notes"].append("NLP requested but no tokens extracted from comment_text")
elif {str(needs_nlp)}:
    result["notes"].append("NLP requested but comment_text column was not found")

numeric_cols = [str(item["column"]) for item in numeric_summaries if str(item.get("column") or "").strip()]
relationship_findings = []
if len(numeric_cols) >= 2:
    corr_input = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    corr_matrix = corr_input.corr(numeric_only=True)
    if corr_matrix is not None and not corr_matrix.empty:
        pairs = []
        cols = list(corr_matrix.columns)
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                value = corr_matrix.iloc[i, j]
                if pd.notna(value):
                    pairs.append((abs(float(value)), float(value), str(cols[i]), str(cols[j])))
        pairs = sorted(pairs, reverse=True)[:8]
        for _, value, c1, c2 in pairs:
            relationship_findings.append({{"feature_a": c1, "feature_b": c2, "correlation": round(float(value), 4)}})
if relationship_findings:
    result["metrics"]["relationship_findings"] = relationship_findings
    result["metrics"]["insights"].append("Computed pairwise numeric feature relationships.")

if {str(needs_visual)}:
    max_visuals = 3

    def can_add_visual():
        return len(result["artifacts"]) < max_visuals

    if can_add_visual() and len(numeric_cols) >= 2:
        numeric_df = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        corr = numeric_df.corr(numeric_only=True)
        if corr is not None and not corr.empty:
            fig, ax = plt.subplots(figsize=(8, 6))
            if sns is not None:
                sns.heatmap(corr, annot=True, cmap="RdBu_r", center=0, ax=ax)
            else:
                image = ax.imshow(corr.to_numpy(), cmap="RdBu_r")
                ax.figure.colorbar(image, ax=ax)
                ax.set_xticks(range(len(corr.columns)))
                ax.set_xticklabels([str(x) for x in corr.columns], rotation=45, ha="right")
                ax.set_yticks(range(len(corr.index)))
                ax.set_yticklabels([str(x) for x in corr.index])
            ax.set_title("Numeric correlation heatmap")
            plot_path = save_plot(fig=fig, name="numeric_correlation_heatmap.png")
            plt.close(fig)
            result["artifacts"].append({{"kind": "correlation_heatmap", "path": plot_path}})
            result["metrics"]["insights"].append("Built numeric correlation heatmap.")

    if can_add_visual() and len(numeric_cols) > 0:
        col = str(numeric_cols[0])
        numeric_values = pd.to_numeric(df[col], errors="coerce").dropna()
        if len(numeric_values) > 0:
            fig, ax = plt.subplots(figsize=(8, 4))
            numeric_values.astype(float).plot(kind="hist", bins=20, ax=ax, title=f"Distribution of {{col}}")
            plot_path = save_plot(fig=fig, name="numeric_distribution.png")
            plt.close(fig)
            result["artifacts"].append({{"kind": "histogram", "column": str(col), "path": plot_path}})
            result["metrics"]["insights"].append(f"Built distribution chart for {{col}}.")

    if can_add_visual() and len(numeric_cols) >= 2 and ({str(needs_dependency)} or len(relationship_findings) > 0):
        col_x = str(numeric_cols[0])
        col_y = str(numeric_cols[1])
        plot_df = df[[col_x, col_y]].copy()
        plot_df[col_x] = pd.to_numeric(plot_df[col_x], errors="coerce")
        plot_df[col_y] = pd.to_numeric(plot_df[col_y], errors="coerce")
        plot_df = plot_df.dropna()
        if len(plot_df) >= 3:
            fig, ax = plt.subplots(figsize=(7, 5))
            ax.scatter(plot_df[col_x], plot_df[col_y], alpha=0.7, color="#4c78a8")
            ax.set_xlabel(col_x)
            ax.set_ylabel(col_y)
            ax.set_title(f"Scatter: {{col_x}} vs {{col_y}}")
            plot_path = save_plot(fig=fig, name="feature_relationship_scatter.png")
            plt.close(fig)
            result["artifacts"].append({{"kind": "scatter", "x": col_x, "y": col_y, "path": plot_path}})
            result["metrics"]["insights"].append(f"Built scatter chart for {{col_x}} vs {{col_y}}.")

    if can_add_visual():
        for col in df.columns:
            ser = df[col].dropna().astype(str).str.strip()
            ser = ser[ser != ""]
            if len(ser) == 0:
                continue
            value_counts = ser.value_counts().head(10)
            if len(value_counts) <= 1:
                continue
            fig, ax = plt.subplots(figsize=(10, 4))
            ax.bar([str(x) for x in value_counts.index], [int(v) for v in value_counts.values], color="#4c78a8")
            ax.set_title(f"Top values in {{col}}")
            ax.set_ylabel("Count")
            ax.tick_params(axis="x", labelrotation=45)
            plot_path = save_plot(fig=fig, name=f"top_values_{{str(col)}}.png")
            plt.close(fig)
            result["artifacts"].append({{"kind": "categorical_bar", "column": str(col), "path": plot_path}})
            result["metrics"]["insights"].append(f"Built category distribution chart for {{col}}.")
            break

    if len(result["artifacts"]) == 0:
        result["notes"].append("Visualization requested but no suitable columns were found for charting.")
""".strip()


from __future__ import annotations


def inject_visualization_fallback(candidate: str) -> str:
    base = str(candidate or "").rstrip()
    suffix = """

# Auto-injected safe visualization fallback to satisfy visualization contract.
try:
    import pandas as pd
    import matplotlib.pyplot as plt
except Exception:
    pd = None
    plt = None

try:
    _existing_result = result
except Exception:
    _existing_result = None
_result_has_dict_interface = True
try:
    _existing_result.get("artifacts")
except Exception:
    _result_has_dict_interface = False
if not _result_has_dict_interface:
    _auto_table_name = list(datasets.keys())[0]
    result = {"status": "ok", "table_name": _auto_table_name, "metrics": {}, "notes": [], "artifacts": []}
_artifacts_appendable = True
try:
    result["artifacts"].append
except Exception:
    _artifacts_appendable = False
if not _artifacts_appendable:
    result["artifacts"] = []
if pd is not None and plt is not None and len(result["artifacts"]) == 0:
    _auto_table_name = result.get("table_name")
    if _auto_table_name not in datasets:
        _auto_table_name = list(datasets.keys())[0]
    _auto_df = datasets[_auto_table_name].copy()
    _auto_saved = False
    _auto_numeric = _auto_df.apply(pd.to_numeric, errors="coerce")
    _auto_numeric_cols = [str(c) for c in _auto_numeric.columns if int(_auto_numeric[c].notna().sum()) > 0]
    if len(_auto_numeric_cols) > 0:
        _auto_col = _auto_numeric_cols[0]
        _auto_clean = _auto_numeric[_auto_col].dropna()
        if len(_auto_clean) > 0:
            _auto_fig, _auto_ax = plt.subplots(figsize=(8, 4))
            _auto_clean.astype(float).plot(kind="hist", bins=20, ax=_auto_ax, title=f"Distribution of {_auto_col}")
            _auto_path = save_plot(fig=_auto_fig, name="auto_visual_fallback_hist.png")
            plt.close(_auto_fig)
            result["artifacts"].append({"kind": "histogram", "column": str(_auto_col), "path": _auto_path})
            _auto_saved = True
    if not _auto_saved:
        for _auto_col in _auto_df.columns:
            _auto_ser = _auto_df[_auto_col].dropna().astype(str).str.strip()
            _auto_ser = _auto_ser[_auto_ser != ""]
            if len(_auto_ser) == 0:
                continue
            _auto_counts = _auto_ser.value_counts().head(10)
            if len(_auto_counts) <= 1:
                continue
            _auto_fig, _auto_ax = plt.subplots(figsize=(10, 4))
            _auto_ax.bar([str(x) for x in _auto_counts.index], [int(v) for v in _auto_counts.values], color="#4c78a8")
            _auto_ax.set_title(f"Top values in {_auto_col}")
            _auto_ax.tick_params(axis="x", labelrotation=45)
            _auto_path = save_plot(fig=_auto_fig, name="auto_visual_fallback_bar.png")
            plt.close(_auto_fig)
            result["artifacts"].append({"kind": "categorical_bar", "column": str(_auto_col), "path": _auto_path})
            _auto_saved = True
            break
    if not _auto_saved:
        result.setdefault("notes", [])
        _notes_appendable = True
        try:
            result["notes"].append
        except Exception:
            _notes_appendable = False
        if _notes_appendable:
            result["notes"].append("Visualization requested but no suitable columns were found for charting.")
""".strip(
        "\n"
    )
    return f"{base}\n\n{suffix}\n"

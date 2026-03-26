from __future__ import annotations

from dataclasses import dataclass
import json
import re
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence, Tuple

from app.services.tabular.parsing import infer_series_kind

TABULAR_COLUMN_METADATA_CONTRACT_VERSION = "tabular_column_metadata_v1"


@dataclass(frozen=True)
class ColumnMetadataBudgetConfig:
    max_aliases_per_column: int = 6
    alias_max_chars: int = 96
    max_sample_values_per_column: int = 5
    sample_value_max_chars: int = 80
    cardinality_scan_rows: int = 256
    max_column_metadata_bytes: int = 48_000


_WHITESPACE_RE = re.compile(r"\s+")
_VALID_CARDINALITY_HINTS = {"empty", "single", "low", "medium", "high"}


def _normalize_space(value: str) -> str:
    return _WHITESPACE_RE.sub(" ", str(value or "").strip())


def _dedupe_key(value: str) -> str:
    return _normalize_space(value).lower()


def _truncate_text(value: Any, *, max_chars: int) -> str:
    text = _normalize_space(str(value or ""))
    if not text:
        return ""
    if int(max_chars) > 0 and len(text) > int(max_chars):
        return text[: int(max_chars)].rstrip()
    return text


def _sanitize_list(
    values: Sequence[Any],
    *,
    max_items: int,
    max_chars: int,
) -> Tuple[List[str], int]:
    sanitized: List[str] = []
    seen = set()
    dropped = 0
    for raw in list(values or []):
        item = _truncate_text(raw, max_chars=max_chars)
        if not item:
            dropped += 1
            continue
        key = _dedupe_key(item)
        if key in seen:
            dropped += 1
            continue
        if len(sanitized) >= int(max_items):
            dropped += 1
            continue
        seen.add(key)
        sanitized.append(item)
    return sanitized, dropped


def _collect_non_empty_series_values(
    series: Any,
    *,
    max_rows: int,
    max_chars: int,
) -> List[str]:
    if series is None:
        return []
    try:
        raw_values = series.tolist()
    except Exception:
        try:
            raw_values = list(series)
        except Exception:
            return []

    out: List[str] = []
    for raw in raw_values:
        if len(out) >= int(max_rows):
            break
        value = _truncate_text(raw, max_chars=max_chars)
        if value:
            out.append(value)
    return out


def _canonical_dtype(value: Any) -> str:
    text = _dedupe_key(str(value or ""))
    if not text:
        return "unknown"
    if any(token in text for token in ("int", "integer")):
        return "integer"
    if any(token in text for token in ("float", "double", "decimal", "numeric", "number")):
        return "numeric"
    if any(token in text for token in ("date", "time", "timestamp")):
        return "datetime"
    if any(token in text for token in ("bool", "boolean")):
        return "boolean"
    if any(token in text for token in ("text", "string", "str", "object", "category", "varchar", "char")):
        return "text"
    if text == "empty":
        return "empty"
    return text


def _infer_dtype_from_series(series: Any) -> str:
    if series is None:
        return "unknown"
    inferred = infer_series_kind(series)
    mapped = {
        "integer": "integer",
        "number": "numeric",
        "boolean": "boolean",
        "datetime_like": "datetime",
        "text": "text",
        "empty": "empty",
    }.get(str(inferred or "").strip().lower(), "unknown")
    return mapped


def _derive_cardinality_hint(values: Sequence[str]) -> str:
    non_empty = [str(item).strip() for item in list(values or []) if str(item).strip()]
    if not non_empty:
        return "empty"
    unique = {_dedupe_key(item) for item in non_empty}
    unique_count = len(unique)
    if unique_count <= 1:
        return "single"
    sample_count = len(non_empty)
    ratio = float(unique_count / max(1, sample_count))
    if unique_count <= 12 and ratio <= 0.35:
        return "low"
    if unique_count <= 64 and ratio <= 0.8:
        return "medium"
    return "high"


def _metadata_size_bytes(metadata: Mapping[str, Mapping[str, Any]]) -> int:
    return len(json.dumps(metadata, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _summarize_metadata_stats(
    *,
    metadata: Mapping[str, Mapping[str, Any]],
    columns_total: int,
    aliases_trimmed_total: int,
    sample_values_trimmed_total: int,
    metadata_columns_dropped: int,
    metadata_budget_bytes: int,
    metadata_budget_enforced: bool,
) -> Dict[str, Any]:
    aliases_total = 0
    sample_values_total = 0
    columns_with_samples = 0
    for entry in metadata.values():
        aliases = entry.get("aliases")
        if isinstance(aliases, list):
            aliases_total += len(aliases)
        samples = entry.get("sample_values")
        if isinstance(samples, list):
            sample_values_total += len(samples)
            if samples:
                columns_with_samples += 1
    return {
        "columns_total": int(columns_total),
        "columns_with_metadata": int(len(metadata)),
        "columns_with_samples": int(columns_with_samples),
        "aliases_total": int(aliases_total),
        "sample_values_total": int(sample_values_total),
        "aliases_trimmed_total": int(max(0, aliases_trimmed_total)),
        "sample_values_trimmed_total": int(max(0, sample_values_trimmed_total)),
        "metadata_columns_dropped": int(max(0, metadata_columns_dropped)),
        "metadata_bytes": int(_metadata_size_bytes(metadata)),
        "metadata_budget_bytes": int(metadata_budget_bytes),
        "metadata_budget_enforced": bool(metadata_budget_enforced),
    }


def _enforce_metadata_budget(
    *,
    metadata: MutableMapping[str, Dict[str, Any]],
    column_order: Sequence[str],
    config: ColumnMetadataBudgetConfig,
    aliases_trimmed_total: int,
    sample_values_trimmed_total: int,
) -> Dict[str, Any]:
    budget = int(config.max_column_metadata_bytes)
    columns_dropped = 0
    budget_enforced = False

    if _metadata_size_bytes(metadata) > budget:
        budget_enforced = True

        for column in column_order:
            entry = metadata.get(column)
            if not isinstance(entry, dict):
                continue
            removed = entry.pop("sample_values", None)
            if isinstance(removed, list):
                sample_values_trimmed_total += len(removed)
        if _metadata_size_bytes(metadata) > budget:
            for column in column_order:
                entry = metadata.get(column)
                if not isinstance(entry, dict):
                    continue
                aliases = entry.get("aliases")
                if isinstance(aliases, list) and len(aliases) > 1:
                    aliases_trimmed_total += len(aliases) - 1
                    entry["aliases"] = aliases[:1]
        if _metadata_size_bytes(metadata) > budget:
            for column in column_order:
                entry = metadata.get(column)
                if not isinstance(entry, dict):
                    continue
                if "cardinality_hint" in entry:
                    entry.pop("cardinality_hint", None)
        if _metadata_size_bytes(metadata) > budget:
            for column in reversed(list(column_order)):
                if column in metadata:
                    metadata.pop(column, None)
                    columns_dropped += 1
                    if _metadata_size_bytes(metadata) <= budget:
                        break

    return _summarize_metadata_stats(
        metadata=metadata,
        columns_total=len(column_order),
        aliases_trimmed_total=aliases_trimmed_total,
        sample_values_trimmed_total=sample_values_trimmed_total,
        metadata_columns_dropped=columns_dropped,
        metadata_budget_bytes=budget,
        metadata_budget_enforced=budget_enforced,
    )


def _build_metadata_entry(
    *,
    column: str,
    raw_name: str,
    display_name: str,
    aliases: Sequence[Any],
    dtype: str,
    sample_values: Sequence[Any],
    cardinality_hint: Optional[str],
    config: ColumnMetadataBudgetConfig,
) -> Tuple[Dict[str, Any], int, int]:
    alias_list, aliases_trimmed = _sanitize_list(
        list(aliases) + [display_name, raw_name, column.replace("_", " ")],
        max_items=int(config.max_aliases_per_column),
        max_chars=int(config.alias_max_chars),
    )
    if not alias_list:
        alias_list = [_truncate_text(display_name or raw_name or column, max_chars=int(config.alias_max_chars))]

    sample_list, samples_trimmed = _sanitize_list(
        list(sample_values),
        max_items=int(config.max_sample_values_per_column),
        max_chars=int(config.sample_value_max_chars),
    )

    entry: Dict[str, Any] = {
        "raw_name": _truncate_text(raw_name or column, max_chars=int(config.alias_max_chars)),
        "normalized_name": str(column),
        "display_name": _truncate_text(display_name or raw_name or column, max_chars=int(config.alias_max_chars)),
        "aliases": alias_list,
        "dtype": _canonical_dtype(dtype),
    }
    if sample_list:
        entry["sample_values"] = sample_list
    hint = str(cardinality_hint or "").strip().lower()
    if hint in _VALID_CARDINALITY_HINTS:
        entry["cardinality_hint"] = hint
    return entry, aliases_trimmed, samples_trimmed


def build_dataframe_column_metadata(
    *,
    df: Any,
    columns: Sequence[str],
    aliases: Mapping[str, Any],
    config: Optional[ColumnMetadataBudgetConfig] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    cfg = config or ColumnMetadataBudgetConfig()
    ordered_columns = [str(col) for col in list(columns or [])]
    alias_map = {str(key): str(value) for key, value in dict(aliases or {}).items()}

    metadata: Dict[str, Dict[str, Any]] = {}
    aliases_trimmed_total = 0
    sample_values_trimmed_total = 0

    df_columns_raw = getattr(df, "columns", None)
    df_columns = [str(col) for col in list(df_columns_raw)] if df_columns_raw is not None else []
    for index, column in enumerate(ordered_columns):
        fallback_name = f"col_{index + 1}"
        raw_name = _truncate_text(alias_map.get(column) or column or fallback_name, max_chars=int(cfg.alias_max_chars))
        display_name = raw_name

        series = None
        if column in df_columns:
            try:
                series = df[column]
            except Exception:
                series = None

        observed_values = _collect_non_empty_series_values(
            series,
            max_rows=int(cfg.cardinality_scan_rows),
            max_chars=max(int(cfg.sample_value_max_chars) * 4, int(cfg.sample_value_max_chars)),
        )
        dtype_hint = _infer_dtype_from_series(series)
        cardinality_hint = _derive_cardinality_hint(observed_values)
        entry, aliases_trimmed, samples_trimmed = _build_metadata_entry(
            column=column,
            raw_name=raw_name,
            display_name=display_name,
            aliases=[display_name],
            dtype=dtype_hint,
            sample_values=observed_values,
            cardinality_hint=cardinality_hint,
            config=cfg,
        )
        metadata[column] = entry
        aliases_trimmed_total += aliases_trimmed
        sample_values_trimmed_total += samples_trimmed

    stats = _enforce_metadata_budget(
        metadata=metadata,
        column_order=ordered_columns,
        config=cfg,
        aliases_trimmed_total=aliases_trimmed_total,
        sample_values_trimmed_total=sample_values_trimmed_total,
    )
    return metadata, stats


def sanitize_tabular_column_metadata(
    *,
    raw_metadata: Any,
    columns: Optional[Sequence[str]],
    aliases: Optional[Mapping[str, Any]] = None,
    config: Optional[ColumnMetadataBudgetConfig] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    cfg = config or ColumnMetadataBudgetConfig()
    raw_map = raw_metadata if isinstance(raw_metadata, Mapping) else {}
    alias_map = {str(key): str(value) for key, value in dict(aliases or {}).items()}
    ordered_columns = [str(col) for col in list(columns or [])]
    if not ordered_columns:
        ordered_columns = [str(key) for key in raw_map.keys()]

    metadata: Dict[str, Dict[str, Any]] = {}
    aliases_trimmed_total = 0
    sample_values_trimmed_total = 0

    for index, column in enumerate(ordered_columns):
        payload = raw_map.get(column)
        payload_map = payload if isinstance(payload, Mapping) else {}
        fallback_name = f"col_{index + 1}"

        raw_name = _truncate_text(
            payload_map.get("raw_name") or alias_map.get(column) or payload_map.get("display_name") or column or fallback_name,
            max_chars=int(cfg.alias_max_chars),
        )
        display_name = _truncate_text(
            payload_map.get("display_name") or raw_name or column or fallback_name,
            max_chars=int(cfg.alias_max_chars),
        )
        dtype_hint = _canonical_dtype(payload_map.get("dtype"))
        aliases_raw = payload_map.get("aliases")
        alias_candidates = aliases_raw if isinstance(aliases_raw, list) else []
        sample_values_raw = payload_map.get("sample_values")
        sample_candidates = sample_values_raw if isinstance(sample_values_raw, list) else []
        cardinality_hint = str(payload_map.get("cardinality_hint") or "").strip().lower()
        if cardinality_hint not in _VALID_CARDINALITY_HINTS:
            cardinality_hint = _derive_cardinality_hint(sample_candidates)

        entry, aliases_trimmed, samples_trimmed = _build_metadata_entry(
            column=column,
            raw_name=raw_name,
            display_name=display_name,
            aliases=alias_candidates,
            dtype=dtype_hint,
            sample_values=sample_candidates,
            cardinality_hint=cardinality_hint,
            config=cfg,
        )
        metadata[column] = entry
        aliases_trimmed_total += aliases_trimmed
        sample_values_trimmed_total += samples_trimmed

    stats = _enforce_metadata_budget(
        metadata=metadata,
        column_order=ordered_columns,
        config=cfg,
        aliases_trimmed_total=aliases_trimmed_total,
        sample_values_trimmed_total=sample_values_trimmed_total,
    )
    return metadata, stats


def aggregate_tabular_column_metadata_stats(stats_items: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    items = [item for item in list(stats_items or []) if isinstance(item, Mapping)]
    total = {
        "tables_count": len(items),
        "columns_total": 0,
        "columns_with_metadata": 0,
        "columns_with_samples": 0,
        "aliases_total": 0,
        "sample_values_total": 0,
        "aliases_trimmed_total": 0,
        "sample_values_trimmed_total": 0,
        "metadata_columns_dropped": 0,
        "metadata_bytes": 0,
        "metadata_budget_bytes": 0,
        "metadata_budget_enforced": False,
    }
    for item in items:
        total["columns_total"] += int(item.get("columns_total", 0) or 0)
        total["columns_with_metadata"] += int(item.get("columns_with_metadata", 0) or 0)
        total["columns_with_samples"] += int(item.get("columns_with_samples", 0) or 0)
        total["aliases_total"] += int(item.get("aliases_total", 0) or 0)
        total["sample_values_total"] += int(item.get("sample_values_total", 0) or 0)
        total["aliases_trimmed_total"] += int(item.get("aliases_trimmed_total", 0) or 0)
        total["sample_values_trimmed_total"] += int(item.get("sample_values_trimmed_total", 0) or 0)
        total["metadata_columns_dropped"] += int(item.get("metadata_columns_dropped", 0) or 0)
        total["metadata_bytes"] += int(item.get("metadata_bytes", 0) or 0)
        total["metadata_budget_bytes"] += int(item.get("metadata_budget_bytes", 0) or 0)
        total["metadata_budget_enforced"] = bool(
            total["metadata_budget_enforced"] or bool(item.get("metadata_budget_enforced", False))
        )
    return total

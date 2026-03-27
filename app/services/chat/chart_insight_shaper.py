from __future__ import annotations

import json
import math
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.services.chat.language import normalize_preferred_response_language


_YEAR_BUCKET_RE = re.compile(r"^(?P<year>\d{4})$")
_YEAR_MONTH_BUCKET_RE = re.compile(r"^(?P<year>\d{4})[-/](?P<month>\d{1,2})$")
_YEAR_MONTH_DAY_BUCKET_RE = re.compile(r"^(?P<year>\d{4})[-/](?P<month>\d{1,2})[-/](?P<day>\d{1,2})$")
_YEAR_QUARTER_BUCKET_RE = re.compile(r"^(?P<year>\d{4})[-/ ]?[qQ](?P<quarter>[1-4])$")
_QUARTER_YEAR_BUCKET_RE = re.compile(r"^[qQ](?P<quarter>[1-4])[-/ ]?(?P<year>\d{4})$")


def _to_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _format_number(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _format_percent(value: float) -> str:
    return f"{value:.1f}%"


def _parse_tabular_rows(result_text: str) -> List[List[Any]]:
    raw = str(result_text or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    rows: List[List[Any]] = []
    for item in parsed:
        if isinstance(item, list):
            rows.append(item)
    return rows


def _parse_chart_buckets(result_text: str) -> List[Dict[str, Any]]:
    rows = _parse_tabular_rows(result_text)
    buckets: List[Dict[str, Any]] = []
    for row_index, row in enumerate(rows):
        if len(row) < 2:
            continue
        bucket = str(row[0]).strip()
        value = _to_number(row[1])
        if not bucket or value is None or value < 0:
            continue
        buckets.append({"bucket": bucket, "value": float(value), "row_index": row_index})
    return buckets


def _parse_time_bucket_key(bucket: str) -> Optional[Tuple[int, int, int, int, int, int]]:
    text = str(bucket or "").strip()
    if not text:
        return None

    year_month_day_match = _YEAR_MONTH_DAY_BUCKET_RE.match(text)
    if year_month_day_match:
        year = int(year_month_day_match.group("year"))
        month = int(year_month_day_match.group("month"))
        day = int(year_month_day_match.group("day"))
        if 1 <= month <= 12 and 1 <= day <= 31:
            return year, month, day, 0, 0, 0

    year_month_match = _YEAR_MONTH_BUCKET_RE.match(text)
    if year_month_match:
        year = int(year_month_match.group("year"))
        month = int(year_month_match.group("month"))
        if 1 <= month <= 12:
            return year, month, 1, 0, 0, 0

    year_match = _YEAR_BUCKET_RE.match(text)
    if year_match:
        return int(year_match.group("year")), 1, 1, 0, 0, 0

    year_quarter_match = _YEAR_QUARTER_BUCKET_RE.match(text)
    if year_quarter_match:
        year = int(year_quarter_match.group("year"))
        quarter = int(year_quarter_match.group("quarter"))
        month = 1 + ((quarter - 1) * 3)
        return year, month, 1, 0, 0, 0

    quarter_year_match = _QUARTER_YEAR_BUCKET_RE.match(text)
    if quarter_year_match:
        year = int(quarter_year_match.group("year"))
        quarter = int(quarter_year_match.group("quarter"))
        month = 1 + ((quarter - 1) * 3)
        return year, month, 1, 0, 0, 0

    try:
        iso_text = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso_text)
        return dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second
    except Exception:
        return None


def _is_strictly_increasing(values: Sequence[Tuple[int, int, int, int, int, int]]) -> bool:
    return all(values[idx] < values[idx + 1] for idx in range(len(values) - 1))


def _is_strictly_decreasing(values: Sequence[Tuple[int, int, int, int, int, int]]) -> bool:
    return all(values[idx] > values[idx + 1] for idx in range(len(values) - 1))


def _resolve_time_series_order(
    buckets_in_row_order: Sequence[Dict[str, Any]],
) -> Optional[List[Dict[str, Any]]]:
    if len(buckets_in_row_order) < 3:
        return None
    keyed: List[Tuple[Tuple[int, int, int, int, int, int], Dict[str, Any]]] = []
    for item in buckets_in_row_order:
        key = _parse_time_bucket_key(str(item.get("bucket") or ""))
        if key is None:
            return None
        keyed.append((key, item))

    keys = [item[0] for item in keyed]
    if _is_strictly_increasing(keys):
        return [item[1] for item in keyed]
    if _is_strictly_decreasing(keys):
        keyed.reverse()
        return [item[1] for item in keyed]
    return None


def _classify_distribution_shape(
    *,
    ranked_buckets: Sequence[Dict[str, Any]],
    total_value: float,
) -> str:
    if not ranked_buckets:
        return "unknown"
    buckets_total = len(ranked_buckets)
    if buckets_total <= 2:
        return "sparse"
    if total_value <= 0:
        return "unknown"

    shares = [float(item["value"]) / total_value for item in ranked_buckets]
    top1_share = shares[0]
    top3_share = sum(shares[: min(3, len(shares))])
    top4 = shares[: min(4, len(shares))]
    spread_top4 = (max(top4) - min(top4)) if top4 else 0.0

    if buckets_total >= 6 and top1_share >= 0.45 and top3_share >= 0.75 and sum(shares[3:]) >= 0.12:
        return "long_tail"
    if top1_share >= 0.65 or top3_share >= 0.85:
        return "concentrated"
    if buckets_total >= 4 and top1_share <= 0.32 and spread_top4 <= 0.06:
        return "flat"
    if top1_share <= 0.40 and spread_top4 <= 0.10:
        return "balanced"
    return "mixed"


def _build_top_bucket_highlight(
    *,
    ranked_buckets: Sequence[Dict[str, Any]],
    total_value: float,
) -> str:
    top = ranked_buckets[0]
    top_value = float(top["value"])
    top_bucket = str(top["bucket"])
    if len(ranked_buckets) == 1 or total_value <= 0:
        return f"Top bucket: `{top_bucket}` ({_format_number(top_value)})."

    second = ranked_buckets[1]
    second_value = float(second["value"])
    second_bucket = str(second["bucket"])
    top_share = (top_value / total_value) * 100.0
    second_share = (second_value / total_value) * 100.0
    share_gap = top_share - second_share

    if share_gap >= 12.0:
        return (
            f"Top bucket: `{top_bucket}` ({_format_number(top_value)}, {_format_percent(top_share)} of total). "
            f"Runner-up: `{second_bucket}` ({_format_percent(second_share)})."
        )
    return (
        f"Top bucket: `{top_bucket}` ({_format_number(top_value)}, {_format_percent(top_share)}); "
        f"`{second_bucket}` is close at {_format_percent(second_share)}."
    )


def _build_shape_highlight(
    *,
    shape: str,
    ranked_buckets: Sequence[Dict[str, Any]],
    total_value: float,
) -> Optional[str]:
    if total_value <= 0:
        return "All returned bucket values are zero, so concentration signals are not meaningful yet."

    buckets_total = len(ranked_buckets)
    top_n = min(3, buckets_total)
    top_n_share = (sum(float(item["value"]) for item in ranked_buckets[:top_n]) / total_value) * 100.0
    tail_count = max(0, buckets_total - top_n)
    tail_share = max(0.0, 100.0 - top_n_share)

    if shape == "concentrated":
        return (
            f"Shape: concentrated. Top {top_n} buckets account for {_format_percent(top_n_share)} of total, "
            "so most volume sits in a small set."
        )
    if shape == "long_tail":
        return (
            f"Shape: long-tail. Top {top_n} buckets account for {_format_percent(top_n_share)}, "
            f"while the remaining {tail_count} buckets share {_format_percent(tail_share)}."
        )
    if shape == "balanced":
        return (
            f"Shape: balanced across leading buckets (top {top_n} share is {_format_percent(top_n_share)}), "
            "with no single dominant bucket."
        )
    if shape == "flat":
        return (
            f"Shape: flat across leading buckets (top {top_n} share is {_format_percent(top_n_share)}), "
            "with very small spread between top values."
        )
    if shape == "sparse":
        return f"Shape: sparse ({buckets_total} buckets), so a direct bucket-to-bucket comparison is usually enough."
    if buckets_total > 1:
        return f"Top {top_n} buckets cover {_format_percent(top_n_share)} of total."
    return None


def _summarize_time_trend(
    *,
    time_series_buckets: Sequence[Dict[str, Any]],
) -> Optional[str]:
    if len(time_series_buckets) < 3:
        return None

    values = [float(item["value"]) for item in time_series_buckets]
    if not values:
        return None

    max_value = max(values)
    min_value = min(values)
    value_range = max_value - min_value
    if value_range <= 0:
        return "Trend: flat across time buckets."

    x_values = list(range(len(values)))
    mean_x = sum(x_values) / float(len(x_values))
    mean_y = sum(values) / float(len(values))
    var_x = sum((x - mean_x) ** 2 for x in x_values)
    var_y = sum((y - mean_y) ** 2 for y in values)
    if var_x <= 0 or var_y <= 0:
        return "Trend: flat across time buckets."

    covariance = sum((x_values[idx] - mean_x) * (values[idx] - mean_y) for idx in range(len(values)))
    corr = covariance / math.sqrt(var_x * var_y)
    start_value = values[0]
    end_value = values[-1]
    baseline = max(abs(start_value), 1e-9)
    net_change_pct = ((end_value - start_value) / baseline) * 100.0

    if abs(net_change_pct) < 8.0 or abs(corr) < 0.65:
        return "Trend: no clear directional trend across time buckets."

    start_bucket = str(time_series_buckets[0]["bucket"])
    end_bucket = str(time_series_buckets[-1]["bucket"])
    if end_value > start_value:
        return (
            f"Trend: increasing over time (`{start_bucket}` {_format_number(start_value)} -> "
            f"`{end_bucket}` {_format_number(end_value)}, {_format_percent(net_change_pct)})."
        )
    return (
        f"Trend: decreasing over time (`{start_bucket}` {_format_number(start_value)} -> "
        f"`{end_bucket}` {_format_number(end_value)}, {_format_percent(net_change_pct)})."
    )


def _build_contrast_highlight(
    *,
    ranked_buckets: Sequence[Dict[str, Any]],
    total_value: float,
    shape: str,
) -> Optional[str]:
    if len(ranked_buckets) < 2:
        return None
    top_value = float(ranked_buckets[0]["value"])
    second_value = float(ranked_buckets[1]["value"])
    value_gap = top_value - second_value
    if total_value <= 0:
        return None
    gap_share = (value_gap / total_value) * 100.0

    if shape in {"concentrated", "long_tail"} and gap_share >= 10.0:
        return (
            f"Key contrast: `{ranked_buckets[0]['bucket']}` exceeds `{ranked_buckets[1]['bucket']}` "
            f"by {_format_number(value_gap)} ({_format_percent(gap_share)} points)."
        )
    if shape in {"balanced", "flat"} and abs(gap_share) <= 3.5:
        return (
            f"Key contrast: top ranks are close (`{ranked_buckets[0]['bucket']}` vs "
            f"`{ranked_buckets[1]['bucket']}` differ by {_format_number(abs(value_gap))})."
        )
    return None


def _localize_highlight_line(*, line: str, preferred_lang: str) -> str:
    if normalize_preferred_response_language(preferred_lang) != "ru":
        return line

    localized = str(line or "")
    localized = localized.replace("Top bucket:", "\u041b\u0438\u0434\u0435\u0440:")
    localized = localized.replace("Runner-up:", "\u0412\u0442\u043e\u0440\u043e\u0435 \u043c\u0435\u0441\u0442\u043e:")
    localized = localized.replace("of total", "\u043e\u0442 \u043e\u0431\u0449\u0435\u0433\u043e")
    localized = localized.replace("is close at", "\u0431\u043b\u0438\u0437\u043a\u043e \u043a \u043b\u0438\u0434\u0435\u0440\u0443:")
    localized = localized.replace("Shape:", "\u0421\u0442\u0440\u0443\u043a\u0442\u0443\u0440\u0430:")
    localized = localized.replace("concentrated.", "\u043a\u043e\u043d\u0446\u0435\u043d\u0442\u0440\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u0430\u044f.")
    localized = localized.replace("long-tail.", "\u0434\u043b\u0438\u043d\u043d\u044b\u0439 \u0445\u0432\u043e\u0441\u0442.")
    localized = localized.replace("balanced across leading buckets", "\u0441\u0431\u0430\u043b\u0430\u043d\u0441\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u0430\u044f \u043f\u043e \u043b\u0438\u0434\u0438\u0440\u0443\u044e\u0449\u0438\u043c \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u044f\u043c")
    localized = localized.replace("flat across leading buckets", "\u0440\u043e\u0432\u043d\u0430\u044f \u043f\u043e \u043b\u0438\u0434\u0438\u0440\u0443\u044e\u0449\u0438\u043c \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u044f\u043c")
    localized = localized.replace("sparse", "\u0440\u0430\u0437\u0440\u0435\u0436\u0435\u043d\u043d\u0430\u044f")
    localized = localized.replace("Top ", "\u0422\u043e\u043f-")
    localized = localized.replace("buckets account for", "\u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u0439 \u0434\u0430\u044e\u0442")
    localized = localized.replace(
        "so most volume sits in a small set.",
        "\u043e\u0441\u043d\u043e\u0432\u043d\u043e\u0439 \u0432\u0435\u0441 \u0441\u043e\u0441\u0440\u0435\u0434\u043e\u0442\u043e\u0447\u0435\u043d \u0432 \u043d\u0435\u0441\u043a\u043e\u043b\u044c\u043a\u0438\u0445 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u044f\u0445.",
    )
    localized = localized.replace("while the remaining", "\u0430 \u043e\u0441\u0442\u0430\u0432\u0448\u0438\u0435\u0441\u044f")
    localized = localized.replace("buckets share", "\u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u0439 \u0434\u0430\u044e\u0442")
    localized = localized.replace("with no single dominant bucket.", "\u0431\u0435\u0437 \u043e\u0434\u043d\u043e\u0433\u043e \u0434\u043e\u043c\u0438\u043d\u0438\u0440\u0443\u044e\u0449\u0435\u0433\u043e \u0437\u043d\u0430\u0447\u0435\u043d\u0438\u044f.")
    localized = localized.replace("with very small spread between top values.", "\u0441 \u043c\u0438\u043d\u0438\u043c\u0430\u043b\u044c\u043d\u044b\u043c \u0440\u0430\u0437\u0440\u044b\u0432\u043e\u043c \u043c\u0435\u0436\u0434\u0443 \u043b\u0438\u0434\u0435\u0440\u0430\u043c\u0438.")
    localized = localized.replace("so a direct bucket-to-bucket comparison is usually enough.", "\u043f\u043e\u044d\u0442\u043e\u043c\u0443 \u0434\u043e\u0441\u0442\u0430\u0442\u043e\u0447\u043d\u043e \u043f\u0440\u044f\u043c\u043e\u0433\u043e \u0441\u0440\u0430\u0432\u043d\u0435\u043d\u0438\u044f \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u0439.")
    localized = localized.replace("cover", "\u0434\u0430\u044e\u0442")
    localized = localized.replace("Trend:", "\u0422\u0440\u0435\u043d\u0434:")
    localized = localized.replace("increasing over time", "\u0440\u043e\u0441\u0442 \u043f\u043e \u043f\u0435\u0440\u0438\u043e\u0434\u0430\u043c")
    localized = localized.replace("decreasing over time", "\u0441\u043d\u0438\u0436\u0435\u043d\u0438\u0435 \u043f\u043e \u043f\u0435\u0440\u0438\u043e\u0434\u0430\u043c")
    localized = localized.replace("flat across time buckets.", "\u0431\u0435\u0437 \u0438\u0437\u043c\u0435\u043d\u0435\u043d\u0438\u0439 \u043f\u043e \u043f\u0435\u0440\u0438\u043e\u0434\u0430\u043c.")
    localized = localized.replace("no clear directional trend across time buckets.", "\u0431\u0435\u0437 \u044f\u0432\u043d\u043e\u0433\u043e \u043d\u0430\u043f\u0440\u0430\u0432\u043b\u0435\u043d\u0438\u044f \u043f\u043e \u043f\u0435\u0440\u0438\u043e\u0434\u0430\u043c.")
    localized = localized.replace("Key contrast:", "\u041a\u043b\u044e\u0447\u0435\u0432\u043e\u0439 \u043a\u043e\u043d\u0442\u0440\u0430\u0441\u0442:")
    localized = localized.replace("exceeds", "\u0432\u044b\u0448\u0435")
    localized = localized.replace(" points", " \u043f.\u043f.")
    localized = localized.replace("top ranks are close", "\u043b\u0438\u0434\u0435\u0440\u044b \u0431\u043b\u0438\u0437\u043a\u0438")
    localized = localized.replace("differ by", "\u043e\u0442\u043b\u0438\u0447\u0430\u044e\u0442\u0441\u044f \u043d\u0430")
    return localized


def extract_chart_highlights(*, result_text: str, max_items: int = 3, preferred_lang: str = "en") -> List[str]:
    buckets_in_row_order = _parse_chart_buckets(result_text)
    if not buckets_in_row_order:
        return [
            "\u0414\u0435\u0442\u0435\u0440\u043c\u0438\u043d\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u044b\u0439 \u0437\u0430\u043f\u0440\u043e\u0441 \u0434\u043b\u044f \u0433\u0440\u0430\u0444\u0438\u043a\u0430 \u043d\u0435 \u0432\u0435\u0440\u043d\u0443\u043b \u043d\u0435\u043f\u0443\u0441\u0442\u044b\u0445 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u0439."
            if normalize_preferred_response_language(preferred_lang) == "ru"
            else "No non-empty buckets were returned by the deterministic chart query."
        ]

    ranked = sorted(buckets_in_row_order, key=lambda item: float(item["value"]), reverse=True)
    if not ranked:
        return [
            "\u0414\u0435\u0442\u0435\u0440\u043c\u0438\u043d\u0438\u0440\u043e\u0432\u0430\u043d\u043d\u044b\u0439 \u0437\u0430\u043f\u0440\u043e\u0441 \u0434\u043b\u044f \u0433\u0440\u0430\u0444\u0438\u043a\u0430 \u043d\u0435 \u0432\u0435\u0440\u043d\u0443\u043b \u0447\u0438\u0441\u043b\u043e\u0432\u044b\u0445 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0438\u0439."
            if normalize_preferred_response_language(preferred_lang) == "ru"
            else "No numeric chart buckets were returned by the deterministic chart query."
        ]

    max_items = max(1, int(max_items))
    total_value = sum(float(item["value"]) for item in ranked)
    shape = _classify_distribution_shape(ranked_buckets=ranked, total_value=total_value)

    highlights: List[str] = []
    highlights.append(_build_top_bucket_highlight(ranked_buckets=ranked, total_value=total_value))

    shape_line = _build_shape_highlight(shape=shape, ranked_buckets=ranked, total_value=total_value)
    if shape_line:
        highlights.append(shape_line)

    trend_line = _summarize_time_trend(time_series_buckets=_resolve_time_series_order(buckets_in_row_order) or [])
    if trend_line:
        highlights.append(trend_line)
    else:
        contrast_line = _build_contrast_highlight(ranked_buckets=ranked, total_value=total_value, shape=shape)
        if contrast_line:
            highlights.append(contrast_line)

    compact = highlights[:max_items]
    return [_localize_highlight_line(line=item, preferred_lang=preferred_lang) for item in compact]

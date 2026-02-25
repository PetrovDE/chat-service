from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, Tuple
import re


@dataclass
class TimerStat:
    count: int = 0
    total_ms: float = 0.0
    max_ms: float = 0.0


_lock = Lock()
_counters: Dict[str, int] = {}
_timers: Dict[str, TimerStat] = {}


def _make_key(name: str, labels: Dict[str, Any]) -> str:
    if not labels:
        return name
    parts = [f"{k}={labels[k]}" for k in sorted(labels.keys())]
    return f"{name}|{'|'.join(parts)}"


def inc_counter(name: str, value: int = 1, **labels: Any) -> None:
    key = _make_key(name, labels)
    with _lock:
        _counters[key] = _counters.get(key, 0) + int(value)


def observe_ms(name: str, value_ms: float, **labels: Any) -> None:
    key = _make_key(name, labels)
    with _lock:
        stat = _timers.get(key)
        if stat is None:
            stat = TimerStat()
            _timers[key] = stat
        stat.count += 1
        stat.total_ms += float(value_ms)
        if float(value_ms) > stat.max_ms:
            stat.max_ms = float(value_ms)


def snapshot_metrics() -> Dict[str, Dict[str, Any]]:
    with _lock:
        counters = dict(_counters)
        timers = {
            k: {
                "count": v.count,
                "total_ms": round(v.total_ms, 3),
                "avg_ms": round((v.total_ms / v.count), 3) if v.count else 0.0,
                "max_ms": round(v.max_ms, 3),
            }
            for k, v in _timers.items()
        }
    return {"counters": counters, "timers": timers}


def _parse_metric_key(raw_key: str) -> Tuple[str, Dict[str, str]]:
    if "|" not in raw_key:
        return raw_key, {}

    parts = raw_key.split("|")
    name = parts[0]
    labels: Dict[str, str] = {}
    for item in parts[1:]:
        if "=" not in item:
            continue
        k, v = item.split("=", 1)
        labels[str(k)] = str(v)
    return name, labels


def _sanitize_metric_name(name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_:]", "_", name)
    if not sanitized or not re.match(r"^[a-zA-Z_:]", sanitized):
        sanitized = f"metric_{sanitized}"
    return sanitized


def _sanitize_label_name(name: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if not sanitized or not re.match(r"^[a-zA-Z_]", sanitized):
        sanitized = f"label_{sanitized}"
    return sanitized


def _escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"")


def _format_labels(labels: Dict[str, str]) -> str:
    if not labels:
        return ""
    parts = [f'{_sanitize_label_name(k)}="{_escape_label_value(v)}"' for k, v in sorted(labels.items())]
    return "{" + ",".join(parts) + "}"


def render_prometheus_metrics() -> str:
    snap = snapshot_metrics()
    counters = snap.get("counters", {})
    timers = snap.get("timers", {})

    lines = []

    grouped_counters: Dict[str, list] = {}
    for raw_key, value in counters.items():
        name, labels = _parse_metric_key(raw_key)
        metric_name = _sanitize_metric_name(name)
        grouped_counters.setdefault(metric_name, []).append((labels, int(value)))

    for metric_name in sorted(grouped_counters.keys()):
        lines.append(f"# HELP {metric_name} In-memory counter metric.")
        lines.append(f"# TYPE {metric_name} counter")
        for labels, value in sorted(grouped_counters[metric_name], key=lambda x: sorted(x[0].items())):
            lines.append(f"{metric_name}{_format_labels(labels)} {value}")

    grouped_timers: Dict[str, list] = {}
    for raw_key, value in timers.items():
        name, labels = _parse_metric_key(raw_key)
        metric_name = _sanitize_metric_name(name)
        grouped_timers.setdefault(metric_name, []).append((labels, value))

    for metric_name in sorted(grouped_timers.keys()):
        count_name = f"{metric_name}_count"
        sum_name = f"{metric_name}_sum"
        max_name = f"{metric_name}_max"
        avg_name = f"{metric_name}_avg"

        lines.append(f"# HELP {count_name} Total number of timer observations.")
        lines.append(f"# TYPE {count_name} counter")
        lines.append(f"# HELP {sum_name} Total observed duration in milliseconds.")
        lines.append(f"# TYPE {sum_name} counter")
        lines.append(f"# HELP {max_name} Maximum observed duration in milliseconds.")
        lines.append(f"# TYPE {max_name} gauge")
        lines.append(f"# HELP {avg_name} Average observed duration in milliseconds.")
        lines.append(f"# TYPE {avg_name} gauge")

        for labels, value in sorted(grouped_timers[metric_name], key=lambda x: sorted(x[0].items())):
            label_text = _format_labels(labels)
            lines.append(f"{count_name}{label_text} {int(value.get('count', 0))}")
            lines.append(f"{sum_name}{label_text} {float(value.get('total_ms', 0.0))}")
            lines.append(f"{max_name}{label_text} {float(value.get('max_ms', 0.0))}")
            lines.append(f"{avg_name}{label_text} {float(value.get('avg_ms', 0.0))}")

    if not lines:
        return ""

    return "\n".join(lines) + "\n"

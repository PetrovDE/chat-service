from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from uuid import uuid4

from app.core.config import settings
from app.services.chat.complex_analytics.artifacts import (
    artifact_public_url,
    artifact_relative_path,
    to_safe_filename,
)


def _coerce_chart_value(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        try:
            return float(str(value).replace(",", "."))
        except Exception:
            return 0.0


def _relative_upload_path(path_value: Optional[str]) -> Optional[Path]:
    raw = str(path_value or "").strip().replace("\\", "/")
    if not raw:
        return None
    if raw.startswith("uploads/"):
        raw = raw[len("uploads/") :]
    raw = raw.lstrip("/")
    if not raw:
        return None
    candidate = Path(raw)
    if candidate.is_absolute():
        return None
    return candidate


def _chart_artifact_is_available(
    *,
    chart_artifact_path: Optional[str],
    artifact: Optional[Dict[str, Any]],
) -> bool:
    if not isinstance(artifact, dict):
        return False
    artifact_url = str(artifact.get("url") or "").strip()
    if not artifact_url or not artifact_url.startswith("/uploads/"):
        return False
    relative = _relative_upload_path(chart_artifact_path or str(artifact.get("path") or ""))
    if relative is None:
        return False

    uploads_root = settings.get_public_uploads_dir().resolve()
    artifact_disk_path = (uploads_root / relative).resolve()
    try:
        artifact_disk_path.relative_to(uploads_root)
    except Exception:
        return False
    if not artifact_disk_path.exists() or not artifact_disk_path.is_file():
        return False
    if int(artifact_disk_path.stat().st_size or 0) <= 0:
        return False

    expected_url = artifact_public_url(str(artifact_disk_path))
    return bool(expected_url and expected_url == artifact_url)


def render_chart_artifact(
    *,
    rows: Sequence[Tuple[Any, ...]],
    chart_spec: Dict[str, Any],
) -> Dict[str, Any]:
    delivery: Dict[str, Any] = {
        "chart_rendered": False,
        "chart_artifact_available": False,
        "chart_artifact_exists": False,
        "chart_fallback_reason": "none",
        "chart_artifact_path": None,
        "chart_artifact_id": None,
        "artifact": None,
    }
    if not rows:
        delivery["chart_fallback_reason"] = "no_data_for_chart"
        return delivery

    try:
        import matplotlib  # noqa: PLC0415

        matplotlib.use("Agg", force=True)
        import matplotlib.pyplot as plt  # noqa: PLC0415
    except Exception:
        delivery["chart_fallback_reason"] = "renderer_unavailable"
        return delivery

    points: List[Tuple[str, float]] = []
    for row in rows:
        if not row:
            continue
        label = str(row[0] if len(row) > 0 else "").strip()
        if not label:
            continue
        points.append((label, _coerce_chart_value(row[1] if len(row) > 1 else 0)))
    if not points:
        delivery["chart_fallback_reason"] = "no_plot_points"
        return delivery

    artifact_id = uuid4().hex[:12]
    column_hint = str(
        chart_spec.get("matched_chart_field")
        or chart_spec.get("requested_dimension_column")
        or "dimension"
    )
    artifact_name = to_safe_filename(f"tabular_chart_{column_hint}_{artifact_id}.png")
    artifacts_dir = (settings.get_public_uploads_dir() / "tabular_sql" / uuid4().hex).resolve()
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = (artifacts_dir / artifact_name).resolve()

    fig = None
    render_succeeded = False
    try:
        labels = [item[0] for item in points]
        values = [item[1] for item in points]
        fig, ax = plt.subplots(figsize=(10.5, 4.8))
        ax.bar(range(len(labels)), values, color="#2B6CB0")
        ax.set_title(str(chart_spec.get("title") or "Distribution"), fontsize=12)
        ax.set_xlabel(str(chart_spec.get("x_title") or "bucket"))
        ax.set_ylabel(str(chart_spec.get("y_title") or "count"))
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=35, ha="right", fontsize=9)
        fig.tight_layout()
        fig.savefig(artifact_path, dpi=150, bbox_inches="tight")
        render_succeeded = True
    except Exception:
        delivery["chart_fallback_reason"] = "render_exception"
        return delivery
    finally:
        if fig is not None:
            plt.close(fig)

    delivery["chart_rendered"] = render_succeeded
    artifact_exists = artifact_path.exists() and artifact_path.is_file() and artifact_path.stat().st_size > 0
    if not artifact_exists:
        delivery["chart_fallback_reason"] = "artifact_missing_after_render"
        return delivery

    public_url = artifact_public_url(str(artifact_path))
    relative_path = artifact_relative_path(str(artifact_path))
    if not public_url or not relative_path:
        delivery["chart_fallback_reason"] = "artifact_not_public"
        return delivery

    delivery["chart_artifact_path"] = relative_path
    delivery["chart_artifact_id"] = artifact_id
    artifact: Dict[str, Any] = {
        "kind": "tabular_chart",
        "name": artifact_name,
        "path": relative_path,
        "url": public_url,
        "content_type": "image/png",
        "column": str(chart_spec.get("matched_chart_field") or chart_spec.get("requested_dimension_column") or ""),
    }
    delivery["artifact"] = artifact
    available = _chart_artifact_is_available(
        chart_artifact_path=relative_path,
        artifact=artifact,
    )
    if not available:
        delivery["chart_fallback_reason"] = "artifact_not_accessible"
        delivery["artifact"] = None
        return delivery

    delivery["chart_artifact_available"] = True
    delivery["chart_artifact_exists"] = True
    delivery["chart_fallback_reason"] = "none"
    return delivery

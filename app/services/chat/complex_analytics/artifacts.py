from __future__ import annotations

import re
import shutil
from pathlib import Path
from time import time
from typing import Any, Dict, Optional

from app.core.config import settings
from app.observability.metrics import inc_counter


def to_safe_filename(name: str) -> str:
    stem = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(name or "artifact")).strip("._")
    if not stem:
        stem = "artifact"
    if not stem.lower().endswith(".png"):
        stem = f"{stem}.png"
    return stem


def artifact_public_url(path_value: str) -> Optional[str]:
    if not path_value:
        return None
    try:
        artifact_path = Path(str(path_value)).expanduser().resolve()
        uploads_root = settings.get_public_uploads_dir().resolve()
        relative = artifact_path.relative_to(uploads_root)
        return "/uploads/" + "/".join(relative.parts)
    except Exception:
        return None


def artifact_relative_path(path_value: str) -> Optional[str]:
    if not path_value:
        return None
    try:
        artifact_path = Path(str(path_value)).expanduser().resolve()
        uploads_root = settings.get_public_uploads_dir().resolve()
        relative = artifact_path.relative_to(uploads_root)
        return "uploads/" + "/".join(relative.parts)
    except Exception:
        return None


def sanitize_artifact_for_response(raw_artifact: Dict[str, Any]) -> Dict[str, Any]:
    artifact = dict(raw_artifact or {})
    path_value = str(artifact.get("path") or "")
    if path_value:
        relative_path = artifact_relative_path(path_value)
        if relative_path:
            artifact["path"] = relative_path
        else:
            artifact.pop("path", None)
    url_value = str(artifact.get("url") or "")
    if not url_value and path_value:
        public_url = artifact_public_url(path_value)
        if public_url:
            artifact["url"] = public_url
    return artifact


def cleanup_complex_analytics_artifacts(*, artifacts_root: Path) -> Dict[str, int]:
    deleted = 0
    failed = 0
    ttl_hours = int(getattr(settings, "COMPLEX_ANALYTICS_ARTIFACT_TTL_HOURS", 168) or 168)
    max_run_dirs = int(getattr(settings, "COMPLEX_ANALYTICS_ARTIFACT_MAX_RUN_DIRS", 2000) or 2000)
    try:
        artifacts_root.mkdir(parents=True, exist_ok=True)
        now_ts = time()
        ttl_seconds = max(3600, ttl_hours * 3600)
        directories = [p for p in artifacts_root.iterdir() if p.is_dir()]
        directories.sort(key=lambda p: p.stat().st_mtime)

        stale = [p for p in directories if (now_ts - p.stat().st_mtime) > ttl_seconds]
        for path in stale:
            try:
                shutil.rmtree(path, ignore_errors=False)
                deleted += 1
            except Exception:
                failed += 1

        remaining = [p for p in artifacts_root.iterdir() if p.is_dir()]
        remaining.sort(key=lambda p: p.stat().st_mtime)
        overflow = max(0, len(remaining) - max_run_dirs)
        for path in remaining[:overflow]:
            try:
                shutil.rmtree(path, ignore_errors=False)
                deleted += 1
            except Exception:
                failed += 1
    except Exception:
        failed += 1

    if deleted:
        inc_counter("complex_analytics_artifacts_cleanup_total", value=deleted, status="deleted")
    if failed:
        inc_counter("complex_analytics_artifacts_cleanup_total", value=failed, status="failed")
    return {"deleted": deleted, "failed": failed}


# Compatibility aliases used by legacy tests and callers.
_to_safe_filename = to_safe_filename
_artifact_public_url = artifact_public_url
_artifact_relative_path = artifact_relative_path
_sanitize_artifact_for_response = sanitize_artifact_for_response
_cleanup_complex_analytics_artifacts = cleanup_complex_analytics_artifacts

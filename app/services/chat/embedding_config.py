from __future__ import annotations

from typing import Dict, List, Optional, Tuple


def normalize_source(source: Optional[str]) -> str:
    src = (source or "").strip().lower()
    if src == "corporate":
        return "aihub"
    if src in ("aihub", "openai", "ollama", "local"):
        return src
    return "local"


def parse_file_embedding_meta(raw_value: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    raw = (raw_value or "").strip()
    if not raw:
        return None, None

    if ":" in raw:
        mode_raw, model_raw = raw.split(":", 1)
        mode = normalize_source(mode_raw)
        model = model_raw.strip() or None
        if mode in ("local", "ollama", "aihub"):
            return ("local" if mode == "ollama" else mode), model

    return None, raw


def resolve_rag_embedding_config(files, requested_model_source: Optional[str]) -> Tuple[str, Optional[str]]:
    fallback_mode = "aihub" if normalize_source(requested_model_source) == "aihub" else "local"

    first_model_only: Optional[str] = None
    for file_obj in files:
        mode, model = parse_file_embedding_meta(getattr(file_obj, "embedding_model", None))
        if model and not first_model_only:
            first_model_only = model
        if mode:
            return mode, model

    return fallback_mode, first_model_only


def group_files_by_embedding_config(
    files,
    requested_model_source: Optional[str],
) -> Dict[Tuple[str, Optional[str]], List[str]]:
    fallback_mode = "aihub" if normalize_source(requested_model_source) == "aihub" else "local"
    groups: Dict[Tuple[str, Optional[str]], List[str]] = {}

    for file_obj in files:
        mode, model = parse_file_embedding_meta(getattr(file_obj, "embedding_model", None))
        resolved_mode = mode or fallback_mode
        key = (resolved_mode, model)
        groups.setdefault(key, []).append(str(file_obj.id))

    return groups

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List


def dataset_path(dataset_root: Path, dataset_name: str) -> Path:
    return dataset_root / f"{dataset_name}.jsonl"


def load_jsonl(path: Path) -> List[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Dataset file not found: {path}")

    rows: List[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL row at {path}:{line_no}") from exc
            if not isinstance(payload, dict):
                raise ValueError(f"Expected object row at {path}:{line_no}")
            rows.append(payload)
    return rows


def load_named_datasets(dataset_root: Path, dataset_names: Iterable[str]) -> Dict[str, List[dict]]:
    loaded: Dict[str, List[dict]] = {}
    for name in dataset_names:
        loaded[name] = load_jsonl(dataset_path(dataset_root, name))
    return loaded

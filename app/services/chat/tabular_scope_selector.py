from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from app.services.chat.tabular_schema_resolver import normalize_text
from app.services.tabular.sql_execution import ResolvedTabularDataset, ResolvedTabularTable


@dataclass(frozen=True)
class TabularScopeDecision:
    status: str
    target_file: Optional[Any]
    dataset: Optional[ResolvedTabularDataset]
    table: Optional[ResolvedTabularTable]
    clarification_options: List[str] = field(default_factory=list)
    debug_fields: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_selected(self) -> bool:
        return self.status == "selected" and self.target_file is not None and self.dataset is not None and self.table is not None


def _dedupe(values: Iterable[str], *, limit: int) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
        if len(out) >= max(1, int(limit)):
            break
    return out


def _text_overlap_score(*, query_norm: str, candidate_norm: str) -> float:
    if not query_norm or not candidate_norm:
        return 0.0
    if candidate_norm == query_norm:
        return 1.0
    if f" {candidate_norm} " in f" {query_norm} ":
        return 0.92
    query_tokens = set(query_norm.split())
    candidate_tokens = set(candidate_norm.split())
    if not query_tokens or not candidate_tokens:
        return 0.0
    overlap = query_tokens.intersection(candidate_tokens)
    if not overlap:
        return 0.0
    return float(len(overlap) / max(1, len(candidate_tokens)))


def _file_name_score(*, query_norm: str, file_obj: Any) -> Tuple[float, List[str]]:
    reasons: List[str] = []
    best = 0.0
    names = [
        str(getattr(file_obj, "original_filename", "") or "").strip(),
        str(getattr(file_obj, "stored_filename", "") or "").strip(),
    ]
    custom_metadata = getattr(file_obj, "custom_metadata", None)
    if isinstance(custom_metadata, dict):
        names.extend(
            [
                str(custom_metadata.get("display_name") or "").strip(),
                str(custom_metadata.get("filename") or "").strip(),
            ]
        )
    for raw_name in names:
        normalized = normalize_text(raw_name)
        if not normalized:
            continue
        score = _text_overlap_score(query_norm=query_norm, candidate_norm=normalized)
        if score > best:
            best = score
        if score >= 0.9:
            reasons.append(f"filename_match:{raw_name}")
    return best, _dedupe(reasons, limit=3)


def _table_surface_score(*, query_norm: str, table: ResolvedTabularTable) -> Tuple[float, List[str]]:
    reasons: List[str] = []
    table_score = _text_overlap_score(query_norm=query_norm, candidate_norm=normalize_text(table.table_name))
    sheet_score = _text_overlap_score(query_norm=query_norm, candidate_norm=normalize_text(table.sheet_name))
    score = max(table_score, sheet_score)
    if table_score >= 0.9:
        reasons.append(f"table_name_match:{table.table_name}")
    if sheet_score >= 0.9 and str(table.sheet_name or "").strip():
        reasons.append(f"sheet_name_match:{table.sheet_name}")
    return score, _dedupe(reasons, limit=3)


def _column_signal_score(*, query_norm: str, table: ResolvedTabularTable) -> Tuple[float, List[str]]:
    reasons: List[str] = []
    hits: List[str] = []
    max_hits = 4
    for column in list(table.columns or []):
        column_name = str(column or "").strip()
        if not column_name:
            continue
        if _text_overlap_score(query_norm=query_norm, candidate_norm=normalize_text(column_name)) >= 0.9:
            hits.append(column_name)
            if len(hits) >= max_hits:
                break
    if hits:
        reasons.append(f"column_match:{','.join(hits)}")
    score = min(1.0, 0.22 * len(hits))
    return score, reasons


def _score_table(*, query_norm: str, table: ResolvedTabularTable, max_rows: int) -> Tuple[float, List[str]]:
    surface_score, surface_reasons = _table_surface_score(query_norm=query_norm, table=table)
    column_score, column_reasons = _column_signal_score(query_norm=query_norm, table=table)
    row_bonus = 0.0
    row_count = int(table.row_count or 0)
    if max_rows > 0 and row_count > 0:
        row_bonus = min(0.16, float(row_count / max_rows) * 0.16)
    score = (surface_score * 1.4) + column_score + row_bonus
    reasons = _dedupe([*surface_reasons, *column_reasons], limit=6)
    if row_bonus > 0:
        reasons.append(f"row_count_bonus={round(row_bonus, 4)}")
    return score, reasons


def _score_file(*, query_norm: str, file_obj: Any, dataset: ResolvedTabularDataset, max_rows: int) -> Tuple[float, List[str]]:
    file_score, file_reasons = _file_name_score(query_norm=query_norm, file_obj=file_obj)
    table_scores: List[float] = []
    table_reasons: List[str] = []
    for table in list(dataset.tables or []):
        score, reasons = _score_table(query_norm=query_norm, table=table, max_rows=max_rows)
        table_scores.append(score)
        table_reasons.extend(reasons[:2])
    best_table_score = max(table_scores) if table_scores else 0.0
    score = (file_score * 2.2) + best_table_score
    reasons = _dedupe([*file_reasons, *table_reasons], limit=8)
    return score, reasons


def _format_file_option(*, file_obj: Any, dataset: ResolvedTabularDataset) -> str:
    file_name = str(getattr(file_obj, "original_filename", "") or getattr(file_obj, "stored_filename", "") or "unknown")
    sheet_names = _dedupe([str(getattr(item, "sheet_name", "") or "").strip() for item in list(dataset.tables or [])], limit=3)
    if sheet_names:
        return f"{file_name} (sheets: {', '.join(sheet_names)})"
    return file_name


def _format_table_option(*, table: ResolvedTabularTable) -> str:
    sheet = str(getattr(table, "sheet_name", "") or "").strip()
    table_name = str(getattr(table, "table_name", "") or "table").strip()
    rows = int(getattr(table, "row_count", 0) or 0)
    if sheet:
        return f"{sheet} (table={table_name}, rows={rows})"
    return f"{table_name} (rows={rows})"


def _is_ambiguous(*, top_score: float, second_score: Optional[float], margin: float, signal_floor: float) -> bool:
    if second_score is None:
        return False
    if top_score <= signal_floor and second_score <= signal_floor:
        return True
    return (top_score - second_score) < margin


def _select_table(
    *,
    query_norm: str,
    dataset: ResolvedTabularDataset,
    margin: float,
) -> Tuple[Optional[ResolvedTabularTable], str, List[str], Dict[str, Any]]:
    tables = list(dataset.tables or [])
    if not tables:
        return None, "no_tables", [], {"table_scope_candidates": []}
    if len(tables) == 1:
        only = tables[0]
        debug = {
            "table_scope_candidates": [
                {
                    "table_name": only.table_name,
                    "sheet_name": only.sheet_name,
                    "score": 1.0,
                    "reasons": ["single_table_in_dataset"],
                }
            ]
        }
        return only, "selected", [], debug

    max_rows = max([int(getattr(item, "row_count", 0) or 0) for item in tables] or [0])
    scored: List[Tuple[ResolvedTabularTable, float, List[str]]] = []
    for table in tables:
        score, reasons = _score_table(query_norm=query_norm, table=table, max_rows=max_rows)
        scored.append((table, score, reasons))
    scored.sort(
        key=lambda item: (
            -float(item[1]),
            str(getattr(item[0], "sheet_name", "") or "").lower(),
            str(getattr(item[0], "table_name", "") or "").lower(),
        )
    )
    top_table, top_score, _top_reasons = scored[0]
    second_score = scored[1][1] if len(scored) > 1 else None
    debug_candidates = [
        {
            "table_name": table.table_name,
            "sheet_name": table.sheet_name,
            "score": round(float(score), 6),
            "reasons": list(reasons),
        }
        for table, score, reasons in scored[:6]
    ]
    debug = {"table_scope_candidates": debug_candidates}
    if _is_ambiguous(
        top_score=float(top_score),
        second_score=float(second_score) if second_score is not None else None,
        margin=margin,
        signal_floor=0.08,
    ):
        options = [_format_table_option(table=item[0]) for item in scored[:4]]
        return None, "ambiguous_table", options, debug
    return top_table, "selected", [], debug


def select_tabular_scope(
    *,
    query: str,
    files: Sequence[Any],
    resolve_dataset_fn,
    score_margin: float = 0.14,
) -> TabularScopeDecision:
    query_norm = normalize_text(query)
    datasets: List[Tuple[Any, ResolvedTabularDataset]] = []
    for file_obj in list(files or []):
        dataset = resolve_dataset_fn(file_obj)
        if dataset is None or not getattr(dataset, "tables", None):
            continue
        datasets.append((file_obj, dataset))

    if not datasets:
        return TabularScopeDecision(
            status="no_tabular_dataset",
            target_file=None,
            dataset=None,
            table=None,
            clarification_options=[],
            debug_fields={
                "scope_selection_status": "no_tabular_dataset",
                "scope_file_candidates": [],
            },
        )

    if len(datasets) == 1:
        file_obj, dataset = datasets[0]
        table, table_status, table_options, table_debug = _select_table(
            query_norm=query_norm,
            dataset=dataset,
            margin=float(score_margin),
        )
        base_debug = {
            "scope_selection_status": table_status if table_status != "selected" else "selected",
            "scope_file_candidates": [
                {
                    "file_id": str(getattr(file_obj, "id", "") or ""),
                    "file_name": str(getattr(file_obj, "original_filename", "") or getattr(file_obj, "stored_filename", "") or ""),
                    "score": 1.0,
                    "reasons": ["single_tabular_file_in_scope"],
                }
            ],
            **table_debug,
        }
        if table is None:
            return TabularScopeDecision(
                status="ambiguous_table",
                target_file=file_obj,
                dataset=dataset,
                table=None,
                clarification_options=table_options,
                debug_fields=base_debug,
            )
        base_debug.update(
            {
                "scope_selected_file_id": str(getattr(file_obj, "id", "") or ""),
                "scope_selected_file_name": str(
                    getattr(file_obj, "original_filename", "") or getattr(file_obj, "stored_filename", "") or ""
                ),
                "scope_selected_table_name": str(getattr(table, "table_name", "") or ""),
                "scope_selected_sheet_name": str(getattr(table, "sheet_name", "") or ""),
            }
        )
        return TabularScopeDecision(
            status="selected",
            target_file=file_obj,
            dataset=dataset,
            table=table,
            clarification_options=[],
            debug_fields=base_debug,
        )

    max_rows = 0
    for _file_obj, dataset in datasets:
        for table in list(dataset.tables or []):
            max_rows = max(max_rows, int(getattr(table, "row_count", 0) or 0))

    scored_files: List[Tuple[Any, ResolvedTabularDataset, float, List[str]]] = []
    for file_obj, dataset in datasets:
        score, reasons = _score_file(query_norm=query_norm, file_obj=file_obj, dataset=dataset, max_rows=max_rows)
        scored_files.append((file_obj, dataset, score, reasons))
    scored_files.sort(
        key=lambda item: (
            -float(item[2]),
            str(getattr(item[0], "original_filename", "") or getattr(item[0], "stored_filename", "")).lower(),
        )
    )
    top_file, top_dataset, top_score, _top_reasons = scored_files[0]
    second_score = scored_files[1][2] if len(scored_files) > 1 else None

    file_candidates_debug = [
        {
            "file_id": str(getattr(file_obj, "id", "") or ""),
            "file_name": str(getattr(file_obj, "original_filename", "") or getattr(file_obj, "stored_filename", "") or ""),
            "score": round(float(score), 6),
            "reasons": list(reasons),
        }
        for file_obj, _dataset, score, reasons in scored_files[:6]
    ]
    if _is_ambiguous(
        top_score=float(top_score),
        second_score=float(second_score) if second_score is not None else None,
        margin=float(score_margin),
        signal_floor=0.1,
    ):
        options = [_format_file_option(file_obj=item[0], dataset=item[1]) for item in scored_files[:4]]
        return TabularScopeDecision(
            status="ambiguous_file",
            target_file=None,
            dataset=None,
            table=None,
            clarification_options=options,
            debug_fields={
                "scope_selection_status": "ambiguous_file",
                "scope_file_candidates": file_candidates_debug,
            },
        )

    selected_table, table_status, table_options, table_debug = _select_table(
        query_norm=query_norm,
        dataset=top_dataset,
        margin=float(score_margin),
    )
    if selected_table is None:
        return TabularScopeDecision(
            status="ambiguous_table",
            target_file=top_file,
            dataset=top_dataset,
            table=None,
            clarification_options=table_options,
            debug_fields={
                "scope_selection_status": "ambiguous_table",
                "scope_file_candidates": file_candidates_debug,
                "scope_selected_file_id": str(getattr(top_file, "id", "") or ""),
                "scope_selected_file_name": str(
                    getattr(top_file, "original_filename", "") or getattr(top_file, "stored_filename", "") or ""
                ),
                **table_debug,
            },
        )

    return TabularScopeDecision(
        status="selected",
        target_file=top_file,
        dataset=top_dataset,
        table=selected_table,
        clarification_options=[],
        debug_fields={
            "scope_selection_status": "selected",
            "scope_file_candidates": file_candidates_debug,
            "scope_selected_file_id": str(getattr(top_file, "id", "") or ""),
            "scope_selected_file_name": str(
                getattr(top_file, "original_filename", "") or getattr(top_file, "stored_filename", "") or ""
            ),
            "scope_selected_table_name": str(getattr(selected_table, "table_name", "") or ""),
            "scope_selected_sheet_name": str(getattr(selected_table, "sheet_name", "") or ""),
            **table_debug,
        },
    )

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from app.core.config import settings
from app.services.llm.manager import llm_manager

from .full_file_analysis_helpers import (
    _build_direct_full_file_prompt,
    _build_structured_partial,
    _count_rows,
    _extract_doc_row_range,
    _merge_grouped_row_ranges,
    _merge_ranges,
    _merge_structured_partials,
    _normalize_row_ranges_payload,
    _normalize_string_list,
    _parse_json_object,
    _row_ranges_debug,
    _to_int,
    batch_context_docs,
)
from .full_file_analysis_runtime import build_full_file_map_reduce_prompt_runtime

logger = logging.getLogger(__name__)


async def build_full_file_map_reduce_prompt(
    *,
    query: str,
    context_documents: List[Dict[str, Any]],
    preferred_lang: str,
    model_source: Optional[str],
    model_name: Optional[str],
    prompt_max_chars: Optional[int] = None,
) -> Tuple[str, Dict[str, Any]]:
    return await build_full_file_map_reduce_prompt_runtime(
        query=query,
        context_documents=context_documents,
        preferred_lang=preferred_lang,
        model_source=model_source,
        model_name=model_name,
        settings_obj=settings,
        llm_client=llm_manager,
        logger_obj=logger,
        prompt_max_chars=prompt_max_chars,
    )

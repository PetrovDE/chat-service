from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from app.services.chat.language import answer_matches_expected_language
from app.services.llm.manager import llm_manager

logger = logging.getLogger(__name__)


async def enforce_answer_language(
    *,
    answer: str,
    preferred_lang: str,
    model_source: Optional[str],
    provider_mode: Optional[str],
    model_name: Optional[str],
    prompt_max_chars: Optional[int],
) -> Tuple[str, Dict[str, Any]]:
    if not answer.strip():
        return answer, {"enabled": True, "applied": False, "reason": "empty_answer"}
    if answer_matches_expected_language(answer, preferred_lang):
        return answer, {"enabled": True, "applied": False, "reason": "already_expected_language"}

    target = "Russian" if preferred_lang == "ru" else "English"
    rewrite_prompt = (
        f"Rewrite the answer strictly in {target}.\n"
        "Do not change facts, numbers, entities, caveats or structure.\n"
        "Preserve markdown lists and section headings semantics.\n"
        "Return only rewritten answer text.\n\n"
        f"Answer:\n{answer}\n\n"
        "Rewritten answer:"
    )
    try:
        rewritten = await llm_manager.generate_response(
            prompt=rewrite_prompt,
            model_source=model_source,
            provider_mode=provider_mode,
            model_name=model_name,
            temperature=0.0,
            max_tokens=2200,
            conversation_history=None,
            prompt_max_chars=prompt_max_chars,
        )
    except Exception:
        logger.warning("Answer language rewrite failed", exc_info=True)
        return answer, {"enabled": True, "applied": False, "reason": "rewrite_call_failed"}

    new_text = (rewritten.get("response") or "").strip()
    if not new_text:
        return answer, {"enabled": True, "applied": False, "reason": "empty_rewrite"}

    applied = answer_matches_expected_language(new_text, preferred_lang)
    return (
        new_text if applied else answer,
        {
            "enabled": True,
            "applied": bool(applied),
            "reason": "rewritten" if applied else "rewrite_still_wrong_language",
        },
    )


def build_rag_caveats(
    *,
    files: List[Any],
    context_documents: List[Dict[str, Any]],
    rag_debug: Optional[Dict[str, Any]],
) -> List[str]:
    caveats: List[str] = []
    partial_files = []
    for file_obj in files:
        status = str(getattr(file_obj, "is_processed", "") or "")
        if status == "partial_success":
            progress = {}
            custom_meta = getattr(file_obj, "custom_metadata", None)
            if isinstance(custom_meta, dict):
                progress = (
                    custom_meta.get("ingestion_progress")
                    if isinstance(custom_meta.get("ingestion_progress"), dict)
                    else {}
                )
            expected = int(progress.get("total_chunks_expected", 0) or 0)
            failed = int(progress.get("chunks_failed", 0) or 0)
            partial_files.append(f"{getattr(file_obj, 'original_filename', 'unknown')} (bad={failed}, expected={expected})")

    if partial_files:
        caveats.append("Some files were indexed partially: " + "; ".join(partial_files[:5]))
    if not context_documents:
        caveats.append("No relevant chunks were retrieved for this query.")
    coverage = rag_debug.get("coverage") if isinstance(rag_debug, dict) and isinstance(rag_debug.get("coverage"), dict) else {}
    if coverage:
        expected = int(coverage.get("expected_chunks", 0) or 0)
        retrieved = int(coverage.get("retrieved_chunks", 0) or 0)
        complete = bool(coverage.get("complete", False))
        if expected > 0 and not complete:
            caveats.append(
                f"Full-file coverage is incomplete: retrieved {retrieved}/{expected} chunks."
            )
    if isinstance(rag_debug, dict):
        rows_expected = int(rag_debug.get("rows_expected_total", 0) or 0)
        rows_used_reduce = int(rag_debug.get("rows_used_reduce_total", 0) or 0)
        if rows_expected > 0 and rows_used_reduce < rows_expected:
            caveats.append(
                f"Row-level coverage is incomplete: used {rows_used_reduce}/{rows_expected} rows."
            )
        if bool(rag_debug.get("silent_row_loss_detected", False)):
            caveats.append("Potential silent row loss detected: chunk coverage looks high but row coverage is low.")
    if isinstance(rag_debug, dict) and rag_debug.get("truncated"):
        caveats.append("Context was truncated by retrieval limits; answer may be incomplete.")
    return caveats


def append_caveats_and_sources(
    answer: str,
    caveats: List[str],
    sources: List[str],
    *,
    preferred_lang: str = "ru",
) -> str:
    limitations_title = "### Ограничения/нехватка данных"
    sources_title = "### Источники (кратко)"
    no_limitations = "- Существенных ограничений контекста не обнаружено."
    no_sources = "- Релевантные источники не найдены."
    if preferred_lang == "en":
        limitations_title = "### Limitations/Missing Data"
        sources_title = "### Sources (short)"
        no_limitations = "- No major context limitations were detected."
        no_sources = "- No relevant sources were found."

    lines = [answer.strip()]
    lines.append(f"\n\n{limitations_title}")
    if caveats:
        for caveat in caveats:
            lines.append(f"- {caveat}")
    else:
        lines.append(no_limitations)
    lines.append(f"\n{sources_title}")
    if sources:
        for source in sources:
            lines.append(f"- {source}")
    else:
        lines.append(no_sources)
    return "\n".join(lines).strip()


def build_critic_context(context_documents: List[Dict[str, Any]], max_chars: int = 12000) -> str:
    parts: List[str] = []
    used = 0
    for i, doc in enumerate(context_documents, start=1):
        meta = doc.get("metadata") or {}
        filename = meta.get("filename") or "unknown"
        chunk_index = meta.get("chunk_index", "?")
        content = (doc.get("content") or "").strip()
        if not content:
            continue
        block = f"[{i}] file={filename} chunk={chunk_index}\n{content}\n"
        if used + len(block) > max_chars:
            remain = max_chars - used
            if remain <= 0:
                break
            block = block[:remain]
        parts.append(block)
        used += len(block)
        if used >= max_chars:
            break
    return "\n---\n".join(parts)


def extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    raw = (text or "").strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None

    candidate = raw[start:end + 1]
    try:
        return json.loads(candidate)
    except Exception:
        return None


async def run_answer_critic(
    *,
    query: str,
    answer: str,
    context_documents: List[Dict[str, Any]],
    model_source: Optional[str],
    provider_mode: Optional[str],
    model_name: Optional[str],
) -> Tuple[str, Dict[str, Any]]:
    context_text = build_critic_context(context_documents, max_chars=12000)
    if not context_text:
        return answer, {"enabled": True, "applied": False, "reason": "empty_context"}

    critic_prompt = (
        "You are an answer quality critic for RAG.\n"
        "Given user question, draft answer, and evidence context, evaluate factual support.\n"
        "Return STRICT JSON object with fields:\n"
        "supported: boolean,\n"
        "issues: array of short strings,\n"
        "missing_points: array of short strings,\n"
        "refined_answer: string,\n"
        "confidence: number (0..1).\n"
        "Do not return markdown.\n\n"
        f"Question:\n{query}\n\n"
        f"Draft answer:\n{answer}\n\n"
        f"Evidence context:\n{context_text}\n\n"
        "JSON:"
    )

    try:
        critic = await llm_manager.generate_response(
            prompt=critic_prompt,
            model_source=model_source,
            provider_mode=provider_mode,
            model_name=model_name,
            temperature=0.0,
            max_tokens=1200,
            conversation_history=None,
        )
    except Exception as exc:
        logger.warning("Critic step failed: %s", exc)
        return answer, {"enabled": True, "applied": False, "reason": "critic_call_failed"}

    parsed = extract_json_object(critic.get("response", ""))
    if not parsed:
        return answer, {"enabled": True, "applied": False, "reason": "critic_parse_failed"}

    supported = bool(parsed.get("supported", True))
    refined = (parsed.get("refined_answer") or "").strip()
    confidence = parsed.get("confidence")
    issues = parsed.get("issues") if isinstance(parsed.get("issues"), list) else []
    missing = parsed.get("missing_points") if isinstance(parsed.get("missing_points"), list) else []

    apply_refine = (not supported and bool(refined)) or (bool(refined) and refined != answer and len(refined) > 20)
    final_answer = refined if apply_refine else answer

    return final_answer, {
        "enabled": True,
        "applied": bool(apply_refine),
        "supported": supported,
        "confidence": confidence,
        "issues_count": len(issues),
        "missing_points_count": len(missing),
    }

# app/api/v1/endpoints/models.py
from typing import Any, Dict, List
import logging

import httpx
from fastapi import APIRouter, Query

from app.core.config import settings
from app.schemas import ModelsListResponse, ModelsStatusResponse
from app.services.llm.manager import llm_manager
from app.services.llm.model_resolver import CAP_CHAT, CAP_EMBEDDING, infer_model_capability

logger = logging.getLogger(__name__)
router = APIRouter()
HTTP_TIMEOUT_MODELS = httpx.Timeout(10.0, connect=3.0)
AIHUB_PREFERRED_DEFAULTS = {
    CAP_CHAT: "gpt-oss",
    CAP_EMBEDDING: "qwen3-emb",
}


def _normalize_mode(mode: str) -> str:
    return llm_manager.provider_registry.normalize_source(mode)


def _normalize_capability(capability: str) -> str:
    value = str(capability or CAP_CHAT).strip().lower()
    if value in {CAP_CHAT, CAP_EMBEDDING}:
        return value
    return CAP_CHAT


def _parse_catalog(raw_catalog: str) -> List[str]:
    out: List[str] = []
    for item in str(raw_catalog or "").split(","):
        model = str(item or "").strip()
        if model and model not in out:
            out.append(model)
    return out


def _preferred_default_for_mode_capability(mode: str, capability: str) -> str | None:
    if mode != "aihub":
        return None
    preferred = AIHUB_PREFERRED_DEFAULTS.get(capability)
    value = str(preferred or "").strip()
    return value or None


def _catalog_for_mode_capability(mode: str, capability: str) -> List[str]:
    if mode == "aihub":
        return _parse_catalog(
            settings.AIHUB_CHAT_MODEL_CATALOG if capability == CAP_CHAT else settings.AIHUB_EMBED_MODEL_CATALOG
        )
    if mode == "ollama":
        return _parse_catalog(
            settings.OLLAMA_CHAT_MODEL_CATALOG if capability == CAP_CHAT else settings.OLLAMA_EMBED_MODEL_CATALOG
        )
    if mode == "openai":
        if capability == CAP_CHAT and settings.OPENAI_MODEL:
            return [settings.OPENAI_MODEL]
        if capability == CAP_EMBEDDING and settings.OPENAI_EMBEDDING_MODEL:
            return [settings.OPENAI_EMBEDDING_MODEL]
    return []


def _filter_provider_models_by_capability(
    *,
    mode: str,
    capability: str,
    provider_models: List[str],
) -> List[str]:
    if mode == "aihub":
        out: List[str] = []
        for candidate in provider_models:
            model = str(candidate or "").strip()
            if model and model not in out:
                out.append(model)
        return out

    catalog = set(_catalog_for_mode_capability(mode, capability))
    filtered: List[str] = []
    for candidate in provider_models:
        model = str(candidate or "").strip()
        if not model:
            continue
        inferred = infer_model_capability(model)
        if capability == CAP_CHAT:
            if inferred == CAP_EMBEDDING:
                continue
            if model not in filtered:
                filtered.append(model)
            continue

        # Embedding selector should not include unknown chat-like tags unless
        # explicitly configured in provider embedding catalog.
        if inferred == CAP_EMBEDDING or model in catalog:
            if model not in filtered:
                filtered.append(model)
    return filtered


def _model_rows(
    *,
    names: List[str],
    capability: str,
    default_model: str | None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen = set()
    for name in names:
        model = str(name or "").strip()
        if not model or model in seen:
            continue
        inferred = infer_model_capability(model)
        if capability == CAP_CHAT and inferred == CAP_EMBEDDING:
            continue
        if capability == CAP_EMBEDDING and inferred == CAP_CHAT:
            continue
        seen.add(model)
        rows.append(
            {
                "name": model,
                "size": 0,
                "capability": inferred,
                "is_default": bool(model == default_model),
            }
        )

    return rows


def _aihub_type_matches(capability: str, model_type: Any) -> bool:
    raw = str(model_type or "").strip().lower()
    if capability == CAP_EMBEDDING:
        return raw == "embedding"
    if capability == CAP_CHAT:
        return raw in {"chatbot", "chat"}
    return False


@router.get("/list", response_model=ModelsListResponse)
async def list_models(
    mode: str = Query("local"),
    capability: str = Query(CAP_CHAT),
) -> Dict[str, Any]:
    """List available provider models with capability-aware defaults."""
    selected_mode = _normalize_mode(mode)
    selected_capability = _normalize_capability(capability)
    try:
        if selected_capability == CAP_CHAT:
            decision = llm_manager.provider_registry.resolve_chat_model_decision(selected_mode, None)
        else:
            decision = llm_manager.provider_registry.resolve_embedding_model_decision(selected_mode, None)

        provider_models: List[str] = []
        if selected_mode == "aihub":
            detailed = await llm_manager.get_available_models_detailed(
                source=selected_mode,
                capability=selected_capability,
            )
            for row in detailed:
                if not isinstance(row, dict):
                    continue
                name = str(row.get("name") or "").strip()
                if not name:
                    continue
                # Trust AI HUB model type as source of truth for capabilities.
                if selected_capability and not _aihub_type_matches(selected_capability, row.get("type")):
                    continue
                if name not in provider_models:
                    provider_models.append(name)
        else:
            provider_models = await llm_manager.get_available_models(
                source=selected_mode,
                capability=selected_capability,
            )

        # Visible selectors must use provider-discovered availability only.
        candidates = _filter_provider_models_by_capability(
            mode=selected_mode,
            capability=selected_capability,
            provider_models=provider_models,
        )

        merged_names: List[str] = []
        for name in candidates:
            normalized = str(name or "").strip()
            if normalized and normalized not in merged_names:
                merged_names.append(normalized)

        preferred_default_model = _preferred_default_for_mode_capability(
            mode=selected_mode,
            capability=selected_capability,
        )
        available_preferred_default = (
            preferred_default_model
            if preferred_default_model in merged_names
            else None
        )
        available_provider_default = (
            decision.resolved_model
            if decision.resolved_model in merged_names
            else None
        )
        available_default_model = available_preferred_default or available_provider_default
        models = _model_rows(
            names=merged_names,
            capability=selected_capability,
            default_model=available_default_model,
        )
        logger.info(
            (
                "Models listed: mode=%s capability=%s provider=%s default_model=%s "
                "preferred_default=%s resolved_default=%s resolution_source=%s "
                "preferred_available=%s default_available=%s count=%d"
            ),
            mode,
            selected_capability,
            selected_mode,
            available_default_model,
            preferred_default_model,
            decision.resolved_model,
            decision.source,
            bool(available_preferred_default),
            bool(available_default_model),
            len(models),
        )
        return {
            "mode": selected_mode,
            "capability": selected_capability,
            "default_model": available_default_model,
            "models": models,
            "count": len(models),
        }

    except Exception as e:
        logger.error(
            "Error getting models mode=%s capability=%s: %s",
            mode,
            selected_capability,
            e,
            exc_info=True,
        )
        return {
            "mode": selected_mode,
            "capability": selected_capability,
            "default_model": None,
            "models": [],
            "count": 0,
            "error": str(e),
        }


@router.get("/status", response_model=ModelsStatusResponse)
async def models_status() -> Dict[str, Any]:
    """Status of model provider availability."""
    ollama_available = False
    aihub_available = False
    openai_available = False

    try:
        ollama_url = str(settings.EMBEDDINGS_BASEURL).rstrip("/")
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_MODELS) as client:
            response = await client.get(f"{ollama_url}/api/tags")
            ollama_available = response.status_code == 200
    except Exception:
        pass

    try:
        aihub_url = getattr(settings, "AIHUB_URL", None)
        aihub_available = bool(aihub_url)
    except Exception:
        pass

    try:
        openai_api_key = getattr(settings, "OPENAI_API_KEY", None)
        openai_available = bool(openai_api_key)
    except Exception:
        pass

    return {
        "ollama": {
            "available": ollama_available,
            "url": str(settings.EMBEDDINGS_BASEURL),
        },
        "aihub": {
            "available": aihub_available,
            "url": getattr(settings, "AIHUB_URL", None),
        },
        "openai": {
            "available": openai_available,
            "url": None,
        },
    }

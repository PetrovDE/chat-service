from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Optional, Sequence

from app.core.config import settings


CAP_CHAT = "chat"
CAP_EMBEDDING = "embedding"

_EMBED_TOKENS = (
    "embed",
    "embedding",
    "nomic",
    "bge",
    "e5",
    "gte",
    "mxbai",
    "text-embedding",
)
_CHAT_TOKENS = (
    "llama",
    "mistral",
    "gemma",
    "vikhr",
    "gpt",
    "claude",
    "deepseek",
    "qwen",
    "phi",
    "yi",
    "mixtral",
)
_PROVIDER_PREFIX_RE = re.compile(r"^(?P<provider>[a-zA-Z0-9_-]+):(?P<model>.+)$")
_KNOWN_PROVIDER_PREFIXES = {"local", "ollama", "aihub", "corporate", "openai"}


@dataclass(frozen=True)
class ModelResolution:
    provider: str
    capability: str
    requested_model: Optional[str]
    resolved_model: str
    source: str
    reason: str


def normalize_provider(source: Optional[str]) -> str:
    src = str(source or "").strip().lower()
    if not src:
        src = str(settings.DEFAULT_MODEL_SOURCE or "aihub").strip().lower()
    if src == "corporate":
        return "aihub"
    if src in {"local", "ollama"}:
        return "ollama"
    return src


def split_model_prefix(model_name: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    raw = str(model_name or "").strip()
    if not raw:
        return None, None
    m = _PROVIDER_PREFIX_RE.match(raw)
    if not m:
        return None, raw
    provider_raw = str(m.group("provider") or "").strip().lower()
    # Treat "<provider>:<model>" as prefixed only for known provider aliases.
    # This avoids mis-parsing model tags like "nomic-embed-text:latest".
    if provider_raw not in _KNOWN_PROVIDER_PREFIXES:
        return None, raw
    provider = normalize_provider(provider_raw)
    model = str(m.group("model") or "").strip() or None
    return provider, model


def infer_model_capability(model_name: Optional[str]) -> str:
    model = str(model_name or "").strip().lower()
    if not model:
        return "unknown"

    has_embed_marker = any(token in model for token in _EMBED_TOKENS)
    has_chat_marker = any(token in model for token in _CHAT_TOKENS)
    if has_embed_marker and has_chat_marker:
        if "-emb" in model or "embed" in model:
            return CAP_EMBEDDING
        return CAP_CHAT
    if has_embed_marker:
        return CAP_EMBEDDING
    if has_chat_marker:
        return CAP_CHAT
    return "unknown"


def _parse_catalog(raw_catalog: str) -> set[str]:
    out: set[str] = set()
    for item in str(raw_catalog or "").split(","):
        model = str(item or "").strip()
        if model:
            out.add(model)
    return out


class ProviderModelResolver:
    def __init__(self):
        self._policy = str(settings.MODEL_INVALID_OVERRIDE_POLICY or "fallback_default").strip().lower()

    @staticmethod
    def _provider_defaults(provider: str) -> dict[str, Optional[str]]:
        if provider == "aihub":
            return {
                CAP_CHAT: settings.AIHUB_DEFAULT_MODEL,
                CAP_EMBEDDING: settings.AIHUB_EMBEDDING_MODEL,
            }
        if provider == "openai":
            return {
                CAP_CHAT: settings.OPENAI_MODEL,
                CAP_EMBEDDING: settings.OPENAI_EMBEDDING_MODEL,
            }
        # local/ollama
        return {
            CAP_CHAT: settings.OLLAMA_CHAT_MODEL,
            CAP_EMBEDDING: settings.OLLAMA_EMBED_MODEL or settings.EMBEDDINGS_MODEL,
        }

    @staticmethod
    def _provider_catalog(provider: str, capability: str) -> set[str]:
        if provider == "aihub":
            if capability == CAP_EMBEDDING:
                return _parse_catalog(settings.AIHUB_EMBED_MODEL_CATALOG)
            return _parse_catalog(settings.AIHUB_CHAT_MODEL_CATALOG)
        if provider == "openai":
            if capability == CAP_EMBEDDING:
                return {settings.OPENAI_EMBEDDING_MODEL} if settings.OPENAI_EMBEDDING_MODEL else set()
            return {settings.OPENAI_MODEL} if settings.OPENAI_MODEL else set()
        if capability == CAP_EMBEDDING:
            return _parse_catalog(settings.OLLAMA_EMBED_MODEL_CATALOG)
        return _parse_catalog(settings.OLLAMA_CHAT_MODEL_CATALOG)

    def _default_model_for(self, provider: str, capability: str) -> str:
        defaults = self._provider_defaults(provider)
        model = str(defaults.get(capability) or "").strip()
        if not model:
            raise ValueError(f"Default {capability} model is not configured for provider={provider}")
        return model

    def _is_capability_supported(self, *, model_name: str, capability: str, provider: str) -> tuple[bool, str]:
        inferred = infer_model_capability(model_name)
        catalog = self._provider_catalog(provider, capability)
        if catalog and model_name in catalog:
            return True, "catalog_match"
        if capability == CAP_EMBEDDING and inferred == CAP_CHAT:
            return False, "chat_model_for_embedding"
        if capability == CAP_CHAT and inferred == CAP_EMBEDDING:
            return False, "embedding_model_for_chat"
        if inferred in {capability, "unknown"}:
            return True, "inferred_ok"
        return False, "capability_mismatch"

    def _resolve_with_override(
        self,
        *,
        provider: str,
        capability: str,
        requested_model: str,
    ) -> ModelResolution:
        requested_provider, requested_plain = split_model_prefix(requested_model)
        if requested_provider and requested_provider != provider:
            raise ValueError(
                f"{capability} override provider mismatch: requested_provider={requested_provider} selected_provider={provider}"
            )
        if not requested_plain:
            raise ValueError(f"Empty {capability} override")

        supported, reason = self._is_capability_supported(
            model_name=requested_plain,
            capability=capability,
            provider=provider,
        )
        if supported:
            return ModelResolution(
                provider=provider,
                capability=capability,
                requested_model=requested_model,
                resolved_model=requested_plain,
                source="override",
                reason=reason,
            )

        if self._policy == "error":
            raise ValueError(
                f"Invalid {capability} override for provider={provider}: model={requested_plain} reason={reason}"
            )

        return ModelResolution(
            provider=provider,
            capability=capability,
            requested_model=requested_model,
            resolved_model=self._default_model_for(provider, capability),
            source="provider_default",
            reason=f"invalid_override:{reason}",
        )

    def resolve(self, *, provider: str, capability: str, requested_model: Optional[str]) -> ModelResolution:
        normalized_provider = normalize_provider(provider)
        cap = str(capability or "").strip().lower()
        if cap not in {CAP_CHAT, CAP_EMBEDDING}:
            raise ValueError(f"Unsupported capability: {capability}")

        requested = str(requested_model or "").strip()
        if requested:
            return self._resolve_with_override(
                provider=normalized_provider,
                capability=cap,
                requested_model=requested,
            )

        return ModelResolution(
            provider=normalized_provider,
            capability=cap,
            requested_model=None,
            resolved_model=self._default_model_for(normalized_provider, cap),
            source="provider_default",
            reason="no_override",
        )

    def resolve_chat(self, provider: str, requested_model: Optional[str]) -> ModelResolution:
        return self.resolve(provider=provider, capability=CAP_CHAT, requested_model=requested_model)

    def resolve_embedding(self, provider: str, requested_model: Optional[str]) -> ModelResolution:
        return self.resolve(provider=provider, capability=CAP_EMBEDDING, requested_model=requested_model)

    def pick_first_embedding_candidate(
        self,
        *,
        provider: str,
        available_models: Sequence[str],
        preferred: Optional[str],
    ) -> Optional[str]:
        normalized = normalize_provider(provider)
        preferred_model = str(preferred or "").strip()
        available = [str(x or "").strip() for x in available_models if str(x or "").strip()]
        if not available:
            return None

        if preferred_model and preferred_model in available:
            return preferred_model

        catalog = self._provider_catalog(normalized, CAP_EMBEDDING)
        for model in available:
            if model in catalog:
                return model

        for model in available:
            if infer_model_capability(model) == CAP_EMBEDDING:
                return model
        return None

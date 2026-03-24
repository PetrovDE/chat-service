from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Mapping, Optional
from uuid import UUID

from app.observability.context import request_id_ctx


_EVENT_REQUIRED_FIELDS: Dict[str, tuple[str, ...]] = {
    "file_uploaded": ("user_id", "file_id", "upload_id", "storage_key", "status"),
    "file_attached_to_chat": ("user_id", "file_id", "chat_id", "status"),
    "processing_created": (
        "user_id",
        "file_id",
        "processing_id",
        "pipeline_version",
        "embedding_provider",
        "embedding_model",
        "status",
    ),
    "extraction_started": ("user_id", "file_id", "processing_id", "status"),
    "extraction_completed": ("user_id", "file_id", "processing_id", "status"),
    "chunking_completed": ("user_id", "file_id", "processing_id", "status"),
    "embedding_started": (
        "user_id",
        "file_id",
        "processing_id",
        "embedding_provider",
        "embedding_model",
        "status",
    ),
    "embedding_completed": (
        "user_id",
        "file_id",
        "processing_id",
        "embedding_provider",
        "embedding_model",
        "status",
    ),
    "indexing_completed": ("user_id", "file_id", "processing_id", "status"),
    "file_ready": ("user_id", "file_id", "processing_id", "status"),
    "processing_failed": ("user_id", "file_id", "processing_id", "status"),
}


def _to_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, UUID):
        return str(value)
    raw = str(value).strip()
    return raw or None


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _to_id_list(values: Optional[Iterable[Any]]) -> List[str]:
    if not values:
        return []
    out: List[str] = []
    for item in values:
        normalized = _to_id(item)
        if normalized:
            out.append(normalized)
    return out


def _missing_required(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    if isinstance(value, list) and not value:
        return True
    return False


def _build_payload(
    *,
    event: str,
    rid: Optional[str],
    user_id: Optional[Any],
    chat_id: Optional[Any],
    conversation_id: Optional[Any],
    chat_ids: Optional[Iterable[Any]],
    conversation_ids: Optional[Iterable[Any]],
    file_id: Optional[Any],
    filename: Optional[str],
    upload_id: Optional[str],
    processing_id: Optional[Any],
    document_ids: Optional[Iterable[Any]],
    pipeline_version: Optional[str],
    parser_version: Optional[str],
    artifact_version: Optional[str],
    chunking_strategy: Optional[str],
    retrieval_profile: Optional[str],
    processing_stage: Optional[str],
    status: Optional[str],
    storage_key: Optional[str],
    quota_used_bytes: Optional[int],
    quota_limit_bytes: Optional[int],
    is_active_processing: Optional[bool],
    embedding_provider: Optional[str],
    embedding_model: Optional[str],
    embedding_dimension_expected: Optional[int],
    embedding_dimension_actual: Optional[int],
    embedding_dimension_source: Optional[str],
    collection: Optional[str],
    namespace: Optional[str],
    error: Optional[str],
    error_code: Optional[str],
    extras: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    normalized_chat_id = _to_id(chat_id)
    normalized_conversation_id = _to_id(conversation_id) or normalized_chat_id
    normalized_chat_ids = _to_id_list(chat_ids)
    normalized_conversation_ids = _to_id_list(conversation_ids)

    if not normalized_conversation_ids and normalized_chat_ids:
        normalized_conversation_ids = list(normalized_chat_ids)
    if not normalized_chat_ids and normalized_conversation_ids:
        normalized_chat_ids = list(normalized_conversation_ids)

    expected_dim = _to_int(embedding_dimension_expected)
    actual_dim = _to_int(embedding_dimension_actual)

    payload: Dict[str, Any] = {
        "event": str(event or "").strip(),
        "lifecycle_schema_version": 2,
        "rid": _to_id(rid),
        "user_id": _to_id(user_id),
        "uid": _to_id(user_id),
        "chat_id": normalized_chat_id,
        "conversation_id": normalized_conversation_id,
        "chat_ids": normalized_chat_ids,
        "conversation_ids": normalized_conversation_ids,
        "file_id": _to_id(file_id),
        "filename": str(filename).strip() if filename is not None else None,
        "upload_id": _to_id(upload_id),
        "processing_id": _to_id(processing_id),
        "document_ids": _to_id_list(document_ids),
        "pipeline_version": str(pipeline_version).strip() if pipeline_version is not None else None,
        "parser_version": str(parser_version).strip() if parser_version is not None else None,
        "artifact_version": str(artifact_version).strip() if artifact_version is not None else None,
        "chunking_strategy": str(chunking_strategy).strip() if chunking_strategy is not None else None,
        "retrieval_profile": str(retrieval_profile).strip() if retrieval_profile is not None else None,
        "processing_stage": str(processing_stage).strip() if processing_stage is not None else None,
        "status": str(status).strip() if status is not None else None,
        "storage_key": str(storage_key).strip() if storage_key is not None else None,
        "quota_used_bytes": _to_int(quota_used_bytes),
        "quota_limit_bytes": _to_int(quota_limit_bytes),
        "is_active_processing": bool(is_active_processing) if is_active_processing is not None else None,
        "embedding_provider": str(embedding_provider).strip() if embedding_provider is not None else None,
        "embedding_model": str(embedding_model).strip() if embedding_model is not None else None,
        "embedding_dimension_expected": expected_dim,
        "embedding_dimension_actual": actual_dim,
        "embedding_dimension": actual_dim if actual_dim is not None else expected_dim,
        "embedding_dimension_source": (
            str(embedding_dimension_source).strip() if embedding_dimension_source is not None else None
        ),
        "collection": str(collection).strip() if collection is not None else None,
        "namespace": str(namespace).strip() if namespace is not None else None,
        "error": str(error).strip() if error is not None else None,
        "error_code": str(error_code).strip() if error_code is not None else None,
    }
    if extras:
        for key, value in extras.items():
            if key in payload:
                continue
            payload[str(key)] = value
    return payload


def log_file_lifecycle_event(
    logger_obj,
    event: str,
    *,
    rid: Optional[str] = None,
    user_id: Optional[Any] = None,
    chat_id: Optional[Any] = None,
    conversation_id: Optional[Any] = None,
    chat_ids: Optional[Iterable[Any]] = None,
    conversation_ids: Optional[Iterable[Any]] = None,
    file_id: Optional[Any] = None,
    filename: Optional[str] = None,
    upload_id: Optional[str] = None,
    processing_id: Optional[Any] = None,
    document_ids: Optional[Iterable[Any]] = None,
    pipeline_version: Optional[str] = None,
    parser_version: Optional[str] = None,
    artifact_version: Optional[str] = None,
    chunking_strategy: Optional[str] = None,
    retrieval_profile: Optional[str] = None,
    processing_stage: Optional[str] = None,
    status: Optional[str] = None,
    storage_key: Optional[str] = None,
    quota_used_bytes: Optional[int] = None,
    quota_limit_bytes: Optional[int] = None,
    is_active_processing: Optional[bool] = None,
    embedding_provider: Optional[str] = None,
    embedding_model: Optional[str] = None,
    embedding_dimension_expected: Optional[int] = None,
    embedding_dimension_actual: Optional[int] = None,
    embedding_dimension_source: Optional[str] = None,
    collection: Optional[str] = None,
    namespace: Optional[str] = None,
    error: Optional[str] = None,
    error_code: Optional[str] = None,
    extras: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    payload = _build_payload(
        event=event,
        rid=rid if rid is not None else request_id_ctx.get(),
        user_id=user_id,
        chat_id=chat_id,
        conversation_id=conversation_id,
        chat_ids=chat_ids,
        conversation_ids=conversation_ids,
        file_id=file_id,
        filename=filename,
        upload_id=upload_id,
        processing_id=processing_id,
        document_ids=document_ids,
        pipeline_version=pipeline_version,
        parser_version=parser_version,
        artifact_version=artifact_version,
        chunking_strategy=chunking_strategy,
        retrieval_profile=retrieval_profile,
        processing_stage=processing_stage,
        status=status,
        storage_key=storage_key,
        quota_used_bytes=quota_used_bytes,
        quota_limit_bytes=quota_limit_bytes,
        is_active_processing=is_active_processing,
        embedding_provider=embedding_provider,
        embedding_model=embedding_model,
        embedding_dimension_expected=embedding_dimension_expected,
        embedding_dimension_actual=embedding_dimension_actual,
        embedding_dimension_source=embedding_dimension_source,
        collection=collection,
        namespace=namespace,
        error=error,
        error_code=error_code,
        extras=extras,
    )

    required_fields = _EVENT_REQUIRED_FIELDS.get(payload.get("event") or "", ())
    missing = [field for field in required_fields if _missing_required(payload.get(field))]
    if missing:
        logger_obj.warning(
            "file_lifecycle_contract_violation event=%s missing=%s payload=%s",
            payload.get("event"),
            ",".join(missing),
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
        )

    logger_obj.info("file_lifecycle %s", json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return payload

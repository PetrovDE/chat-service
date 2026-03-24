from __future__ import annotations

import asyncio
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from uuid import UUID

from langchain_core.documents import Document

from app.observability.file_lifecycle import log_file_lifecycle_event
from app.services.llm.manager import llm_manager


def _provider_source_for_embedding_mode(mode: str) -> str:
    normalized = str(mode or "local").strip().lower()
    if normalized == "corporate":
        normalized = "aihub"
    if normalized == "ollama":
        normalized = "local"
    if normalized == "openai":
        return "openai"
    if normalized == "aihub":
        return "aihub"
    return "ollama"


def classify_ingestion_exception(exc: Exception) -> Dict[str, Any]:
    if getattr(exc, "retryable", None) is not None:
        code_parts = []
        provider = getattr(exc, "provider", None)
        if provider:
            code_parts.append(str(provider))
        code_parts.append(type(exc).__name__.lower())
        status_code = getattr(exc, "status_code", None)
        if status_code:
            code_parts.append(str(status_code))
        code = "_".join(code_parts) if code_parts else type(exc).__name__.lower()
        fatal = bool(not bool(getattr(exc, "retryable", False)))
        return {"code": code, "retryable": bool(getattr(exc, "retryable", False)), "fatal": fatal}

    status_code = None
    response = getattr(exc, "response", None)
    if response is not None:
        status_code = int(getattr(response, "status_code", 0) or 0)
    elif getattr(exc, "status_code", None) is not None:
        try:
            status_code = int(getattr(exc, "status_code"))
        except Exception:
            status_code = None

    message = str(exc or "").lower()
    if status_code in {401, 403}:
        return {"code": f"http_{status_code}", "retryable": False, "fatal": True}
    if status_code in {400, 404, 422}:
        return {"code": f"http_{status_code}", "retryable": False, "fatal": True}
    if status_code in {408, 425, 429} or (status_code is not None and 500 <= status_code <= 599):
        return {"code": f"http_{status_code}", "retryable": True, "fatal": False}

    auth_markers = [
        "unauthorized",
        "forbidden",
        "invalid api key",
        "authentication",
        "access token",
        "keycloak",
        "auth failed",
    ]
    if any(marker in message for marker in auth_markers):
        return {"code": "auth_error", "retryable": False, "fatal": True}

    transient_markers = [
        "timeout",
        "timed out",
        "connect",
        "connection",
        "network",
        "temporarily unavailable",
        "remote protocol",
    ]
    if any(marker in message for marker in transient_markers):
        return {"code": "transient_error", "retryable": True, "fatal": False}

    return {"code": "unknown_error", "retryable": False, "fatal": False}


async def finalize_ingestion_pipeline(
    *,
    db: Any,
    file_id: UUID,
    processing_id: Optional[UUID] = None,
    progress: Dict[str, Any],
    embedding_mode: str,
    embedding_model: str,
    error_message: Optional[str],
    extra_metadata: Optional[Dict[str, Any]],
    safe_ratio_fn: Callable[[int, int], float],
    utc_now_iso_fn: Callable[[], str],
    bad_chunk_ratio_threshold: float,
    update_processing_status_fn,
    inc_counter_fn,
    logger_obj,
) -> str:
    expected = int(progress.get("total_chunks_expected", 0) or 0)
    processed = int(progress.get("chunks_processed", 0) or 0)
    failed = int(progress.get("chunks_failed", 0) or 0)
    indexed = int(progress.get("chunks_indexed", 0) or 0)
    vector_upserts_actual = int(progress.get("vector_upserts_actual", indexed) or 0)
    fatal_error = bool(progress.get("fatal_error", False))
    parsing_ok = bool(progress.get("parsing_ok", False))
    chunking_ok = bool(progress.get("chunking_ok", False))

    if processed > expected:
        logger_obj.warning(
            "Finalize normalized exceeded counters: file_id=%s expected_before=%d processed=%d",
            file_id,
            expected,
            processed,
        )
        expected = processed
        progress["total_chunks_expected"] = expected

    if expected > 0 and processed < expected:
        remainder = expected - processed
        failed += remainder
        processed = expected
        progress["chunks_failed"] = failed
        progress["chunks_processed"] = processed
        logger_obj.warning(
            "Finalize fixed incomplete counters: file_id=%s expected=%d processed_before=%d added_failed=%d",
            file_id,
            expected,
            processed - remainder,
            remainder,
        )

    bad_ratio = safe_ratio_fn(failed, expected)
    threshold = float(bad_chunk_ratio_threshold)

    counts_consistent = bool(processed == (indexed + failed))
    upserts_consistent = bool(vector_upserts_actual == indexed)
    expected_consistent = bool(expected > 0 and processed == expected and indexed <= expected)
    all_required_done = bool(
        parsing_ok
        and chunking_ok
        and expected_consistent
        and counts_consistent
        and upserts_consistent
        and failed <= 0
        and indexed > 0
        and not fatal_error
    )

    if all_required_done:
        final_status = "completed"
    elif indexed <= 0:
        final_status = "failed"
    elif fatal_error:
        final_status = "failed"
    elif failed > 0 and bad_ratio > threshold:
        final_status = "failed"
    elif failed > 0 or not counts_consistent or not upserts_consistent or indexed < expected:
        final_status = "partial_failed"
    else:
        final_status = "failed"

    progress["status"] = final_status
    progress["bad_ratio"] = bad_ratio
    progress["finished_at"] = utc_now_iso_fn()
    progress["stage"] = final_status

    metadata_patch: Dict[str, Any] = {"ingestion_progress": progress}
    if error_message:
        metadata_patch["error"] = error_message
    if extra_metadata:
        metadata_patch.update(extra_metadata)

    await update_processing_status_fn(
        db,
        file_id=file_id,
        processing_id=processing_id,
        status=final_status,
        chunks_count=indexed,
        embedding_model=f"{embedding_mode}:{embedding_model}",
        metadata_patch=metadata_patch,
    )

    inc_counter_fn("ingestion_chunks_total", value=expected)
    inc_counter_fn("ingestion_chunks_ok", value=indexed)
    inc_counter_fn("ingestion_chunks_bad", value=failed)
    inc_counter_fn("ingestion_upserts_ok", value=indexed)
    inc_counter_fn("ingestion_upserts_fail", value=failed)
    inc_counter_fn("ingestion_finalize_total", status=final_status)
    logger_obj.info(
        "Ingestion finalized: file_id=%s status=%s expected=%d processed=%d indexed=%d failed=%d bad_ratio=%.4f threshold=%.4f fatal=%s",
        file_id,
        final_status,
        expected,
        processed,
        indexed,
        failed,
        bad_ratio,
        threshold,
        fatal_error,
    )
    return final_status


async def process_file_pipeline(
    *,
    file_id: UUID,
    processing_id: Optional[UUID] = None,
    file_path: Path,
    embedding_mode: str,
    embedding_model: str,
    pipeline_version: Optional[str] = None,
    parser_version: Optional[str] = None,
    artifact_version: Optional[str] = None,
    chunking_strategy: Optional[str] = None,
    retrieval_profile: Optional[str] = None,
    async_session_factory,
    crud_file_module,
    conversation_file_model,
    select_fn,
    document_loader_obj,
    text_splitter_obj,
    vector_store_obj,
    embeddings_manager_cls,
    build_tabular_dataset_metadata_fn,
    finalize_ingestion_fn,
    batch_fn,
    utc_now_iso_fn,
    extract_xlsx_stats_fn,
    persist_derived_artifacts_fn,
    file_id_ctx_var,
    settings_obj,
    observe_ms_fn,
    logger_obj,
) -> Tuple[bool, bool]:
    file_ctx_token = file_id_ctx_var.set(str(file_id))
    started_ms = asyncio.get_running_loop().time()
    progress: Dict[str, Any] = {
        "status": "queued",
        "stage": "queued",
        "total_chunks_expected": 0,
        "chunks_processed": 0,
        "chunks_failed": 0,
        "chunks_indexed": 0,
        "vector_upserts_expected": 0,
        "vector_upserts_actual": 0,
        "embedding_batches_total": 0,
        "embedding_batches_failed": 0,
        "parsing_ok": False,
        "chunking_ok": False,
        "fatal_error": False,
        "failure_code": None,
        "chunk_types_count": {},
        "checkpoint": {"next_batch_index": 1, "batch_size": 0},
        "started_at": utc_now_iso_fn(),
        "finished_at": None,
        "bad_ratio": 0.0,
        "pipeline_version": str(pipeline_version or ""),
        "parser_version": str(parser_version or ""),
        "artifact_version": str(artifact_version or ""),
        "chunking_strategy": str(chunking_strategy or ""),
        "retrieval_profile": str(retrieval_profile or ""),
        "processing_id": str(processing_id) if processing_id is not None else None,
    }
    error_message: Optional[str] = None
    extra_metadata: Dict[str, Any] = {}
    failure_retryable = False
    observed_embedding_dimension: Optional[int] = None
    observed_collection: Optional[str] = None
    lifecycle_user_id: Optional[str] = None
    lifecycle_filename: Optional[str] = None
    lifecycle_upload_id: Optional[str] = None
    lifecycle_storage_key: Optional[str] = None
    lifecycle_chat_ids: List[str] = []
    document_ids_sample: List[str] = []

    async with async_session_factory() as db:
        try:
            async def _checkpoint(*, status: str, stage: str) -> None:
                progress["status"] = status
                progress["stage"] = stage
                metadata_patch: Dict[str, Any] = {
                    "ingestion_progress": progress,
                    "error": None,
                    "pipeline_version": str(pipeline_version or ""),
                    "parser_version": str(parser_version or ""),
                    "artifact_version": str(artifact_version or ""),
                    "chunking_strategy": str(chunking_strategy or ""),
                    "retrieval_profile": str(retrieval_profile or ""),
                }
                if observed_embedding_dimension is not None:
                    metadata_patch["embedding_dimension"] = int(observed_embedding_dimension)
                await crud_file_module.update_processing_status(
                    db,
                    file_id=file_id,
                    processing_id=processing_id,
                    status=status,
                    metadata_patch=metadata_patch,
                )

            logger_obj.info(
                "Starting file processing: file_id=%s mode=%s embedding_model=%s path=%s",
                file_id, embedding_mode, embedding_model, file_path
            )
            await _checkpoint(status="parsing", stage="parsing")

            q = select_fn(conversation_file_model.chat_id).where(conversation_file_model.file_id == file_id)
            r = await db.execute(q)
            chat_ids = r.scalars().all()
            lifecycle_chat_ids = [str(chat_id_value) for chat_id_value in chat_ids]
            chat_id = lifecycle_chat_ids[0] if lifecycle_chat_ids else None

            file_record = await crud_file_module.get(db, id=file_id)
            if not file_record:
                raise ValueError(f"File record not found: {file_id}")
            lifecycle_user_id = str(getattr(file_record, "user_id", "") or "")
            lifecycle_filename = str(getattr(file_record, "original_filename", "") or "")
            lifecycle_storage_key = str(getattr(file_record, "storage_key", "") or "")
            raw_file_custom_meta = getattr(file_record, "custom_metadata", None)
            file_custom_meta = raw_file_custom_meta if isinstance(raw_file_custom_meta, dict) else {}
            upload_id_raw = file_custom_meta.get("upload_id")
            lifecycle_upload_id = str(upload_id_raw).strip() if upload_id_raw is not None else None
            if lifecycle_upload_id == "":
                lifecycle_upload_id = None

            log_file_lifecycle_event(
                logger_obj,
                "extraction_started",
                user_id=lifecycle_user_id,
                chat_id=chat_id,
                conversation_id=chat_id,
                chat_ids=lifecycle_chat_ids,
                conversation_ids=lifecycle_chat_ids,
                file_id=file_id,
                filename=lifecycle_filename,
                upload_id=lifecycle_upload_id,
                processing_id=processing_id,
                pipeline_version=(str(pipeline_version or "") or None),
                parser_version=(str(parser_version or "") or None),
                artifact_version=(str(artifact_version or "") or None),
                chunking_strategy=(str(chunking_strategy or "") or None),
                retrieval_profile=(str(retrieval_profile or "") or None),
                processing_stage="parsing",
                status="parsing",
                storage_key=lifecycle_storage_key,
                embedding_provider=embedding_mode,
                embedding_model=embedding_model,
            )

            stage_t0 = asyncio.get_running_loop().time()
            docs = await document_loader_obj.load_file(str(file_path))
            if not docs:
                raise ValueError("No documents loaded from file")

            total_chars = sum(len(d.page_content or "") for d in docs)
            xlsx_stats = extract_xlsx_stats_fn(docs) if file_path.suffix.lower() in (".xlsx", ".xls") else {}
            chunk_type_counter = Counter()
            for d in docs:
                metadata = d.metadata if isinstance(getattr(d, "metadata", None), dict) else {}
                chunk_type_counter[str(metadata.get("chunk_type") or "unknown")] += 1
            progress["chunk_types_count"] = dict(chunk_type_counter)
            logger_obj.info(
                "Loaded documents=%d extracted_text_chars=%d file_id=%s chunk_types=%s %s",
                len(docs),
                total_chars,
                file_id,
                dict(chunk_type_counter),
                " ".join([f"{k}={v}" for k, v in xlsx_stats.items()]) if xlsx_stats else "",
            )
            observe_ms_fn("ingestion_stage_ms", (asyncio.get_running_loop().time() - stage_t0) * 1000.0, stage="extract")
            if total_chars < 50:
                raise ValueError("Extracted text is empty/too small (possible scanned PDF).")
            progress["parsing_ok"] = True
            await _checkpoint(status="parsed", stage="parsed")
            log_file_lifecycle_event(
                logger_obj,
                "extraction_completed",
                user_id=lifecycle_user_id,
                chat_id=chat_id,
                conversation_id=chat_id,
                chat_ids=lifecycle_chat_ids,
                conversation_ids=lifecycle_chat_ids,
                file_id=file_id,
                filename=lifecycle_filename,
                upload_id=lifecycle_upload_id,
                processing_id=processing_id,
                pipeline_version=(str(pipeline_version or "") or None),
                parser_version=(str(parser_version or "") or None),
                artifact_version=(str(artifact_version or "") or None),
                chunking_strategy=(str(chunking_strategy or "") or None),
                retrieval_profile=(str(retrieval_profile or "") or None),
                processing_stage="parsed",
                status="parsed",
                storage_key=lifecycle_storage_key,
                embedding_provider=embedding_mode,
                embedding_model=embedding_model,
                extras={
                    "documents_loaded": len(docs),
                    "extracted_text_chars": total_chars,
                    "chunk_types_count": dict(chunk_type_counter),
                },
            )
            active_processing = None
            try:
                active_processing = await crud_file_module.get_active_processing(
                    db,
                    file_id=file_id,
                    user_id=file_record.user_id,
                )
            except Exception:
                logger_obj.warning(
                    "Could not resolve active processing profile: file_id=%s",
                    file_id,
                    exc_info=True,
                )
            is_active_processing = bool(
                active_processing is not None
                and processing_id is not None
                and str(active_processing.id) == str(processing_id)
            )

            file_ext = file_path.suffix.lower().lstrip(".")
            try:
                derived = await asyncio.to_thread(
                    persist_derived_artifacts_fn,
                    file_id=file_id,
                    processing_id=processing_id,
                    file_path=file_path,
                    docs=docs,
                    pipeline_version=pipeline_version,
                    parser_version=parser_version,
                    artifact_version=artifact_version,
                    owner_user_id=file_record.user_id,
                )
                extra_metadata["derived_artifacts"] = dict(derived.summary)
                logger_obj.info(
                    "Derived artifacts persisted: file_id=%s processing_id=%s manifest=%s artifacts=%d",
                    file_id,
                    processing_id,
                    derived.summary.get("manifest_path"),
                    int(derived.summary.get("total_artifacts", 0) or 0),
                )
            except Exception:
                logger_obj.warning("Derived artifacts persistence failed for file_id=%s", file_id, exc_info=True)
                extra_metadata["derived_artifacts_error"] = "persist_failed"
            if file_ext in {"xlsx", "xls", "csv", "tsv"}:
                try:
                    tabular_dataset = await asyncio.to_thread(
                        build_tabular_dataset_metadata_fn,
                        file_id=file_id,
                        file_path=file_path,
                        file_type=file_ext,
                        source_filename=file_record.original_filename,
                    )
                    if tabular_dataset:
                        extra_metadata["tabular_dataset"] = tabular_dataset
                        logger_obj.info(
                            "Tabular dataset generated: file_id=%s dataset_version=%s tables=%d engine=%s",
                            file_id,
                            tabular_dataset.get("dataset_version"),
                            len(tabular_dataset.get("tables", [])),
                            tabular_dataset.get("engine"),
                        )
                except Exception:
                    logger_obj.warning("Tabular dataset generation failed for file_id=%s", file_id, exc_info=True)
                    extra_metadata["tabular_dataset_error"] = "generation_failed"

            stage_t0 = asyncio.get_running_loop().time()
            await _checkpoint(status="chunking", stage="chunking")
            if file_ext in ("xlsx", "xls", "csv", "tsv"):
                chunks = docs
            else:
                chunks = text_splitter_obj.split_documents(docs)
                # Keep a compact file-level summary chunk for selective indexing in narrative docs.
                summary_lines: List[str] = []
                for d in docs[:4]:
                    content = str(getattr(d, "page_content", "") or "").strip()
                    if content:
                        summary_lines.append(content[:420])
                summary_text = "\n\n".join(summary_lines)
                if summary_text:
                    summary_meta: Dict[str, Any] = {
                        "source": str(file_path),
                        "file_type": file_ext,
                        "source_type": "document",
                        "artifact_type": "document_summary",
                        "chunk_type": "document_summary",
                    }
                    chunks.insert(0, Document(page_content=summary_text, metadata=summary_meta))
            if not chunks:
                raise ValueError("No chunks created from documents")
            progress["total_chunks_expected"] = len(chunks)
            progress["chunking_ok"] = True
            logger_obj.info(
                "Created chunks_expected=%d chunk_size=%d overlap=%d file_id=%s",
                len(chunks),
                settings_obj.CHUNK_SIZE,
                settings_obj.CHUNK_OVERLAP,
                file_id,
            )
            log_file_lifecycle_event(
                logger_obj,
                "chunking_completed",
                user_id=lifecycle_user_id,
                chat_id=chat_id,
                conversation_id=chat_id,
                chat_ids=lifecycle_chat_ids,
                conversation_ids=lifecycle_chat_ids,
                file_id=file_id,
                filename=lifecycle_filename,
                upload_id=lifecycle_upload_id,
                processing_id=processing_id,
                pipeline_version=(str(pipeline_version or "") or None),
                parser_version=(str(parser_version or "") or None),
                artifact_version=(str(artifact_version or "") or None),
                chunking_strategy=(str(chunking_strategy or "") or None),
                retrieval_profile=(str(retrieval_profile or "") or None),
                processing_stage="chunking",
                status="chunked",
                storage_key=lifecycle_storage_key,
                embedding_provider=embedding_mode,
                embedding_model=embedding_model,
                extras={
                    "chunks_expected": len(chunks),
                    "chunk_size": int(settings_obj.CHUNK_SIZE),
                    "chunk_overlap": int(settings_obj.CHUNK_OVERLAP),
                },
            )
            observe_ms_fn("ingestion_stage_ms", (asyncio.get_running_loop().time() - stage_t0) * 1000.0, stage="chunk")

            await _checkpoint(status="embedding", stage="embedding")
            emb = embeddings_manager_cls(mode=embedding_mode, model=embedding_model)
            logger_obj.info("Embedding mode=%s resolved_model=%s", embedding_mode, embedding_model)
            provider_source = _provider_source_for_embedding_mode(embedding_mode)
            dim_decision = llm_manager.provider_registry.resolve_embedding_dimension_decision(
                provider_source,
                embedding_model,
            )
            expected_embedding_dimension = (
                int(dim_decision.dimension) if int(dim_decision.dimension or 0) > 0 else None
            )
            expected_dim_source = f"{dim_decision.source}:{dim_decision.reason}"
            namespace = str(getattr(settings_obj, "COLLECTION_NAME", "documents") or "documents")
            expected_collection = None
            if expected_embedding_dimension is not None:
                try:
                    expected_collection = vector_store_obj.resolve_collection_name(
                        embedding=[0.0] * expected_embedding_dimension,
                        metadata={
                            "embedding_mode": embedding_mode,
                            "embedding_model": embedding_model,
                        },
                    )
                except Exception:
                    expected_collection = None

            log_file_lifecycle_event(
                logger_obj,
                "embedding_started",
                user_id=lifecycle_user_id,
                chat_id=chat_id,
                conversation_id=chat_id,
                chat_ids=lifecycle_chat_ids,
                conversation_ids=lifecycle_chat_ids,
                file_id=file_id,
                filename=lifecycle_filename,
                upload_id=lifecycle_upload_id,
                processing_id=processing_id,
                pipeline_version=(str(pipeline_version or "") or None),
                parser_version=(str(parser_version or "") or None),
                artifact_version=(str(artifact_version or "") or None),
                chunking_strategy=(str(chunking_strategy or "") or None),
                retrieval_profile=(str(retrieval_profile or "") or None),
                processing_stage="embedding",
                status="embedding",
                storage_key=lifecycle_storage_key,
                embedding_provider=embedding_mode,
                embedding_model=embedding_model,
                embedding_dimension_expected=expected_embedding_dimension,
                embedding_dimension_source=expected_dim_source,
                collection=expected_collection,
                namespace=namespace,
                extras={"provider_source": provider_source},
            )
            logger_obj.info(
                (
                    "Embedding target selected: file_id=%s processing_id=%s provider=%s mode=%s model=%s "
                    "expected_dim=%s expected_dim_source=%s collection=%s namespace=%s"
                ),
                file_id,
                processing_id,
                provider_source,
                embedding_mode,
                embedding_model,
                expected_embedding_dimension,
                expected_dim_source,
                expected_collection,
                namespace,
            )

            existing_progress = (
                raw_file_custom_meta.get("ingestion_progress")
                if isinstance(raw_file_custom_meta, dict)
                else None
            )
            resume_batch_index = 1
            if isinstance(existing_progress, dict):
                chk = existing_progress.get("checkpoint")
                if isinstance(chk, dict):
                    try:
                        resume_batch_index = max(1, int(chk.get("next_batch_index", 1) or 1))
                    except Exception:
                        resume_batch_index = 1

            deleted = 0
            if resume_batch_index <= 1:
                try:
                    if processing_id is not None:
                        deleted = vector_store_obj.delete_by_metadata({"processing_id": str(processing_id)})
                        logger_obj.info(
                            "Pre-clean vector store by processing_id=%s file_id=%s deleted=%d",
                            processing_id,
                            file_id,
                            deleted,
                        )
                    else:
                        deleted = vector_store_obj.delete_by_metadata({"file_id": str(file_id)})
                        logger_obj.info("Pre-clean vector store by file_id=%s deleted=%d", file_id, deleted)
                except Exception:
                    logger_obj.warning("Vector pre-clean failed (continue)", exc_info=True)
            else:
                logger_obj.info(
                    "Resuming embedding/indexing from checkpoint: file_id=%s next_batch_index=%d",
                    file_id,
                    resume_batch_index,
                )

            items: List[Tuple[str, Dict[str, Any], str]] = []
            empty_chunks = 0
            target_collection_logged = False
            for idx, cd in enumerate(chunks):
                text = (cd.page_content or "").strip()
                if not text:
                    empty_chunks += 1
                    continue
                meta: Dict[str, Any] = {
                    "file_id": str(file_id),
                    "processing_id": str(processing_id) if processing_id is not None else "",
                    "owner_user_id": str(file_record.user_id),
                    "user_id": str(file_record.user_id),
                    "chat_id": chat_id,
                    "chunk_index": idx,
                    "doc_id": str(file_id),
                    "chunk_id": f"{file_id}_{idx}",
                    "filename": file_record.original_filename,
                    "file_type": str(getattr(file_record, "extension", getattr(file_record, "file_type", "")) or "").lower(),
                    "source_type": "tabular" if file_ext in {"xlsx", "xls", "csv", "tsv"} else "document",
                    "artifact_type": str((cd.metadata or {}).get("artifact_type") or (cd.metadata or {}).get("chunk_type") or "chunk"),
                    "embedding_mode": embedding_mode,
                    "embedding_model": embedding_model,
                    "collection": getattr(settings_obj, "COLLECTION_NAME", "documents"),
                    "pipeline_version": str(pipeline_version or ""),
                    "parser_version": str(parser_version or ""),
                    "artifact_version": str(artifact_version or ""),
                    "retrieval_profile": str(retrieval_profile or ""),
                    "is_active_processing": is_active_processing,
                }
                if cd.metadata:
                    for k, v in cd.metadata.items():
                        if k not in meta:
                            meta[k] = v
                doc_id = f"{file_id}_{idx}"
                if len(document_ids_sample) < 20:
                    document_ids_sample.append(doc_id)
                items.append((text, meta, doc_id))

            if not items:
                raise ValueError("All chunks are empty after split (nothing to embed).")

            progress["chunks_failed"] = int(progress["chunks_failed"]) + empty_chunks
            progress["chunks_processed"] = int(progress["chunks_processed"]) + empty_chunks
            batch_size = 32
            batches = batch_fn(items, batch_size)
            progress["embedding_batches_total"] = len(batches)
            progress["vector_upserts_expected"] = len(items)
            progress["checkpoint"] = {"next_batch_index": resume_batch_index, "batch_size": batch_size}
            logger_obj.info(
                "Embedding batches=%d batch_size=%d resume_from_batch=%d",
                len(batches),
                batch_size,
                resume_batch_index,
            )

            stage_t0 = asyncio.get_running_loop().time()
            for i, batch in enumerate(batches, start=1):
                if i < resume_batch_index:
                    continue
                texts = [t for (t, _, _) in batch]
                try:
                    vectors = await emb.embedd_documents_async(texts)
                except Exception as emb_exc:
                    progress["chunks_failed"] = int(progress["chunks_failed"]) + len(batch)
                    progress["chunks_processed"] = int(progress["chunks_processed"]) + len(batch)
                    progress["embedding_batches_failed"] = int(progress["embedding_batches_failed"]) + 1
                    classified = classify_ingestion_exception(emb_exc)
                    progress["failure_code"] = classified["code"]
                    logger_obj.warning(
                        "Embedding batch %d/%d failed code=%s retryable=%s fatal=%s",
                        i,
                        len(batches),
                        classified["code"],
                        classified["retryable"],
                        classified["fatal"],
                        exc_info=True,
                    )
                    progress["checkpoint"] = {"next_batch_index": i + 1, "batch_size": batch_size}
                    await _checkpoint(status="embedding", stage="embedding")
                    if classified["fatal"]:
                        progress["fatal_error"] = True
                        raise
                    continue

                if not vectors or len(vectors) != len(batch):
                    progress["chunks_failed"] = int(progress["chunks_failed"]) + len(batch)
                    progress["chunks_processed"] = int(progress["chunks_processed"]) + len(batch)
                    progress["embedding_batches_failed"] = int(progress["embedding_batches_failed"]) + 1
                    progress["checkpoint"] = {"next_batch_index": i + 1, "batch_size": batch_size}
                    await _checkpoint(status="embedding", stage="embedding")
                    logger_obj.warning("Embedding batch %d/%d invalid vectors size", i, len(batches))
                    continue

                for vec, (text, meta, doc_id) in zip(vectors, batch):
                    try:
                        observed_embedding_dimension = int(len(vec))
                        meta["embedding_dimension"] = observed_embedding_dimension
                        meta["collection"] = vector_store_obj.resolve_collection_name(
                            embedding=vec,
                            metadata=meta,
                        )
                        observed_collection = str(meta["collection"])
                        if not target_collection_logged:
                            logger_obj.info(
                                "Vector target: file_id=%s provider=%s model=%s dimension=%d collection=%s",
                                file_id,
                                embedding_mode,
                                embedding_model,
                                len(vec),
                                meta["collection"],
                            )
                            target_collection_logged = True
                        ok = vector_store_obj.add_document(
                            doc_id=doc_id,
                            embedding=vec,
                            metadata=meta,
                            content=text,
                        )
                        if ok:
                            progress["chunks_indexed"] = int(progress["chunks_indexed"]) + 1
                            progress["chunks_processed"] = int(progress["chunks_processed"]) + 1
                            progress["vector_upserts_actual"] = int(progress["vector_upserts_actual"]) + 1
                        else:
                            progress["chunks_failed"] = int(progress["chunks_failed"]) + 1
                            progress["chunks_processed"] = int(progress["chunks_processed"]) + 1
                            logger_obj.warning("Vector upsert returned False doc_id=%s", doc_id)
                    except Exception as upsert_exc:
                        progress["chunks_failed"] = int(progress["chunks_failed"]) + 1
                        progress["chunks_processed"] = int(progress["chunks_processed"]) + 1
                        classified = classify_ingestion_exception(upsert_exc)
                        progress["failure_code"] = classified["code"]
                        if classified["fatal"]:
                            progress["fatal_error"] = True
                            raise
                        logger_obj.warning("Vector upsert failed doc_id=%s", doc_id, exc_info=True)
                progress["checkpoint"] = {"next_batch_index": i + 1, "batch_size": batch_size}
                await _checkpoint(
                    status="indexing" if i < len(batches) else "embedding",
                    stage="indexing" if i < len(batches) else "embedding",
                )

            progress["embedding_ok"] = bool(int(progress.get("embedding_batches_failed", 0) or 0) == 0)
            progress["indexing_ok"] = bool(
                int(progress.get("vector_upserts_actual", 0) or 0) == int(progress.get("chunks_indexed", 0) or 0)
            )
            log_file_lifecycle_event(
                logger_obj,
                "embedding_completed",
                user_id=lifecycle_user_id,
                chat_id=chat_id,
                conversation_id=chat_id,
                chat_ids=lifecycle_chat_ids,
                conversation_ids=lifecycle_chat_ids,
                file_id=file_id,
                filename=lifecycle_filename,
                upload_id=lifecycle_upload_id,
                processing_id=processing_id,
                pipeline_version=(str(pipeline_version or "") or None),
                parser_version=(str(parser_version or "") or None),
                artifact_version=(str(artifact_version or "") or None),
                chunking_strategy=(str(chunking_strategy or "") or None),
                retrieval_profile=(str(retrieval_profile or "") or None),
                processing_stage="embedding",
                status="embedded",
                storage_key=lifecycle_storage_key,
                embedding_provider=embedding_mode,
                embedding_model=embedding_model,
                embedding_dimension_expected=expected_embedding_dimension,
                embedding_dimension_actual=observed_embedding_dimension,
                embedding_dimension_source=expected_dim_source,
                collection=observed_collection,
                namespace=namespace,
                document_ids=document_ids_sample,
                extras={
                    "embedding_batches_total": int(progress.get("embedding_batches_total", 0) or 0),
                    "embedding_batches_failed": int(progress.get("embedding_batches_failed", 0) or 0),
                },
            )
            log_file_lifecycle_event(
                logger_obj,
                "indexing_completed",
                user_id=lifecycle_user_id,
                chat_id=chat_id,
                conversation_id=chat_id,
                chat_ids=lifecycle_chat_ids,
                conversation_ids=lifecycle_chat_ids,
                file_id=file_id,
                filename=lifecycle_filename,
                upload_id=lifecycle_upload_id,
                processing_id=processing_id,
                pipeline_version=(str(pipeline_version or "") or None),
                parser_version=(str(parser_version or "") or None),
                artifact_version=(str(artifact_version or "") or None),
                chunking_strategy=(str(chunking_strategy or "") or None),
                retrieval_profile=(str(retrieval_profile or "") or None),
                processing_stage="indexing",
                status="indexed",
                storage_key=lifecycle_storage_key,
                embedding_provider=embedding_mode,
                embedding_model=embedding_model,
                embedding_dimension_expected=expected_embedding_dimension,
                embedding_dimension_actual=observed_embedding_dimension,
                embedding_dimension_source=expected_dim_source,
                collection=observed_collection,
                namespace=namespace,
                document_ids=document_ids_sample,
                extras={
                    "vector_upserts_expected": int(progress.get("vector_upserts_expected", 0) or 0),
                    "vector_upserts_actual": int(progress.get("vector_upserts_actual", 0) or 0),
                    "chunks_indexed": int(progress.get("chunks_indexed", 0) or 0),
                    "chunks_failed": int(progress.get("chunks_failed", 0) or 0),
                },
            )
            observe_ms_fn("ingestion_stage_ms", (asyncio.get_running_loop().time() - stage_t0) * 1000.0, stage="embed_upsert")
            logger_obj.info(
                (
                    "File processing done before finalize: file_id=%s chunks_expected=%d chunks_processed=%d "
                    "chunks_indexed=%d chunks_failed=%d empty_chunks=%d deleted_old=%d chat_id=%s "
                    "embedding_model=%s embedding_batches=%d embedding_batches_failed=%d "
                    "upserts_expected=%d upserts_actual=%d"
                ),
                file_id,
                progress["total_chunks_expected"],
                progress["chunks_processed"],
                progress["chunks_indexed"],
                progress["chunks_failed"],
                empty_chunks,
                deleted,
                chat_id,
                embedding_model,
                progress["embedding_batches_total"],
                progress["embedding_batches_failed"],
                progress["vector_upserts_expected"],
                progress["vector_upserts_actual"],
            )

        except Exception as e:
            classified = classify_ingestion_exception(e)
            error_message = f"{type(e).__name__}: {e}"
            progress["failure_code"] = classified["code"]
            progress["fatal_error"] = bool(progress.get("fatal_error", False) or classified["fatal"])
            failure_retryable = bool(
                classified["retryable"]
                and not progress["fatal_error"]
                and int(progress.get("chunks_processed", 0) or 0) == 0
            )
            progress["stage"] = "failed"
            progress["status"] = "failed"
            logger_obj.error("File processing failed: file_id=%s err=%s", file_id, error_message, exc_info=True)
        finally:
            try:
                if observed_embedding_dimension is not None:
                    extra_metadata["embedding_dimension"] = int(observed_embedding_dimension)
                if observed_collection:
                    extra_metadata["collection"] = str(observed_collection)
                extra_metadata["namespace"] = str(getattr(settings_obj, "COLLECTION_NAME", "documents") or "documents")
                observe_ms_fn(
                    "ingestion_total_ms",
                    (asyncio.get_running_loop().time() - started_ms) * 1000.0,
                    file_type=file_path.suffix.lower().lstrip(".") or "unknown",
                )
                final_status = await finalize_ingestion_fn(
                    db=db,
                    file_id=file_id,
                    processing_id=processing_id,
                    progress=progress,
                    embedding_mode=embedding_mode,
                    embedding_model=embedding_model,
                    error_message=error_message,
                    extra_metadata=extra_metadata,
                )
                retryable = bool(final_status == "failed" and failure_retryable)
                return (final_status in ("completed", "partial_failed", "partial_success"), retryable)
            finally:
                file_id_ctx_var.reset(file_ctx_token)

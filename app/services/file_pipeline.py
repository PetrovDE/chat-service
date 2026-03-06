from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from uuid import UUID


async def finalize_ingestion_pipeline(
    *,
    db: Any,
    file_id: UUID,
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

    if indexed <= 0:
        final_status = "failed"
    elif failed <= 0:
        final_status = "completed"
    elif bad_ratio > threshold:
        final_status = "failed"
    else:
        final_status = "partial_success"

    progress["status"] = final_status
    progress["bad_ratio"] = bad_ratio
    progress["finished_at"] = utc_now_iso_fn()
    progress["stage"] = "finalized"

    metadata_patch: Dict[str, Any] = {"ingestion_progress": progress}
    if error_message:
        metadata_patch["error"] = error_message
    if extra_metadata:
        metadata_patch.update(extra_metadata)

    await update_processing_status_fn(
        db,
        file_id=file_id,
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
        "Ingestion finalized: file_id=%s status=%s expected=%d processed=%d indexed=%d failed=%d bad_ratio=%.4f threshold=%.4f",
        file_id,
        final_status,
        expected,
        processed,
        indexed,
        failed,
        bad_ratio,
        threshold,
    )
    return final_status


async def process_file_pipeline(
    *,
    file_id: UUID,
    file_path: Path,
    embedding_mode: str,
    embedding_model: str,
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
    file_id_ctx_var,
    settings_obj,
    observe_ms_fn,
    logger_obj,
) -> Tuple[bool, bool]:
    file_ctx_token = file_id_ctx_var.set(str(file_id))
    started_ms = asyncio.get_running_loop().time()
    progress: Dict[str, Any] = {
        "status": "processing",
        "stage": "queued",
        "total_chunks_expected": 0,
        "chunks_processed": 0,
        "chunks_failed": 0,
        "chunks_indexed": 0,
        "started_at": utc_now_iso_fn(),
        "finished_at": None,
        "bad_ratio": 0.0,
    }
    error_message: Optional[str] = None
    extra_metadata: Dict[str, Any] = {}

    async with async_session_factory() as db:
        try:
            logger_obj.info(
                "Starting file processing: file_id=%s mode=%s embedding_model=%s path=%s",
                file_id, embedding_mode, embedding_model, file_path
            )
            await crud_file_module.update_processing_status(
                db,
                file_id=file_id,
                status="processing",
                metadata_patch={"ingestion_progress": progress, "error": None},
            )

            q = select_fn(conversation_file_model.conversation_id).where(conversation_file_model.file_id == file_id)
            r = await db.execute(q)
            conv_ids = r.scalars().all()
            conversation_id = str(conv_ids[0]) if conv_ids else None

            stage_t0 = asyncio.get_running_loop().time()
            progress["stage"] = "extract"
            docs = await document_loader_obj.load_file(str(file_path))
            if not docs:
                raise ValueError("No documents loaded from file")

            total_chars = sum(len(d.page_content or "") for d in docs)
            xlsx_stats = extract_xlsx_stats_fn(docs) if file_path.suffix.lower() in (".xlsx", ".xls") else {}
            logger_obj.info(
                "Loaded documents=%d extracted_text_chars=%d file_id=%s %s",
                len(docs),
                total_chars,
                file_id,
                " ".join([f"{k}={v}" for k, v in xlsx_stats.items()]) if xlsx_stats else "",
            )
            observe_ms_fn("ingestion_stage_ms", (asyncio.get_running_loop().time() - stage_t0) * 1000.0, stage="extract")
            if total_chars < 50:
                raise ValueError("Extracted text is empty/too small (possible scanned PDF).")

            file_record = await crud_file_module.get(db, id=file_id)
            if not file_record:
                raise ValueError(f"File record not found: {file_id}")

            file_ext = file_path.suffix.lower().lstrip(".")
            if file_ext in {"xlsx", "xls", "csv"}:
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
            progress["stage"] = "chunk"
            if file_ext in ("xlsx", "xls", "csv"):
                chunks = docs
            else:
                chunks = text_splitter_obj.split_documents(docs)
            if not chunks:
                raise ValueError("No chunks created from documents")
            progress["total_chunks_expected"] = len(chunks)
            logger_obj.info(
                "Created chunks_expected=%d chunk_size=%d overlap=%d file_id=%s",
                len(chunks),
                settings_obj.CHUNK_SIZE,
                settings_obj.CHUNK_OVERLAP,
                file_id,
            )
            observe_ms_fn("ingestion_stage_ms", (asyncio.get_running_loop().time() - stage_t0) * 1000.0, stage="chunk")

            emb = embeddings_manager_cls(mode=embedding_mode, model=embedding_model)
            logger_obj.info("Embedding mode=%s resolved_model=%s", embedding_mode, embedding_model)

            deleted = 0
            try:
                deleted = vector_store_obj.delete_by_metadata({"file_id": str(file_id)})
                logger_obj.info("Pre-clean vector store by file_id=%s deleted=%d", file_id, deleted)
            except Exception:
                logger_obj.warning("Vector pre-clean failed (continue)", exc_info=True)

            items: List[Tuple[str, Dict[str, Any], str]] = []
            empty_chunks = 0
            for idx, cd in enumerate(chunks):
                text = (cd.page_content or "").strip()
                if not text:
                    empty_chunks += 1
                    continue
                meta: Dict[str, Any] = {
                    "file_id": str(file_id),
                    "user_id": str(file_record.user_id),
                    "conversation_id": conversation_id,
                    "chunk_index": idx,
                    "doc_id": str(file_id),
                    "chunk_id": f"{file_id}_{idx}",
                    "filename": file_record.original_filename,
                    "file_type": file_record.file_type,
                    "embedding_mode": embedding_mode,
                    "embedding_model": embedding_model,
                }
                if cd.metadata:
                    for k, v in cd.metadata.items():
                        if k not in meta:
                            meta[k] = v
                doc_id = f"{file_id}_{idx}"
                items.append((text, meta, doc_id))

            if not items:
                raise ValueError("All chunks are empty after split (nothing to embed).")

            progress["chunks_failed"] = int(progress["chunks_failed"]) + empty_chunks
            progress["chunks_processed"] = int(progress["chunks_processed"]) + empty_chunks
            stored = 0
            batches = batch_fn(items, 32)
            logger_obj.info("Embedding batches=%d batch_size=%d", len(batches), 32)

            stage_t0 = asyncio.get_running_loop().time()
            progress["stage"] = "embed_upsert"
            for i, batch in enumerate(batches, start=1):
                texts = [t for (t, _, _) in batch]
                try:
                    vectors = await emb.embedd_documents_async(texts)
                except Exception:
                    progress["chunks_failed"] = int(progress["chunks_failed"]) + len(batch)
                    progress["chunks_processed"] = int(progress["chunks_processed"]) + len(batch)
                    logger_obj.warning("Embedding batch %d/%d failed (skip batch)", i, len(batches), exc_info=True)
                    continue

                if not vectors or len(vectors) != len(batch):
                    progress["chunks_failed"] = int(progress["chunks_failed"]) + len(batch)
                    progress["chunks_processed"] = int(progress["chunks_processed"]) + len(batch)
                    logger_obj.warning("Embedding batch %d/%d invalid vectors size", i, len(batches))
                    continue

                for vec, (text, meta, doc_id) in zip(vectors, batch):
                    try:
                        ok = vector_store_obj.add_document(
                            doc_id=doc_id,
                            embedding=vec,
                            metadata=meta,
                            content=text,
                        )
                        if ok:
                            stored += 1
                            progress["chunks_indexed"] = int(progress["chunks_indexed"]) + 1
                            progress["chunks_processed"] = int(progress["chunks_processed"]) + 1
                        else:
                            progress["chunks_failed"] = int(progress["chunks_failed"]) + 1
                            progress["chunks_processed"] = int(progress["chunks_processed"]) + 1
                            logger_obj.warning("Vector upsert returned False doc_id=%s", doc_id)
                    except Exception:
                        progress["chunks_failed"] = int(progress["chunks_failed"]) + 1
                        progress["chunks_processed"] = int(progress["chunks_processed"]) + 1
                        logger_obj.warning("Vector upsert failed doc_id=%s", doc_id, exc_info=True)

            observe_ms_fn("ingestion_stage_ms", (asyncio.get_running_loop().time() - stage_t0) * 1000.0, stage="embed_upsert")
            logger_obj.info(
                "File processing done before finalize: file_id=%s chunks_expected=%d chunks_processed=%d chunks_indexed=%d chunks_failed=%d empty_chunks=%d deleted_old=%d conversation_id=%s embedding_model=%s",
                file_id,
                progress["total_chunks_expected"],
                progress["chunks_processed"],
                progress["chunks_indexed"],
                progress["chunks_failed"],
                empty_chunks,
                deleted,
                conversation_id,
                embedding_model,
            )

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}"
            progress["stage"] = "failed"
            logger_obj.error("File processing failed: file_id=%s err=%s", file_id, error_message, exc_info=True)
        finally:
            try:
                observe_ms_fn(
                    "ingestion_total_ms",
                    (asyncio.get_running_loop().time() - started_ms) * 1000.0,
                    file_type=file_path.suffix.lower().lstrip(".") or "unknown",
                )
                final_status = await finalize_ingestion_fn(
                    db=db,
                    file_id=file_id,
                    progress=progress,
                    embedding_mode=embedding_mode,
                    embedding_model=embedding_model,
                    error_message=error_message,
                    extra_metadata=extra_metadata,
                )
                retryable = bool(
                    final_status == "failed"
                    and int(progress.get("total_chunks_expected", 0) or 0) == 0
                    and int(progress.get("chunks_processed", 0) or 0) == 0
                    and bool(error_message)
                )
                return (final_status in ("completed", "partial_success"), retryable)
            finally:
                file_id_ctx_var.reset(file_ctx_token)

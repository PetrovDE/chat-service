# Changelog

## [Unreleased] - 2026-03-03

### Documentation
- Added architecture study baseline for production LLM chat + file analytics: 
  - `docs/11_llm_file_chat_best_practices_architecture.md`
  - README section `Target architecture direction` with implementation focus points.
- Reworked architecture baseline for closed contour operation:
  - `AI HUB` defined as primary model runtime,
  - `Ollama` restricted to emergency fallback-by-policy,
  - added implementation prompt pack: `docs/12_codex_cursor_prompts_offline_architecture.md`.

### Ð˜Ð·Ð¼ÐµÐ½ÐµÐ½Ð¾
- Frontend composer: Ð¿Ð¾Ð»Ðµ Ð²Ð²Ð¾Ð´Ð° Ð¾ÑÑ‚Ð°Ð²Ð»ÐµÐ½Ð¾ Ð¾Ñ‚Ð´ÐµÐ»ÑŒÐ½Ð¾Ð¹ Ð²ÐµÑ€Ñ…Ð½ÐµÐ¹ ÑÑ‚Ñ€Ð¾ÐºÐ¾Ð¹; Ð½Ð¸Ð¶Ð½ÑÑ ÑÑ‚Ñ€Ð¾ÐºÐ° ÐºÐ¾Ð½Ñ‚Ñ€Ð¾Ð»Ð¾Ð² Ð¿Ñ€Ð¸Ð²ÐµÐ´ÐµÐ½Ð° Ðº Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚Ñƒ `File + provider + model + RAG mode + Send`.
- ÐŸÐ¾Ð´ ÑÐµÐ»ÐµÐºÑ‚Ð°Ð¼Ð¸ provider/model/rag mode Ð´Ð¾Ð±Ð°Ð²Ð»ÐµÐ½Ñ‹ inline-Ð¿Ð¾Ð´ÑÐºÐ°Ð·ÐºÐ¸ (`ÐŸÑ€Ð¾Ð²Ð°Ð¹Ð´ÐµÑ€ AI`, `Ð’Ñ‹Ð±Ð¾Ñ€ Ð¼Ð¾Ð´ÐµÐ»Ð¸`, `Ð ÐµÐ¶Ð¸Ð¼ Ñ€Ð°Ð±Ð¾Ñ‚Ñ‹ Ñ Ð´Ð¾ÐºÑƒÐ¼ÐµÐ½Ñ‚Ð°Ð¼Ð¸`).
- `Logout` Ð² `Settings` Ð¿ÐµÑ€ÐµÐ½ÐµÑÑ‘Ð½ Ð² Ñ„ÑƒÑ‚ÐµÑ€ Ð¸ Ð¾Ñ„Ð¾Ñ€Ð¼Ð»ÐµÐ½ ÐºÐ°Ðº danger-ÐºÐ½Ð¾Ð¿ÐºÐ° Ñ€ÑÐ´Ð¾Ð¼ Ñ `Save`.

### Ð˜ÑÐ¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾
- Ð’ sidebar Ñ‡Ð°Ñ‚Ð¾Ð² `Del` Ð·Ð°Ð¼ÐµÐ½Ñ‘Ð½ Ð½Ð° Ð¾Ñ‚Ð´ÐµÐ»ÑŒÐ½ÑƒÑŽ Ð·Ð°Ð¼ÐµÑ‚Ð½ÑƒÑŽ ÐºÐ½Ð¾Ð¿ÐºÑƒ ÑƒÐ´Ð°Ð»ÐµÐ½Ð¸Ñ Ñ Ð¸ÐºÐ¾Ð½ÐºÐ¾Ð¹ ÐºÐ¾Ñ€Ð·Ð¸Ð½Ñ‹ ÑÐ¿Ñ€Ð°Ð²Ð° Ð¾Ñ‚ ÑÑ‚Ñ€Ð¾ÐºÐ¸ Ñ‡Ð°Ñ‚Ð°.
- Ð‘Ð»Ð¾Ðº `RAG debug` Ð¿Ð¾Ð´ ÑÐ¾Ð¾Ð±Ñ‰ÐµÐ½Ð¸ÐµÐ¼ Ð±Ð¾Ð»ÑŒÑˆÐµ Ð½Ðµ Ð¾Ñ‚Ð¾Ð±Ñ€Ð°Ð¶Ð°ÐµÑ‚ÑÑ Ð¿Ñ€Ð¸ Ð²Ñ‹ÐºÐ»ÑŽÑ‡ÐµÐ½Ð½Ð¾Ð¼ Ñ„Ð»Ð°Ð³Ðµ: debug-Ð¼ÐµÑ‚Ð° Ð¿Ð¾ÐºÐ°Ð·Ñ‹Ð²Ð°ÐµÑ‚ÑÑ Ñ‚Ð¾Ð»ÑŒÐºÐ¾ ÐµÑÐ»Ð¸ Ð·Ð°Ð¿Ñ€Ð¾Ñ Ð¾Ñ‚Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½ Ñ `rag_debug=true`.

### Backend RAG (2026-03-03)
- Stage 0 diagnostics:
  - added row-level debug fields: `rows_expected_total`, `rows_retrieved_total`, `rows_used_map_total`, `rows_used_reduce_total`, `row_coverage_ratio`;
  - added full-file per-batch diagnostics: `batch_rows_start_end`, `batch_input_chars`, `batch_output_chars`;
  - added AIHub prompt trim telemetry in debug: `prompt_chars_before`, `prompt_chars_after`, `prompt_truncated`.
- Stage 1 ingestion:
  - `xlsx/xls/csv` loader switched to adaptive row-dense chunking (`XLSX_CHUNK_MAX_CHARS`, `XLSX_CHUNK_MAX_ROWS`);
  - added wide-sheet column pruning (`XLSX_MAX_COLUMNS_PER_CHUNK`) with full column set in metadata.
- Stage 2 coverage safeguards:
  - added full-file row-coverage repass (`RAG_FULL_FILE_MIN_ROW_COVERAGE`, `RAG_FULL_FILE_ESCALATION_MAX_CHUNKS`);
  - added `silent_row_loss_detected` signal when chunk coverage is high but row coverage is low.
- Stage 3 full-file answer quality:
  - map step now returns structured JSON (`facts`, `aggregates`, `row_ranges_covered`, `missing_data`);
  - reduce step merges structured payload deterministically (`strategy=structured_map_reduce`);
  - added setting `FULL_FILE_MAP_MAX_TOKENS`.
- Stage 4 deterministic tabular path:
  - ingestion creates sidecar SQLite dataset for `xlsx/xls/csv` (`custom_metadata.tabular_sidecar`);
  - aggregate intents route to `retrieval_mode=tabular_sql` (LangChain SQL tool execution), LLM used only for presentation;
  - added `tabular_profile` intent path for broad analytical prompts (`per-column stats/metrics`, `Ð¾Ð±Ñ‰Ð¸Ð¹ Ð°Ð½Ð°Ð»Ð¸Ð· Ñ„Ð°Ð¹Ð»Ð°`);
  - file delete now cleans sidecar path best effort.
- Embedding robustness:
  - replaced lossy truncation of long local/Ollama embedding inputs with segmentation + overlap + mean pooling (`OLLAMA_EMBED_MAX_INPUT_CHARS`, `OLLAMA_EMBED_SEGMENT_OVERLAP_CHARS`);
  - improved Ollama `/api/embed` HTTP 400 diagnostics in logs.
- XLSX serialization:
  - long cells are preserved via continuation lines (`Row N (cont)`) and optional cap `XLSX_CELL_MAX_CHARS` (default `0`, no cap).
- `full_file` retrieval: removed post-retrieval squeeze to hybrid limit (`top_k*4`); full retrieved set is preserved within `RAG_FULL_FILE_MAX_CHUNKS`.
- Added full-file coverage diagnostics in debug: `retrieved_chunks_total`, `coverage.expected_chunks`, `coverage.retrieved_chunks`, `coverage.ratio`, `coverage.complete`.
- Added explicit incomplete-coverage signal: when `retrieved < expected`, `truncated=true` and caveat about incomplete analysis.
- Extended tabular debug/source metadata: `row_start`, `row_end`, `total_rows`; sources now include `rows=<start>-<end>` when available.
- Updated full-file map-reduce prompts for table batches to preserve row ranges and numeric signals (outliers/trends).
- Increased default `FULL_FILE_MAP_BATCH_MAX_CHARS` to `25000` to reduce full-file map/reduce latency on large spreadsheets.
- Added response language policy: answer language follows user query language (RU->RU, EN->EN) with post-generation language rewrite fallback.
- Added coverage-based full-file source aggregation (`sheet + merged row ranges`) to avoid references to only the last chunk.
- Stabilized Excel/CSV ingestion for embeddings: reduced row-block size to compact chunks (up to 20 rows) to prevent provider `400` on oversized inputs.
- Added dynamic retrieval budget policy for `auto/hybrid`:
  - short query -> ~20% from known document chunks,
  - fact query -> ~10%,
  - broad/analysis query -> ~30%.
- Added low-coverage one-step escalation with debug trace (`rag_debug.retrieval_policy.escalation`):
  - increase `top_k` or
  - switch to `full_file` for small documents.
- Fixed context merge dedup stability for tabular chunks:
  - dedup key in retrieval context now uses `chunk_id` (fallback: `file_id + chunk_index`) and no longer uses text-prefix/header similarity,
  - prevents false collapse of Excel chunks with identical repeated headers (`=== ... EXCEL | SHEET ... ===`).
- Split metadata semantics for IDs:
  - `doc_id` now represents the whole document (`file_id`),
  - `chunk_id` represents the exact chunk (`<file_id>_<chunk_index>`).
- Added regression tests:
  - `test_xlsx_wide_sheet_chunking_adaptive`
  - `test_full_file_row_coverage_debug_fields`
  - `test_full_file_map_reduce_structured_preserves_ranges`
  - `test_aihub_prompt_truncation_debug_visible`
  - `test_tabular_intent_routes_to_sql_path`
  - `test_local_embedding_inputs_are_segmented_without_lossy_truncation`
  - `test_xlsx_long_cell_not_lossy_truncated`
  - `test_tabular_profile_intent_has_priority_over_aggregate_keywords`

### Backend Refactor (2026-03-03)
- Decomposed `app/services/chat_orchestrator.py` into dedicated modules under `app/services/chat/*`.
- Kept API/SSE contracts unchanged while moving language policy, RAG prompt builder, full-file map/reduce, debug/source formatting and post-processing logic out of the facade.
- Migrated integration tests to direct imports from `app/services/chat/*` modules and removed legacy private-alias coupling in `chat_orchestrator`.
- Chat orchestration now uses shared private pipeline steps for generation/post-processing to reduce duplication between stream and non-stream flows.

## [1.0.0] - 2025-10-16

### Ð”Ð¾Ð±Ð°Ð²Ð»ÐµÐ½Ð¾
- âœ… Ð¡Ð¸ÑÑ‚ÐµÐ¼Ð° Ð°ÑƒÑ‚ÐµÐ½Ñ‚Ð¸Ñ„Ð¸ÐºÐ°Ñ†Ð¸Ð¸ (JWT)
- âœ… Ð£Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¸Ðµ Ð±ÐµÑÐµÐ´Ð°Ð¼Ð¸
- âœ… Ð˜ÑÑ‚Ð¾Ñ€Ð¸Ñ Ñ‡Ð°Ñ‚Ð¾Ð²
- âœ… Ð˜Ð½Ñ‚ÐµÐ³Ñ€Ð°Ñ†Ð¸Ñ Ñ Ollama
- âœ… Ð˜Ð½Ñ‚ÐµÐ³Ñ€Ð°Ñ†Ð¸Ñ Ñ OpenAI
- âœ… PostgreSQL Ð±Ð°Ð·Ð° Ð´Ð°Ð½Ð½Ñ‹Ñ…
- âœ… ÐœÐ¸Ð³Ñ€Ð°Ñ†Ð¸Ð¸ Alembic
- âœ… API Ð´Ð¾ÐºÑƒÐ¼ÐµÐ½Ñ‚Ð°Ñ†Ð¸Ñ
- âœ… ÐÐ´Ð°Ð¿Ñ‚Ð¸Ð²Ð½Ñ‹Ð¹ UI
- âœ… Ð›Ð¾Ð³Ð¸Ñ€Ð¾Ð²Ð°Ð½Ð¸Ðµ Ð·Ð°Ð¿Ñ€Ð¾ÑÐ¾Ð²

### Ð˜Ð·Ð¼ÐµÐ½ÐµÐ½Ð¾
- ÐœÐ¾Ð´ÑƒÐ»ÑŒÐ½Ð°Ñ ÑÑ‚Ñ€ÑƒÐºÑ‚ÑƒÑ€Ð° frontend
- ÐžÐ¿Ñ‚Ð¸Ð¼Ð¸Ð·Ð°Ñ†Ð¸Ñ SQL Ð·Ð°Ð¿Ñ€Ð¾ÑÐ¾Ð²

### Ð˜ÑÐ¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¾
- Ð˜Ð·Ð¾Ð»ÑÑ†Ð¸Ñ Ð´Ð°Ð½Ð½Ñ‹Ñ… Ð¿Ð¾Ð»ÑŒÐ·Ð¾Ð²Ð°Ñ‚ÐµÐ»ÐµÐ¹
- Ð¡Ñ‡Ñ‘Ñ‚Ñ‡Ð¸Ðº ÑÐ¾Ð¾Ð±Ñ‰ÐµÐ½Ð¸Ð¹ Ð² Ð±ÐµÑÐµÐ´Ð°Ñ…




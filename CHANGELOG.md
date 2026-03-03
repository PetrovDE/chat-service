# Changelog

## [Unreleased] - 2026-03-03

### Изменено
- Frontend composer: поле ввода оставлено отдельной верхней строкой; нижняя строка контролов приведена к формату `File + provider + model + RAG mode + Send`.
- Под селектами provider/model/rag mode добавлены inline-подсказки (`Провайдер AI`, `Выбор модели`, `Режим работы с документами`).
- `Logout` в `Settings` перенесён в футер и оформлен как danger-кнопка рядом с `Save`.

### Исправлено
- В sidebar чатов `Del` заменён на отдельную заметную кнопку удаления с иконкой корзины справа от строки чата.
- Блок `RAG debug` под сообщением больше не отображается при выключенном флаге: debug-мета показывается только если запрос отправлен с `rag_debug=true`.

### Backend RAG (2026-03-03)
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

### Backend Refactor (2026-03-03)
- Decomposed `app/services/chat_orchestrator.py` into dedicated modules under `app/services/chat/*`.
- Kept API/SSE contracts unchanged while moving language policy, RAG prompt builder, full-file map/reduce, debug/source formatting and post-processing logic out of the facade.
- Migrated integration tests to direct imports from `app/services/chat/*` modules and removed legacy private-alias coupling in `chat_orchestrator`.
- Chat orchestration now uses shared private pipeline steps for generation/post-processing to reduce duplication between stream and non-stream flows.

## [1.0.0] - 2025-10-16

### Добавлено
- ✅ Система аутентификации (JWT)
- ✅ Управление беседами
- ✅ История чатов
- ✅ Интеграция с Ollama
- ✅ Интеграция с OpenAI
- ✅ PostgreSQL база данных
- ✅ Миграции Alembic
- ✅ API документация
- ✅ Адаптивный UI
- ✅ Логирование запросов

### Изменено
- Модульная структура frontend
- Оптимизация SQL запросов

### Исправлено
- Изоляция данных пользователей
- Счётчик сообщений в беседах

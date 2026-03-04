# llama-service

Сервис для диалогов с LLM (SSE/обычный ответ), RAG по загруженным файлам и управлением чатами/файлами через API и встроенный web-frontend.

## Что делает сервис
- Принимает сообщения в чат (`/api/v1/chat`, `/api/v1/chat/stream`).
- Поддерживает провайдеры моделей: `ollama/local`, `aihub` (`corporate` alias), `openai`.
- Загружает файлы, асинхронно индексирует их в ChromaDB и использует в RAG (`/api/v1/files/*`).
- Для `xlsx/xls/csv` поддерживает adaptive row-dense ingestion и deterministic `tabular_sql` path для aggregate-вопросов.
- Для аналитических запросов по файлу поддерживает deterministic `tabular_profile` (per-column SQL stats).
- Хранит пользователей, чаты, сообщения и файлы в PostgreSQL.
- Отдаёт встроенный frontend из `frontend/` (SPA монтируется на `/`).

## Быстрый старт
### 1) Backend
1. Установить зависимости:
```bash
pip install -r requirements.txt
```
2. Поднять PostgreSQL (локально через compose):
```bash
docker compose -f docker-compose.db.yml up -d
```
3. Заполнить `.env` минимумом:
```env
DATABASE_URL=postgresql+asyncpg://llama_chat_user:5432@localhost:5432/llama_chat_db
ALEMBIC_DATABASE_URL=postgresql://llama_chat_user:5432@localhost:5432/llama_chat_db
JWT_SECRET_KEY=change-me
```
4. Применить миграции:
```bash
alembic upgrade head
```
5. (Опционально) создать администратора:
```bash
python scripts/create_admin.py
```
По умолчанию создаётся `admin / admin123456`.
6. Запустить API:
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 2) Frontend
Frontend отдельной сборки не требует.
- Открыть: `http://localhost:8000/`
- API/Swagger: `http://localhost:8000/docs`
- Health: `http://localhost:8000/health`
- Prometheus metrics: `http://localhost:8000/metrics`

## Краткая схема компонентов
```text
Browser SPA (frontend/index.html + static/js)
    -> FastAPI (app/main.py, /api/v1/*)
        -> Chat orchestration (app/services/chat_orchestrator.py)
            -> LLM providers (app/services/llm/providers/*)
            -> RAG retriever (app/rag/retriever.py)
            -> Tabular SQL path (app/services/chat/tabular_sql.py, LangChain SQL tools)
                -> ChromaDB (.chromadb)
        -> File ingestion worker (app/services/file.py, in-process queue)
            -> loaders/splitters/embeddings
            -> ChromaDB upsert
        -> CRUD/SQLAlchemy (app/crud/*, app/db/*)
            -> PostgreSQL
```

## Документация (`docs/*`)
1. [`docs/00_project_map.md`](docs/00_project_map.md)
2. [`docs/01_service_spec.md`](docs/01_service_spec.md)
3. [`docs/02_api_contracts.md`](docs/02_api_contracts.md)
4. [`docs/03_backend_architecture.md`](docs/03_backend_architecture.md)
5. [`docs/04_frontend_architecture.md`](docs/04_frontend_architecture.md)
6. [`docs/05_observability.md`](docs/05_observability.md)
7. [`docs/06_testing_and_dod.md`](docs/06_testing_and_dod.md)
8. [`docs/examples/chat.request.json`](docs/examples/chat.request.json)
9. [`docs/examples/chat.response.json`](docs/examples/chat.response.json)
10. [`docs/examples/delete.error.404.json`](docs/examples/delete.error.404.json)
11. [`docs/examples/delete.response.json`](docs/examples/delete.response.json)
12. [`docs/examples/retrieve_debug.request.json`](docs/examples/retrieve_debug.request.json)
13. [`docs/examples/retrieve_debug.response.json`](docs/examples/retrieve_debug.response.json)
14. [`docs/examples/status.response.partial_success.json`](docs/examples/status.response.partial_success.json)
15. [`docs/examples/status.response.processing.json`](docs/examples/status.response.processing.json)
16. [`docs/examples/upload.request.json`](docs/examples/upload.request.json)
17. [`docs/examples/upload.response.json`](docs/examples/upload.response.json)
18. [`docs/ux/chat_states.md`](docs/ux/chat_states.md)
19. [`docs/ux/file_ingestion_progress.md`](docs/ux/file_ingestion_progress.md)
20. [`docs/09_dynamic_rag_budget_plan.md`](docs/09_dynamic_rag_budget_plan.md)

## Актуальные UI-изменения (frontend)
- В sidebar чатов удаление вынесено в отдельную кнопку-корзину справа от каждого чата.
- В `Settings` кнопка `Logout` расположена в футере рядом с `Save` и оформлена как danger-кнопка.
- В composer поле ввода оставлено сверху, а ниже расположен единый ряд контролов: `File + provider + model + RAG mode + Send`.
- Под селектами `provider/model/rag mode` показываются inline-подсказки.
- `RAG debug` показывается под assistant-сообщением только когда пользователь включил этот флаг в `Settings`.

## Troubleshooting (топ-5)
1. `401/403` на защищённых endpoint'ах (`/api/v1/files/*`, `/api/v1/conversations/*`, `/api/v1/stats/*`).
Где смотреть: DevTools Network (заголовок `Authorization`), backend-логи с `rid/uid` (stdout, формат из `app/core/logging.py`).

2. Файл завис в `processing` или ушёл в `failed`.
Где смотреть: `GET /api/v1/files/status/{file_id}` (stage/counters/error), `GET /api/v1/stats/observability` (admin), backend-логи `app/services/file.py`.

3. Пустой/слабый RAG-ответ.
Где смотреть: отправить chat с `rag_debug=true` (или `?debug=true`), проверить `rag_debug.top_chunks`, `filters/where`, `retrieval_mode`; сравнить с `docs/examples/retrieve_debug.*.json`.
Если файл табличный (`xlsx/xls/csv`) и в debug видно аномально малое `retrieved_chunks_total` при нормальном `chunks_count`, проверьте metadata chunk identity:
`chunk_id`, `chunk_index` (dedup контекста выполняется по `chunk_id`, fallback: `file_id + chunk_index`).
Для `full_file` дополнительно проверьте row-level поля:
`rows_expected_total`, `rows_retrieved_total`, `rows_used_map_total`, `rows_used_reduce_total`, `row_coverage_ratio`.

4. Не поднимается приложение из-за настроек.
Где смотреть: значения в `.env` (`DATABASE_URL`, `ALEMBIC_DATABASE_URL`, `JWT_SECRET_KEY`), ошибки старта в логах uvicorn/FastAPI, проверка БД через `docker compose -f docker-compose.db.yml ps`.

5. Модели недоступны/медленные ответы.
Где смотреть: `GET /api/v1/models/status`, `GET /api/v1/models/list?mode=...`, логи провайдера в backend (`app/services/llm/providers/*`), общие метрики задержек в `/metrics`.

## XLSX / CSV Settings (LangChain-first)
- `XLSX_CHUNK_MAX_CHARS`: adaptive char-budget для row-dense чанков.
- `XLSX_CHUNK_MAX_ROWS`: safety cap по строкам на chunk.
- `XLSX_MAX_COLUMNS_PER_CHUNK`: soft cap колонок для wide-листов (value-dense pruning).
- `FULL_FILE_MAP_MAX_TOKENS`: лимит map шага в structured full-file map/reduce.
- `RAG_FULL_FILE_MIN_ROW_COVERAGE`: порог row coverage для full-file.
- `RAG_FULL_FILE_ESCALATION_MAX_CHUNKS`: лимит repass-бюджета при low row coverage.
- `XLSX_CELL_MAX_CHARS`: optional cap на длину ячейки в chunk serialization (`0` = без cap).
- `OLLAMA_EMBED_MAX_INPUT_CHARS`: max размер одного embed-сегмента для local/Ollama.
- `OLLAMA_EMBED_SEGMENT_OVERLAP_CHARS`: overlap между embed-сегментами.
- Для `xlsx/xls/csv` ingestion создает sidecar SQLite dataset (`custom_metadata.tabular_sidecar`) для deterministic `tabular_sql` path.

## Chat Service Internals (2026-03-03)

`app/services/chat_orchestrator.py` is now a thin facade.
Most chat internals are split into `app/services/chat/*` modules:
- `language.py`
- `context.py`
- `embedding_config.py`
- `sources_debug.py`
- `full_file_analysis.py`
- `postprocess.py`
- `rag_prompt_builder.py`
- `retrieval_policy.py` (dynamic retrieval budget and low-coverage escalation)
- Stream and non-stream chat flows now share internal generation/post-processing steps in `ChatOrchestrator`.

This keeps HTTP/SSE behavior stable while reducing file size and improving testability.

## Target architecture direction

- Use `AI HUB` as the default and primary model provider for all chat/file analytics requests.
- Allow `Ollama (llama)` only as emergency fallback when `AI HUB` is unavailable and request waiting is not acceptable.
- Enforce policy-based model routing with circuit breaker and explicit route telemetry (`model_route`, `fallback_reason`).
- Keep dual-path answering: deterministic `tabular_sql/profile` for numeric truth, retrieval+LLM only for narrative analysis.
- Replace in-process ingestion worker with a durable queue/executor (idempotent jobs, retry policy, restart recovery).
- Evolve tabular storage from per-file SQLite sidecars to shared `DuckDB/Parquet` runtime for large `xlsx/csv`.
- Keep full-file coverage diagnostics as a hard quality gate (`rows_expected/retrieved/used`, coverage ratios, truncation flags).
- Implement unified observability + eval framework for offline contour (including fallback-rate and SLO alarms).
- Use [`docs/11_llm_file_chat_best_practices_architecture.md`](docs/11_llm_file_chat_best_practices_architecture.md) as the architecture baseline.
- Use [`docs/12_codex_cursor_prompts_offline_architecture.md`](docs/12_codex_cursor_prompts_offline_architecture.md) as implementation prompt pack for Cursor/Codex.

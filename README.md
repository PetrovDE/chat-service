# llama-service

Ð¡ÐµÑ€Ð²Ð¸Ñ Ð´Ð»Ñ Ð´Ð¸Ð°Ð»Ð¾Ð³Ð¾Ð² Ñ LLM (SSE/Ð¾Ð±Ñ‹Ñ‡Ð½Ñ‹Ð¹ Ð¾Ñ‚Ð²ÐµÑ‚), RAG Ð¿Ð¾ Ð·Ð°Ð³Ñ€ÑƒÐ¶ÐµÐ½Ð½Ñ‹Ð¼ Ñ„Ð°Ð¹Ð»Ð°Ð¼ Ð¸ ÑƒÐ¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð¸ÐµÐ¼ Ñ‡Ð°Ñ‚Ð°Ð¼Ð¸/Ñ„Ð°Ð¹Ð»Ð°Ð¼Ð¸ Ñ‡ÐµÑ€ÐµÐ· API Ð¸ Ð²ÑÑ‚Ñ€Ð¾ÐµÐ½Ð½Ñ‹Ð¹ web-frontend.

## Ð§Ñ‚Ð¾ Ð´ÐµÐ»Ð°ÐµÑ‚ ÑÐµÑ€Ð²Ð¸Ñ
- ÐŸÑ€Ð¸Ð½Ð¸Ð¼Ð°ÐµÑ‚ ÑÐ¾Ð¾Ð±Ñ‰ÐµÐ½Ð¸Ñ Ð² Ñ‡Ð°Ñ‚ (`/api/v1/chat`, `/api/v1/chat/stream`).
- ÐŸÐ¾Ð´Ð´ÐµÑ€Ð¶Ð¸Ð²Ð°ÐµÑ‚ Ð¿Ñ€Ð¾Ð²Ð°Ð¹Ð´ÐµÑ€Ñ‹ Ð¼Ð¾Ð´ÐµÐ»ÐµÐ¹: `ollama/local`, `aihub` (`corporate` alias), `openai`.
- Ð—Ð°Ð³Ñ€ÑƒÐ¶Ð°ÐµÑ‚ Ñ„Ð°Ð¹Ð»Ñ‹, Ð°ÑÐ¸Ð½Ñ…Ñ€Ð¾Ð½Ð½Ð¾ Ð¸Ð½Ð´ÐµÐºÑÐ¸Ñ€ÑƒÐµÑ‚ Ð¸Ñ… Ð² ChromaDB Ð¸ Ð¸ÑÐ¿Ð¾Ð»ÑŒÐ·ÑƒÐµÑ‚ Ð² RAG (`/api/v1/files/*`).
- Ð”Ð»Ñ `xlsx/xls/csv/tsv` Ð¿Ð¾Ð´Ð´ÐµÑ€Ð¶Ð¸Ð²Ð°ÐµÑ‚ table-aware ingestion (file/sheet summaries + row-groups) Ð¸ deterministic `tabular_sql` path Ð´Ð»Ñ aggregate/profile/lookup Ð²Ð¾Ð¿Ñ€Ð¾ÑÐ¾Ð².
- Ð”Ð»Ñ Ð°Ð½Ð°Ð»Ð¸Ñ‚Ð¸Ñ‡ÐµÑÐºÐ¸Ñ… Ð·Ð°Ð¿Ñ€Ð¾ÑÐ¾Ð² Ð¿Ð¾ Ñ„Ð°Ð¹Ð»Ñƒ Ð¿Ð¾Ð´Ð´ÐµÑ€Ð¶Ð¸Ð²Ð°ÐµÑ‚ deterministic `tabular_profile` (per-column SQL stats).
- Ð¥Ñ€Ð°Ð½Ð¸Ñ‚ Ð¿Ð¾Ð»ÑŒÐ·Ð¾Ð²Ð°Ñ‚ÐµÐ»ÐµÐ¹, Ñ‡Ð°Ñ‚Ñ‹, ÑÐ¾Ð¾Ð±Ñ‰ÐµÐ½Ð¸Ñ Ð¸ Ñ„Ð°Ð¹Ð»Ñ‹ Ð² PostgreSQL.
- ÐžÑ‚Ð´Ð°Ñ‘Ñ‚ Ð²ÑÑ‚Ñ€Ð¾ÐµÐ½Ð½Ñ‹Ð¹ frontend Ð¸Ð· `frontend/` (SPA Ð¼Ð¾Ð½Ñ‚Ð¸Ñ€ÑƒÐµÑ‚ÑÑ Ð½Ð° `/`).

## Ð‘Ñ‹ÑÑ‚Ñ€Ñ‹Ð¹ ÑÑ‚Ð°Ñ€Ñ‚
### 1) Backend
1. Ð£ÑÑ‚Ð°Ð½Ð¾Ð²Ð¸Ñ‚ÑŒ Ð·Ð°Ð²Ð¸ÑÐ¸Ð¼Ð¾ÑÑ‚Ð¸:
```bash
pip install -r requirements.txt
```
2. ÐŸÐ¾Ð´Ð½ÑÑ‚ÑŒ PostgreSQL (Ð»Ð¾ÐºÐ°Ð»ÑŒÐ½Ð¾ Ñ‡ÐµÑ€ÐµÐ· compose):
```bash
docker compose -f docker-compose.db.yml up -d
```
3. Ð—Ð°Ð¿Ð¾Ð»Ð½Ð¸Ñ‚ÑŒ `.env` Ð¼Ð¸Ð½Ð¸Ð¼ÑƒÐ¼Ð¾Ð¼:
```env
DATABASE_URL=postgresql+asyncpg://llama_chat_user:5432@localhost:5432/llama_chat_db
ALEMBIC_DATABASE_URL=postgresql://llama_chat_user:5432@localhost:5432/llama_chat_db
JWT_SECRET_KEY=change-me
```
4. ÐŸÑ€Ð¸Ð¼ÐµÐ½Ð¸Ñ‚ÑŒ Ð¼Ð¸Ð³Ñ€Ð°Ñ†Ð¸Ð¸:
```bash
alembic upgrade head
```
5. (ÐžÐ¿Ñ†Ð¸Ð¾Ð½Ð°Ð»ÑŒÐ½Ð¾) ÑÐ¾Ð·Ð´Ð°Ñ‚ÑŒ Ð°Ð´Ð¼Ð¸Ð½Ð¸ÑÑ‚Ñ€Ð°Ñ‚Ð¾Ñ€Ð°:
```bash
python scripts/create_admin.py
```
ÐŸÐ¾ ÑƒÐ¼Ð¾Ð»Ñ‡Ð°Ð½Ð¸ÑŽ ÑÐ¾Ð·Ð´Ð°Ñ‘Ñ‚ÑÑ `admin / admin123456`.
6. Ð—Ð°Ð¿ÑƒÑÑ‚Ð¸Ñ‚ÑŒ API:
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 2) Frontend
Frontend Ð¾Ñ‚Ð´ÐµÐ»ÑŒÐ½Ð¾Ð¹ ÑÐ±Ð¾Ñ€ÐºÐ¸ Ð½Ðµ Ñ‚Ñ€ÐµÐ±ÑƒÐµÑ‚.
- ÐžÑ‚ÐºÑ€Ñ‹Ñ‚ÑŒ: `http://localhost:8000/`
- API/Swagger: `http://localhost:8000/docs`
- Health: `http://localhost:8000/health`
- Prometheus metrics: `http://localhost:8000/metrics`

## ÐšÑ€Ð°Ñ‚ÐºÐ°Ñ ÑÑ…ÐµÐ¼Ð° ÐºÐ¾Ð¼Ð¿Ð¾Ð½ÐµÐ½Ñ‚Ð¾Ð²
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

## Ð”Ð¾ÐºÑƒÐ¼ÐµÐ½Ñ‚Ð°Ñ†Ð¸Ñ (`docs/*`)
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
14. [`docs/examples/status.response.partial_failed.json`](docs/examples/status.response.partial_failed.json)
15. [`docs/examples/status.response.processing.json`](docs/examples/status.response.processing.json)
16. [`docs/examples/upload.request.json`](docs/examples/upload.request.json)
17. [`docs/examples/upload.response.json`](docs/examples/upload.response.json)
18. [`docs/ux/chat_states.md`](docs/ux/chat_states.md)
19. [`docs/ux/file_ingestion_progress.md`](docs/ux/file_ingestion_progress.md)
20. [`docs/09_dynamic_rag_budget_plan.md`](docs/09_dynamic_rag_budget_plan.md)
21. [`docs/13_persistent_user_files_architecture.md`](docs/13_persistent_user_files_architecture.md)

## ÐÐºÑ‚ÑƒÐ°Ð»ÑŒÐ½Ñ‹Ðµ UI-Ð¸Ð·Ð¼ÐµÐ½ÐµÐ½Ð¸Ñ (frontend)
- Ð’ sidebar Ñ‡Ð°Ñ‚Ð¾Ð² ÑƒÐ´Ð°Ð»ÐµÐ½Ð¸Ðµ Ð²Ñ‹Ð½ÐµÑÐµÐ½Ð¾ Ð² Ð¾Ñ‚Ð´ÐµÐ»ÑŒÐ½ÑƒÑŽ ÐºÐ½Ð¾Ð¿ÐºÑƒ-ÐºÐ¾Ñ€Ð·Ð¸Ð½Ñƒ ÑÐ¿Ñ€Ð°Ð²Ð° Ð¾Ñ‚ ÐºÐ°Ð¶Ð´Ð¾Ð³Ð¾ Ñ‡Ð°Ñ‚Ð°.
- Ð’ `Settings` ÐºÐ½Ð¾Ð¿ÐºÐ° `Logout` Ñ€Ð°ÑÐ¿Ð¾Ð»Ð¾Ð¶ÐµÐ½Ð° Ð² Ñ„ÑƒÑ‚ÐµÑ€Ðµ Ñ€ÑÐ´Ð¾Ð¼ Ñ `Save` Ð¸ Ð¾Ñ„Ð¾Ñ€Ð¼Ð»ÐµÐ½Ð° ÐºÐ°Ðº danger-ÐºÐ½Ð¾Ð¿ÐºÐ°.
- Ð’ composer Ð¿Ð¾Ð»Ðµ Ð²Ð²Ð¾Ð´Ð° Ð¾ÑÑ‚Ð°Ð²Ð»ÐµÐ½Ð¾ ÑÐ²ÐµÑ€Ñ…Ñƒ, Ð° Ð½Ð¸Ð¶Ðµ Ñ€Ð°ÑÐ¿Ð¾Ð»Ð¾Ð¶ÐµÐ½ ÐµÐ´Ð¸Ð½Ñ‹Ð¹ Ñ€ÑÐ´ ÐºÐ¾Ð½Ñ‚Ñ€Ð¾Ð»Ð¾Ð²: `File + provider + chat model + embedding model + RAG mode + Send`.
- ÐŸÐ¾Ð´ ÑÐµÐ»ÐµÐºÑ‚Ð°Ð¼Ð¸ `provider/chat model/embedding model/rag mode` Ð¿Ð¾ÐºÐ°Ð·Ñ‹Ð²Ð°ÑŽÑ‚ÑÑ inline-Ð¿Ð¾Ð´ÑÐºÐ°Ð·ÐºÐ¸.
- `RAG debug` Ð¿Ð¾ÐºÐ°Ð·Ñ‹Ð²Ð°ÐµÑ‚ÑÑ Ð¿Ð¾Ð´ assistant-ÑÐ¾Ð¾Ð±Ñ‰ÐµÐ½Ð¸ÐµÐ¼ Ñ‚Ð¾Ð»ÑŒÐºÐ¾ ÐºÐ¾Ð³Ð´Ð° Ð¿Ð¾Ð»ÑŒÐ·Ð¾Ð²Ð°Ñ‚ÐµÐ»ÑŒ Ð²ÐºÐ»ÑŽÑ‡Ð¸Ð» ÑÑ‚Ð¾Ñ‚ Ñ„Ð»Ð°Ð³ Ð² `Settings`.

## Troubleshooting (Ñ‚Ð¾Ð¿-5)
1. `401/403` Ð½Ð° Ð·Ð°Ñ‰Ð¸Ñ‰Ñ‘Ð½Ð½Ñ‹Ñ… endpoint'Ð°Ñ… (`/api/v1/files/*`, `/api/v1/conversations/*`, `/api/v1/stats/*`).
Ð“Ð´Ðµ ÑÐ¼Ð¾Ñ‚Ñ€ÐµÑ‚ÑŒ: DevTools Network (Ð·Ð°Ð³Ð¾Ð»Ð¾Ð²Ð¾Ðº `Authorization`), backend-Ð»Ð¾Ð³Ð¸ Ñ `rid/uid` (stdout, Ñ„Ð¾Ñ€Ð¼Ð°Ñ‚ Ð¸Ð· `app/core/logging.py`).

2. Ð¤Ð°Ð¹Ð» Ð·Ð°Ð²Ð¸Ñ Ð² `processing` Ð¸Ð»Ð¸ ÑƒÑˆÑ‘Ð» Ð² `failed`.
Ð“Ð´Ðµ ÑÐ¼Ð¾Ñ‚Ñ€ÐµÑ‚ÑŒ: `GET /api/v1/files/{file_id}/status` (stage/counters/error), `GET /api/v1/stats/observability` (admin), backend-Ð»Ð¾Ð³Ð¸ `app/services/file.py`.

3. ÐŸÑƒÑÑ‚Ð¾Ð¹/ÑÐ»Ð°Ð±Ñ‹Ð¹ RAG-Ð¾Ñ‚Ð²ÐµÑ‚.
Ð“Ð´Ðµ ÑÐ¼Ð¾Ñ‚Ñ€ÐµÑ‚ÑŒ: Ð¾Ñ‚Ð¿Ñ€Ð°Ð²Ð¸Ñ‚ÑŒ chat Ñ `rag_debug=true` (Ð¸Ð»Ð¸ `?debug=true`), Ð¿Ñ€Ð¾Ð²ÐµÑ€Ð¸Ñ‚ÑŒ `rag_debug.top_chunks`, `filters/where`, `retrieval_mode`; ÑÑ€Ð°Ð²Ð½Ð¸Ñ‚ÑŒ Ñ `docs/examples/retrieve_debug.*.json`.
Ð•ÑÐ»Ð¸ Ñ„Ð°Ð¹Ð» Ñ‚Ð°Ð±Ð»Ð¸Ñ‡Ð½Ñ‹Ð¹ (`xlsx/xls/csv/tsv`) Ð¸ Ð² debug Ð²Ð¸Ð´Ð½Ð¾ Ð°Ð½Ð¾Ð¼Ð°Ð»ÑŒÐ½Ð¾ Ð¼Ð°Ð»Ð¾Ðµ `retrieved_chunks_total` Ð¿Ñ€Ð¸ Ð½Ð¾Ñ€Ð¼Ð°Ð»ÑŒÐ½Ð¾Ð¼ `chunks_count`, Ð¿Ñ€Ð¾Ð²ÐµÑ€ÑŒÑ‚Ðµ metadata chunk identity:
`chunk_id`, `chunk_index` (dedup ÐºÐ¾Ð½Ñ‚ÐµÐºÑÑ‚Ð° Ð²Ñ‹Ð¿Ð¾Ð»Ð½ÑÐµÑ‚ÑÑ Ð¿Ð¾ `chunk_id`, fallback: `file_id + chunk_index`).
Ð”Ð»Ñ `full_file` Ð´Ð¾Ð¿Ð¾Ð»Ð½Ð¸Ñ‚ÐµÐ»ÑŒÐ½Ð¾ Ð¿Ñ€Ð¾Ð²ÐµÑ€ÑŒÑ‚Ðµ row-level Ð¿Ð¾Ð»Ñ:
`rows_expected_total`, `rows_retrieved_total`, `rows_used_map_total`, `rows_used_reduce_total`, `row_coverage_ratio`.

4. ÐÐµ Ð¿Ð¾Ð´Ð½Ð¸Ð¼Ð°ÐµÑ‚ÑÑ Ð¿Ñ€Ð¸Ð»Ð¾Ð¶ÐµÐ½Ð¸Ðµ Ð¸Ð·-Ð·Ð° Ð½Ð°ÑÑ‚Ñ€Ð¾ÐµÐº.
Ð“Ð´Ðµ ÑÐ¼Ð¾Ñ‚Ñ€ÐµÑ‚ÑŒ: Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ Ð² `.env` (`DATABASE_URL`, `ALEMBIC_DATABASE_URL`, `JWT_SECRET_KEY`), Ð¾ÑˆÐ¸Ð±ÐºÐ¸ ÑÑ‚Ð°Ñ€Ñ‚Ð° Ð² Ð»Ð¾Ð³Ð°Ñ… uvicorn/FastAPI, Ð¿Ñ€Ð¾Ð²ÐµÑ€ÐºÐ° Ð‘Ð” Ñ‡ÐµÑ€ÐµÐ· `docker compose -f docker-compose.db.yml ps`.

5. ÐœÐ¾Ð´ÐµÐ»Ð¸ Ð½ÐµÐ´Ð¾ÑÑ‚ÑƒÐ¿Ð½Ñ‹/Ð¼ÐµÐ´Ð»ÐµÐ½Ð½Ñ‹Ðµ Ð¾Ñ‚Ð²ÐµÑ‚Ñ‹.
Ð“Ð´Ðµ ÑÐ¼Ð¾Ñ‚Ñ€ÐµÑ‚ÑŒ: `GET /api/v1/models/status`, `GET /api/v1/models/list?mode=...&capability=chat|embedding`, Ð»Ð¾Ð³Ð¸ Ð¿Ñ€Ð¾Ð²Ð°Ð¹Ð´ÐµÑ€Ð° Ð² backend (`app/services/llm/providers/*`), Ð¾Ð±Ñ‰Ð¸Ðµ Ð¼ÐµÑ‚Ñ€Ð¸ÐºÐ¸ Ð·Ð°Ð´ÐµÑ€Ð¶ÐµÐº Ð² `/metrics`.
Ð”Ð»Ñ AI HUB ÑÐ¿Ð¸ÑÐ¾Ðº capability-Ð¼Ð¾Ð´ÐµÐ»ÐµÐ¹ Ð¾Ð¿Ñ€ÐµÐ´ÐµÐ»ÑÐµÑ‚ÑÑ Ð¿Ð¾ `type` Ð¸Ð· Ð¾Ñ‚Ð²ÐµÑ‚Ð° Ð¿Ñ€Ð¾Ð²Ð°Ð¹Ð´ÐµÑ€Ð° (`chatbot`/`embedding`), Ð° catalog Ð¸ÑÐ¿Ð¾Ð»ÑŒÐ·ÑƒÐµÑ‚ÑÑ ÐºÐ°Ðº fallback ÐµÑÐ»Ð¸ discovery Ð½ÐµÐ´Ð¾ÑÑ‚ÑƒÐ¿ÐµÐ½.

## XLSX / CSV Settings (LangChain-first)
- `XLSX_CHUNK_MAX_CHARS`: adaptive char-budget Ð´Ð»Ñ row-dense Ñ‡Ð°Ð½ÐºÐ¾Ð².
- `XLSX_CHUNK_MAX_ROWS`: safety cap Ð¿Ð¾ ÑÑ‚Ñ€Ð¾ÐºÐ°Ð¼ Ð½Ð° chunk.
- `XLSX_MAX_COLUMNS_PER_CHUNK`: soft cap ÐºÐ¾Ð»Ð¾Ð½Ð¾Ðº Ð´Ð»Ñ wide-Ð»Ð¸ÑÑ‚Ð¾Ð² (value-dense pruning).
- `FULL_FILE_MAP_MAX_TOKENS`: Ð»Ð¸Ð¼Ð¸Ñ‚ map ÑˆÐ°Ð³Ð° Ð² structured full-file map/reduce.
- `RAG_FULL_FILE_MIN_ROW_COVERAGE`: Ð¿Ð¾Ñ€Ð¾Ð³ row coverage Ð´Ð»Ñ full-file.
- `RAG_FULL_FILE_ESCALATION_MAX_CHUNKS`: Ð»Ð¸Ð¼Ð¸Ñ‚ repass-Ð±ÑŽÐ´Ð¶ÐµÑ‚Ð° Ð¿Ñ€Ð¸ low row coverage.
- `XLSX_CELL_MAX_CHARS`: optional cap Ð½Ð° Ð´Ð»Ð¸Ð½Ñƒ ÑÑ‡ÐµÐ¹ÐºÐ¸ Ð² chunk serialization (`0` = Ð±ÐµÐ· cap).
- `TABULAR_ROW_GROUP_ROWS_NARROW / MEDIUM / WIDE`: Ð´Ð¸Ð½Ð°Ð¼Ð¸Ñ‡ÐµÑÐºÐ¸Ð¹ Ñ€Ð°Ð·Ð¼ÐµÑ€ row-group Ñ‡Ð°Ð½ÐºÐ¾Ð².
- `TABULAR_MAX_EMBEDDING_DOCS`: hard cap Ð½Ð° Ñ‡Ð¸ÑÐ»Ð¾ embedding Ð´Ð¾ÐºÑƒÐ¼ÐµÐ½Ñ‚Ð¾Ð² Ð´Ð»Ñ huge Ñ‚Ð°Ð±Ð»Ð¸Ñ†.
- `TABULAR_WIDE_CELL_HARD_LIMIT`: hard cap Ð´Ð»Ñ ÑÐ»Ð¸ÑˆÐºÐ¾Ð¼ Ð´Ð»Ð¸Ð½Ð½Ñ‹Ñ… Ñ‚ÐµÐºÑÑ‚Ð¾Ð²Ñ‹Ñ… ÑÑ‡ÐµÐµÐº (ÐµÑÐ»Ð¸ `XLSX_CELL_MAX_CHARS=0`).
- `OLLAMA_EMBED_MAX_INPUT_CHARS`: max Ñ€Ð°Ð·Ð¼ÐµÑ€ Ð¾Ð´Ð½Ð¾Ð³Ð¾ embed-ÑÐµÐ³Ð¼ÐµÐ½Ñ‚Ð° Ð´Ð»Ñ local/Ollama.
- `OLLAMA_EMBED_SEGMENT_OVERLAP_CHARS`: overlap Ð¼ÐµÐ¶Ð´Ñƒ embed-ÑÐµÐ³Ð¼ÐµÐ½Ñ‚Ð°Ð¼Ð¸.
- Ð”Ð»Ñ `xlsx/xls/csv/tsv` ingestion ÑÐ¾Ñ…Ñ€Ð°Ð½ÑÐµÑ‚ `tabular_dataset` (shared DuckDB/Parquet runtime) Ð´Ð»Ñ deterministic `tabular_sql` path.

## Ingestion Status Lifecycle
- `uploaded -> queued -> parsing -> parsed -> chunking -> embedding -> indexing -> completed`
- Ð§Ð°ÑÑ‚Ð¸Ñ‡Ð½Ñ‹Ð¹ ÑƒÑÐ¿ÐµÑ…: `partial_failed`
- Ð¤Ð°Ñ‚Ð°Ð»ÑŒÐ½Ð°Ñ Ð¾ÑˆÐ¸Ð±ÐºÐ°: `failed`
- `completed` Ð²Ñ‹ÑÑ‚Ð°Ð²Ð»ÑÐµÑ‚ÑÑ Ñ‚Ð¾Ð»ÑŒÐºÐ¾ Ð¿Ñ€Ð¸ Ð¿Ð¾Ð»Ð½Ð¾Ð¹ ÐºÐ¾Ð½ÑÐ¸ÑÑ‚ÐµÐ½Ñ‚Ð½Ð¾ÑÑ‚Ð¸ expected/processed/indexed/upsert counters.

## RAG Debug Quick Checks
- `retrieval_path`: `vector` Ð¸Ð»Ð¸ `structured`
- `top_chunks[*].chunk_type`: `file_summary | sheet_summary | row_group | ...`
- `top_similarity_scores`: Ð²ÐµÑ€Ñ…Ð½Ð¸Ðµ similarity Ð·Ð½Ð°Ñ‡ÐµÐ½Ð¸Ñ
- `context_tokens`: Ð¾Ñ†ÐµÐ½ÐºÐ° Ñ„Ð°ÐºÑ‚Ð¸Ñ‡ÐµÑÐºÐ¸ Ð¾Ñ‚Ð¿Ñ€Ð°Ð²Ð»ÐµÐ½Ð½Ð¾Ð³Ð¾ ÐºÐ¾Ð½Ñ‚ÐµÐºÑÑ‚Ð° Ð² LLM

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
- Tabular storage now uses shared `DuckDB/Parquet` runtime for large `xlsx/csv` (no SQLite sidecar fallback in active backend flow).
- Keep full-file coverage diagnostics as a hard quality gate (`rows_expected/retrieved/used`, coverage ratios, truncation flags).
- Implement unified observability + eval framework for offline contour (including fallback-rate and SLO alarms).
- Use [`docs/11_llm_file_chat_best_practices_architecture.md`](docs/11_llm_file_chat_best_practices_architecture.md) as the architecture baseline.
- Use [`docs/12_codex_cursor_prompts_offline_architecture.md`](docs/12_codex_cursor_prompts_offline_architecture.md) as implementation prompt pack for Cursor/Codex.

## Embedding Defaults (2026-03-23)
- Embedding model resolution is provider-aware and capability-aware (chat and embedding are resolved independently).
- AI HUB default embedding model: `qwen3-emb`.
- Local/Ollama default embedding model: `OLLAMA_EMBED_MODEL` (by default `nomic-embed-text:latest`), with optional capability-based runtime match from available local models.
- `arctic` remains available as explicit AI HUB embedding override (`embedding_model=arctic`).
- Embedding dimension source-of-truth is model-aware (not provider-wide):
  - configured metadata map: `EMBEDDING_MODEL_DIMENSIONS` (for example `aihub:qwen3-emb=4096`),
  - runtime observed cache per `(provider, embedding_model)` when metadata is absent,
  - deprecated global fallback: `EMBEDDINGS_DIM` (used only when model metadata is unavailable).
- Dimension validation now compares `actual` against expected dimension resolved for the active embedding model; mismatch raises a clear runtime error with provider/model/source details.
- Invalid embedding overrides that point to chat-only models (for example `llama3.2:latest`) do not trigger cross-provider fallback and are resolved only within the selected provider policy.
- Provider routing for embeddings is strict:
  - `local/ollama` -> Ollama only,
  - `aihub` -> AI HUB only,
  - no hidden cross-provider fallback.
- Upload/reprocess performs embedding preflight validation and returns `422` for auth/config/model-availability errors.
- New vectors are written into model-scoped collections (`<base>_<dim>d_<mode>_<model>_<hash>`), so different embedding spaces (provider/model) are not mixed.
- `GET /api/v1/models/list` provider-aware behavior:
  - `capability=chat` and `capability=embedding` are resolved independently,
  - AI HUB capability filtering uses provider model `type` (`chatbot` vs `embedding`),
  - if AI HUB discovery is unavailable, configured catalog is used as fallback.

## Time Rendering Consistency (2026-03-12)
- File timestamps and message timestamps now use the same frontend time parsing/formatting path (`frontend/static/js/time-format.js`).
- Backend serialization for conversation messages and file timestamps is normalized to UTC-aware ISO (`+00:00`), then rendered in client local timezone.
- This removes file-menu UTC/GMT-like drift relative to message timestamps.

## Prompt Max Chars Behavior (2026-03-12)
- Chat API request field `prompt_max_chars` now accepts `1000..500000`.
- AI HUB still applies provider-side cap `AIHUB_MAX_PROMPT_CHARS` (default `50000`), so request values above this are clamped for AI HUB calls.
- To effectively raise AI HUB prompt size, increase server setting `AIHUB_MAX_PROMPT_CHARS` (not only UI request value).
- RAG/provider debug now includes `prompt_chars_requested`, `prompt_chars_configured`, `prompt_chars_limit`, `prompt_chars_before`, `prompt_chars_after`, `prompt_truncated`.

## Persistent User Files Architecture (2026-03-23)
- File model was rebuilt to persistent user-owned artifacts.
- Raw file is persisted first and is independent from chat lifecycle.
- One file can be attached to many chats via `chat_file_links`.
- Per-user raw storage quota is enforced: `1 GB`.
- Processing is explicitly versioned in `file_processing_profiles`:
  - `pipeline_version`, `parser_version`, `artifact_version`,
  - `embedding_provider`, `embedding_model`, `embedding_dimension`,
  - `chunking_strategy`, `retrieval_profile`, `is_active`.
- Reprocess/reindex creates a new processing version and activates it on success.
- Delete lifecycle removes chat links, vectors, artifacts, raw file, and frees quota.

Detailed reference:
- [`docs/13_persistent_user_files_architecture.md`](docs/13_persistent_user_files_architecture.md)

## Frontend File UX (2026-03-23)
- Composer upload now targets persistent user file library (not one-shot attachment memory).
- Active chat panel shows files linked to current chat and supports:
  - attach existing library file,
  - detach from current chat,
  - delete and reprocess actions.
- Right sidebar is now `My files` library with quota and file details/debug panel.
- File statuses (`uploaded/processing/ready/failed/deleting/deleted`) are rendered in both:
  - file library,
  - current chat file panel.
- Upload can auto-attach to currently open chat via `chat_id`.
- Chat payload uses persisted chat links (`getCurrentChatFileIds`) instead of temporary `attachedFiles`.
- UX hardening pass adds:
  - optimistic attach/detach with rollback,
  - clearer library-vs-chat hints (`Upload to library`, `Files in this chat`),
  - debug-only technical identifiers (`file_id`, `processing_id`),
  - cleanup of unused legacy frontend modules (`file-manager`, `auth-ui`, `conversations-ui`, `utils`).

## Table-Aware RAG Refactor (2026-03-23)
- Ingestion now follows `raw file -> derived artifacts -> selective indexing`.
- Derived artifacts are persisted per processing version:
  - `runtime/file_artifacts/<file_id>/<processing_id>/manifest.json`
- CSV/TSV parsing now records:
  - encoding, delimiter, header detection, inferred types, preview rows.
- XLSX parsing now records:
  - workbook/sheet summaries, inferred types, preview rows, row windows.
- Retrieval routing now exposes explicit strategy modes:
  - `semantic`, `analytical`, `combined`.
- Combined mode performs:
  - semantic prefetch on tabular artifacts,
  - then deterministic SQL on selected scope.
- Full-file keyword hack was removed:
  - no implicit full-file retrieval from query keywords.
  - full-file is explicit (`rag_mode=full_file`) or policy escalation.
- Vector metadata propagation now includes:
  - `owner_user_id`, `processing_id`, `artifact_type`, `source_type`,
  - `pipeline_version`, `parser_version`, `artifact_version`,
  - `embedding_model`, `embedding_dimension`.
- RAG debug payload was extended with:
  - `strategy_mode`, `analytical_mode_used`, `active_processing_ids`, `avg_similarity`.

See:
- [`docs/04_ingestion_pipeline.md`](docs/04_ingestion_pipeline.md)
- [`docs/03_rag_pipeline.md`](docs/03_rag_pipeline.md)

## Runtime Storage Layout (inside service folder)
- `runtime/raw_files/`
- `runtime/temp_uploads/`
- `runtime/file_artifacts/`
- `runtime/public/uploads/`
- `runtime/vector/chromadb/`
- `runtime/queue/.ingestion_jobs.sqlite3`
- `runtime/tabular_runtime/`

Runtime data is git-ignored (`.gitignore` includes `/runtime/` and related paths).

## Breaking Changes (Files API)
- Removed: `POST /api/v1/files/process/{file_id}`
- Use: `POST /api/v1/files/{file_id}/reprocess`
- File status route is now: `GET /api/v1/files/{file_id}/status`
- File API is chat-link based (`chat_id`) and user-owned, not attachment-temporary.


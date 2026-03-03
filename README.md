# llama-service

Сервис для диалогов с LLM (SSE/обычный ответ), RAG по загруженным файлам и управлением чатами/файлами через API и встроенный web-frontend.

## Что делает сервис
- Принимает сообщения в чат (`/api/v1/chat`, `/api/v1/chat/stream`).
- Поддерживает провайдеры моделей: `ollama/local`, `aihub` (`corporate` alias), `openai`.
- Загружает файлы, асинхронно индексирует их в ChromaDB и использует в RAG (`/api/v1/files/*`).
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
DATABASE_URL=postgresql+asyncpg://llama_chat_user:1306@localhost:5432/llama_chat_db
ALEMBIC_DATABASE_URL=postgresql://llama_chat_user:1306@localhost:5432/llama_chat_db
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

4. Не поднимается приложение из-за настроек.
Где смотреть: значения в `.env` (`DATABASE_URL`, `ALEMBIC_DATABASE_URL`, `JWT_SECRET_KEY`), ошибки старта в логах uvicorn/FastAPI, проверка БД через `docker compose -f docker-compose.db.yml ps`.

5. Модели недоступны/медленные ответы.
Где смотреть: `GET /api/v1/models/status`, `GET /api/v1/models/list?mode=...`, логи провайдера в backend (`app/services/llm/providers/*`), общие метрики задержек в `/metrics`.

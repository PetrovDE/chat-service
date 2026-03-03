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

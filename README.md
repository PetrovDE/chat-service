# LLaMA Chat Service

AI-powered chat service with Retrieval Augmented Generation (RAG), поддержка локальных (Ollama) и внешних (OpenAI) LLM, файловый storage, мультипользовательские чаты и расширяемый API.

---

## 📋 Requirements

- Python 3.10+
- PostgreSQL 14+
- Ollama (for local LLM)
- (Опционально) OpenAI API key

---

## 🚀 Quick Start

1. **Install dependencies**
    ```
    pip install -r requirements.txt
    ```

2. **Set up environment**
    ```
    # Копировать и настроить переменные окружения
    cp .env.example .env
    # Пример содержимого .env:
    # DATABASE_URL=postgresql+asyncpg://user:password@localhost/llama_db
    # JWT_SECRET_KEY=your-secret-key-change-this
    # EMBEDDINGS_BASEURL=http://localhost:11434
    # OPENAI_API_KEY=sk-xxx (опционально)
    # etc.
    ```

3. **Initialize database**
    ```
    # Создать таблицы
    python scripts/init_db.py

    # Создать администратора
    python scripts/create_admin.py
    ```

4. **Run server**
    ```
    uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
    ```

---

## 📚 API Documentation

- Swagger UI: [http://localhost:8000/docs](http://localhost:8000/docs)
- ReDoc: [http://localhost:8000/redoc](http://localhost:8000/redoc)

---

## 🏗 Project Structure
```
app/
├── api/
│ └── v1/
│ ├── endpoints/ # API endpoints (auth, chat, files, etc.)
│ └── router.py # Main API router
├── core/ # Core configuration
│ ├── config.py # Settings and environment variables
│ ├── security.py # JWT and password hashing
│ ├── logging.py # Logging configuration
│ └── exceptions.py # Custom exceptions
├── crud/ # Database CRUD operations
│ ├── conversation.py # Conversation operations
│ ├── message.py # Message operations
│ ├── user.py # User operations
│ └── file.py # File operations
├── db/ # Database layer
│ ├── models/ # SQLAlchemy models
│ ├── session.py # Database session management
│ └── base.py # Base model
├── schemas/ # Pydantic schemas
│ ├── chat.py # Chat request/response schemas
│ ├── user.py # User schemas
│ ├── conversation.py # Conversation schemas
│ └── file.py # File schemas
├── services/ # Business logic
│ ├── llm/ # LLM management
│ │ ├── manager.py # Main LLM manager
│ │ └── providers/ # Provider implementations
│ ├── chat.py # Chat service
│ ├── file.py # File service
│ └── stats.py # Statistics service
├── rag/ # Retrieval Augmented Generation
│ ├── embeddings.py # Embedding generation
│ ├── retriever.py # Document retrieval
│ ├── vector_store.py # Vector database operations
│ ├── document_loader.py # Document loading and parsing
│ └── text_splitter.py # Text chunking
├── utils/ # Utility functions
└── main.py # FastAPI application entry point

frontend/
├── static/
│ ├── css/ # Stylesheets
│ ├── js/ # JavaScript modules
│ └── index.html # Main HTML page

scripts/
├── init_db.py # Database initialization
└── create_admin.py # Admin user creation

alembic/ # Database migrations
├── versions/ # Migration files
└── env.py # Alembic configuration
```
---

## 🔑 Default Admin

- Username: `admin`
- Password: `admin123456`

⚠️ **Не забудьте сменить пароль администратора после первого запуска!**

---

## 🌐 Главные возможности

- Асинхронная работа FastAPI на Python 3.10+
- Поддержка сетевых и локальных LLM: Ollama (через API) и OpenAI (GPT-4, 3.5-turbo)
- Retrieval Augmented Generation (файловый и личный RAG, векторные БД)
- Режим стриминга (Server-Sent Events)
- Многоуровневая аутентификация (JWT)
- Управление файлами (загрузка, чтение, прослеживание статуса embeddings)
- Гибкая маршрутизация и DI через Depends
- Логирование и кастомные обработчики ошибок
- Swagger/OpenAPI по умолчанию

---

## 📑 Основные Endpoint API

| Метод | Endpoint                        | Описание                                             | Авторизация    |
|-------|----------------------------------|------------------------------------------------------|----------------|
| POST  | /api/v1/chat/stream              | Стриминговый чат c RAG                               | Необязательно  |
| POST  | /api/v1/chat/                    | Классический чат завершённый (json ответ)            | Необязательно  |
| POST  | /api/v1/auth/login               | Вход (получить JWT)                                 | Нет            |
| POST  | /api/v1/auth/register            | Регистрация пользователя                            | Нет            |
| GET   | /api/v1/auth/me                  | Инфо о пользователе                                 | Да             |
| GET   | /api/v1/models/                  | Получить все доступные языковые модели               | Нет            |
| POST  | /api/v1/files/upload             | Загрузка файлов пользователя                        | Да             |
| GET   | /api/v1/files/                   | Список файлов пользователя                          | Да             |
| GET   | /api/v1/files/{file_id}          | Получить инфо/статус определённого файла             | Да             |
| POST  | /api/v1/files/process/{file_id}  | Запустить обработку файла для embeddings             | Да             |
| DELETE| /api/v1/files/{file_id}          | Удаление файла пользователя                         | Да             |
| GET   | /api/v1/conversations/           | Список всех диалогов пользователя                   | Да             |
| POST  | /api/v1/conversations/           | Создать новый диалог                                 | Да             |
| GET   | /api/v1/conversations/{conv_id}  | Получить сообщения диалога                          | Да             |
| GET   | /api/v1/stats/                   | Получить статистику по чатам/файлам                  | Да             |

> Примеры можно видеть в Swagger UI — структура моделей описана автоматически.

---

## 🗂 Пример .env

Database
DATABASE_URL=postgresql+asyncpg://user:password@localhost/llama_db

Secret keys
JWT_SECRET_KEY=your-secret-key-change-this

LLM и Embeddings
EMBEDDINGS_BASEURL=http://localhost:11434
EMBEDDINGS_MODEL=llama3

OpenAI (опционально)
OPENAI_API_KEY=sk-xxxxxx
OPENAI_MODEL=gpt-4

Application
ALLOWED_ORIGINS=*

text

---

## ☑️ Примеры запросов

**Чат (stream):**
curl -X POST http://localhost:8000/api/v1/chat/stream
-H "Authorization: Bearer <your_JWT_here>"
-H "Content-Type: application/json"
-d '{"message": "Привет!", "model_source": "ollama", "model_name": "llama3"}'

text

**Загрузка файла:**
curl -X POST http://localhost:8000/api/v1/files/upload
-H "Authorization: Bearer <your_JWT_here>"
-F "file=@mydoc.pdf"

text

**Получить список моделей:**
curl http://localhost:8000/api/v1/models/

text

---

## 📒 Особенности

- **RAG**: Можно загружать файлы (pdf, txt, docx, xlsx), после их индексации — использовать как знания для чата.
- **LLM-Выбор**: Можно явно передавать model_source/model_name или использовать default.
- **История и восстановление**: Все сообщения хранятся построчно, последовательность сохраняется для каждой сессии.

---

## 📝 License

MIT
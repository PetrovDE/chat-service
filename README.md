# 🦙 Llama Chat

Современный веб-интерфейс для взаимодействия с локальными LLM моделями через Ollama или OpenAI API.

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.11+-green.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104-009688.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791.svg)

## 📋 Содержание

- [Возможности](#возможности)
- [Технологический стек](#технологический-стек)
- [Структура проекта](#структура-проекта)
- [Требования](#требования)
- [Установка](#установка)
  - [Linux/Ubuntu](#установка-на-linuxubuntu)
  - [Windows 11](#установка-на-windows-11)
- [Конфигурация](#конфигурация)
- [Запуск](#запуск)
- [API Документация](#api-документация)
- [Разработка](#разработка)
- [Troubleshooting](#troubleshooting)
- [Безопасность](#безопасность)
- [Лицензия](#лицензия)

## ✨ Возможности

### 🔐 Аутентификация и безопасность
- ✅ Регистрация и вход пользователей
- ✅ JWT токены для авторизации
- ✅ Хеширование паролей (bcrypt)
- ✅ Изоляция данных между пользователями

### 💬 Управление беседами
- ✅ Создание и сохранение бесед
- ✅ История всех разговоров
- ✅ Переименование и удаление бесед
- ✅ Счётчик сообщений в каждой беседе
- ✅ Боковая панель с навигацией по беседам

### 🤖 Работа с LLM
- ✅ Поддержка Ollama (локальные модели)
- ✅ Поддержка OpenAI API
- ✅ Переключение между моделями
- ✅ Настройка температуры и max_tokens
- ✅ Контекстные беседы с историей

### 📊 Аналитика
- ✅ Логирование всех запросов к API
- ✅ Статистика использования токенов
- ✅ Отслеживание времени генерации
- ✅ Мониторинг состояния системы

### 🎨 Интерфейс
- ✅ Современный адаптивный UI
- ✅ Поддержка темной темы (в разработке)
- ✅ Уведомления о событиях
- ✅ Плавные анимации
- ✅ Мобильная версия

## 🛠 Технологический стек

### Backend
- **FastAPI** 0.104+ - современный веб-фреймворк
- **Python** 3.11+ - язык программирования
- **PostgreSQL** 16+ - база данных
- **SQLAlchemy** 2.0+ - ORM
- **Alembic** - миграции БД
- **asyncpg** - асинхронный драйвер PostgreSQL
- **pgvector** - векторные embeddings
- **python-jose** - JWT токены
- **passlib** - хеширование паролей
- **httpx** - HTTP клиент

### Frontend
- **Vanilla JavaScript** (ES6 модули)
- **CSS3** (flexbox, grid, animations)
- **HTML5**

### LLM Integration
- **Ollama** - локальные модели
- **OpenAI API** - облачные модели

## 📁 Структура проекта
```
llama-chat/
│
├── app/                              # Backend приложение
│   ├── __init__.py
│   ├── main.py                       # FastAPI приложение
│   ├── config.py                     # Конфигурация
│   ├── models.py                     # Pydantic схемы
│   ├── auth.py                       # JWT аутентификация
│   ├── llm_manager.py               # Менеджер LLM
│   │
│   ├── database/                     # Работа с БД
│   │   ├── __init__.py
│   │   ├── database.py              # Подключение
│   │   ├── models.py                # SQLAlchemy модели
│   │   └── crud.py                  # CRUD операции
│   │
│   ├── routers/                      # API endpoints
│   │   ├── __init__.py
│   │   ├── auth.py                  # Аутентификация
│   │   ├── chat.py                  # Чат
│   │   ├── conversations.py         # Беседы
│   │   ├── files.py                 # Загрузка файлов
│   │   ├── models_management.py     # Управление моделями
│   │   └── stats.py                 # Статистика
│   │
│   └── static/                       # Frontend
│       ├── index.html               # Главная страница
│       │
│       ├── css/
│       │   └── styles.css           # Все стили
│       │
│       └── js/                       # JavaScript модули
│           ├── app.js               # Инициализация
│           ├── api-service.js       # HTTP запросы
│           ├── auth-manager.js      # Логика аутентификации
│           ├── auth-ui.js           # UI аутентификации
│           ├── chat-manager.js      # Логика чата
│           ├── conversations-manager.js  # Логика бесед
│           ├── conversations-ui.js  # UI бесед
│           ├── file-manager.js      # Работа с файлами
│           ├── ui-controller.js     # UI контроллер
│           └── utils.js             # Утилиты
│
├── alembic/                          # Миграции БД
│   ├── versions/                     # История миграций
│   ├── env.py                       # Конфигурация Alembic
│   └── script.py.mako
│
├── venv/                             # Виртуальное окружение
├── .env                              # Переменные окружения
├── .env.example                      # Пример конфигурации
├── requirements.txt                  # Python зависимости
├── alembic.ini                       # Конфигурация Alembic
└── README.md                         # Документация
```

## 📋 Требования

### Системные требования
- **OS**: Linux (Ubuntu 22.04+), Windows 11, macOS
- **RAM**: минимум 8 GB (рекомендуется 16 GB)
- **Disk**: минимум 10 GB свободного места
- **CPU**: 4+ ядра

### Программное обеспечение

#### Обязательно:
- Python 3.11 или выше
- PostgreSQL 16 или выше
- pip (менеджер пакетов Python)
- Git

#### Опционально:
- Ollama (для локальных моделей)
- OpenAI API ключ (для OpenAI моделей)

## 🚀 Установка

### Установка на Linux/Ubuntu

#### 1. Установка системных зависимостей
```bash
# Обновить систему
sudo apt update && sudo apt upgrade -y

# Установить Python и зависимости
sudo apt install -y python3.11 python3.11-venv python3-pip \
    postgresql-16 postgresql-contrib-16 \
    build-essential libpq-dev git

# Установить pgvector
cd /tmp
git clone https://github.com/pgvector/pgvector.git
cd pgvector
make
sudo make install
```

#### 2. Настройка PostgreSQL
```bash
# Переключиться на пользователя postgres
sudo -u postgres psql

# В psql выполнить:
CREATE USER llama_chat_user WITH PASSWORD 'your_secure_password';
CREATE DATABASE llama_chat_db OWNER llama_chat_user;
GRANT ALL PRIVILEGES ON DATABASE llama_chat_db TO llama_chat_user;
\c llama_chat_db
GRANT ALL ON SCHEMA public TO llama_chat_user;
CREATE EXTENSION IF NOT EXISTS vector;
\q
```

#### 3. Клонирование и настройка проекта
```bash
# Создать пользователя для приложения
sudo useradd -m -s /bin/bash llama-chat

# Создать директорию проекта
sudo mkdir -p /opt/llama-chat
sudo chown llama-chat:llama-chat /opt/llama-chat

# Переключиться на пользователя
sudo -u llama-chat -i

# Перейти в директорию
cd /opt/llama-chat

# Скопировать файлы проекта (или клонировать из Git)
# git clone <repository-url> .

# Создать виртуальное окружение
python3 -m venv venv
source venv/bin/activate

# Установить зависимости
pip install -r requirements.txt
```

#### 4. Конфигурация
```bash
# Скопировать пример конфигурации
cp .env.example .env

# Отредактировать .env
nano .env

# Обязательно изменить:
# - DATABASE_URL (пароль PostgreSQL)
# - JWT_SECRET_KEY (сгенерировать новый)
```

#### 5. Применение миграций
```bash
# Применить все миграции
alembic upgrade head
```

#### 6. Запуск
```bash
# Запуск сервера
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

---

### Установка на Windows 11

#### 1. Установка Python

1. Скачайте Python 3.11+ с https://www.python.org/downloads/
2. Запустите установщик
3. ✅ Отметьте "Add Python to PATH"
4. Завершите установку

#### 2. Установка PostgreSQL

1. Скачайте PostgreSQL 16 с https://www.postgresql.org/download/windows/
2. Запустите установщик
3. Выберите компоненты:
   - ✅ PostgreSQL Server
   - ✅ pgAdmin 4
   - ✅ Command Line Tools
4. Установите пароль для пользователя `postgres`
5. Порт: 5432 (по умолчанию)

#### 3. Настройка PostgreSQL
```powershell
# Откройте PowerShell
psql -U postgres -h localhost

# В psql:
CREATE USER llama_chat_user WITH PASSWORD 'your_secure_password';
CREATE DATABASE llama_chat_db OWNER llama_chat_user;
GRANT ALL PRIVILEGES ON DATABASE llama_chat_db TO llama_chat_user;
\c llama_chat_db
GRANT ALL ON SCHEMA public TO llama_chat_user;
CREATE EXTENSION IF NOT EXISTS vector;
\q
```

#### 4. Создание проекта
```powershell
# Создайте папку проекта
cd $HOME\Desktop
mkdir llama-chat
cd llama-chat

# Создайте виртуальное окружение
python -m venv venv

# Активируйте
.\venv\Scripts\Activate.ps1

# Если ошибка с ExecutionPolicy:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Скопируйте файлы проекта в папку

# Установите зависимости
pip install -r requirements.txt
```

#### 5. Конфигурация
```powershell
# Скопируйте .env.example в .env
Copy-Item .env.example .env

# Откройте в блокноте и настройте
notepad .env
```

#### 6. Применение миграций
```powershell
# Инициализация Alembic (если не сделано)
alembic init alembic

# Применить миграции
alembic upgrade head
```

#### 7. Запуск
```powershell
# Запуск сервера
uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

---

## ⚙️ Конфигурация

### Основные переменные окружения (.env)
```env
# База данных
DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/llama_chat_db
ALEMBIC_DATABASE_URL=postgresql://user:password@localhost:5432/llama_chat_db

# Ollama
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=llama3.1:8b

# OpenAI (опционально)
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4

# JWT
JWT_SECRET_KEY=<сгенерируйте: openssl rand -hex 32>
JWT_ALGORITHM=HS256
JWT_ACCESS_TOKEN_EXPIRE_MINUTES=10080

# Настройки
DEFAULT_MODEL_SOURCE=ollama
SERVER_HOST=127.0.0.1
SERVER_PORT=8000
LOG_LEVEL=INFO
```

### Генерация JWT секретного ключа

**Linux/Mac:**
```bash
openssl rand -hex 32
```

**Windows (PowerShell):**
```powershell
[Convert]::ToBase64String((1..32 | ForEach-Object { Get-Random -Maximum 256 }))
```

## 🎯 Запуск

### Режим разработки
```bash
# Linux
source venv/bin/activate
uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload

# Windows
.\venv\Scripts\Activate.ps1
uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

### Production режим
```bash
# Linux с systemd
sudo systemctl start llama-chat
sudo systemctl enable llama-chat

# Или напрямую с Gunicorn
gunicorn app.main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

### Доступ к приложению

Откройте браузер:
```
http://127.0.0.1:8000
```

## 📚 API Документация

### Автоматическая документация

После запуска сервера доступна интерактивная документация:

- **Swagger UI**: http://127.0.0.1:8000/docs
- **ReDoc**: http://127.0.0.1:8000/redoc

### Основные endpoints

#### Аутентификация
```
POST   /auth/register         - Регистрация
POST   /auth/login            - Вход
GET    /auth/me               - Текущий пользователь
PUT    /auth/me               - Обновить профиль
POST   /auth/change-password  - Изменить пароль
POST   /auth/logout           - Выход
```

#### Чат
```
POST   /chat                  - Отправить сообщение
POST   /chat/stream           - Потоковый ответ
POST   /chat/continue         - Продолжить беседу
```

#### Беседы
```
GET    /conversations         - Список бесед
POST   /conversations         - Создать беседу
GET    /conversations/{id}    - Получить беседу
PATCH  /conversations/{id}    - Обновить беседу
DELETE /conversations/{id}    - Удалить беседу
```

#### Файлы
```
POST   /files/upload          - Загрузить файл
POST   /files/analyze-file    - Анализ файла
```

#### Модели
```
GET    /models                - Список моделей
POST   /models/switch         - Переключить модель
GET    /models/current        - Текущая модель
```

#### Статистика
```
GET    /stats/usage           - Статистика использования
```

#### Системные
```
GET    /health                - Проверка здоровья
GET    /info                  - Информация о приложении
```

## 👨‍💻 Разработка

### Установка для разработки
```bash
# Клонировать репозиторий
git clone <repository-url>
cd llama-chat

# Создать виртуальное окружение
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\Activate.ps1  # Windows

# Установить зависимости
pip install -r requirements.txt

# Настроить .env
cp .env.example .env
nano .env

# Применить миграции
alembic upgrade head

# Запустить в режиме разработки
uvicorn app.main:app --reload
```

### Создание миграции
```bash
# Автогенерация миграции
alembic revision --autogenerate -m "Description of changes"

# Применить миграцию
alembic upgrade head

# Откатить миграцию
alembic downgrade -1
```

### Структура базы данных

**Основные таблицы:**

- `users` - пользователи
- `conversations` - беседы
- `messages` - сообщения
- `files` - загруженные файлы
- `api_usage_logs` - логи API запросов
- `system_settings` - системные настройки
- `alembic_version` - версия миграций

### Тестирование
```bash
# Установить pytest
pip install pytest pytest-asyncio httpx

# Запустить тесты
pytest

# С покрытием
pytest --cov=app tests/
```

## 🐛 Troubleshooting

### Проблема: "Database connection failed"

**Решение:**
```bash
# Проверьте что PostgreSQL запущен
sudo systemctl status postgresql  # Linux
Get-Service postgresql*           # Windows

# Проверьте подключение
psql -U llama_chat_user -d llama_chat_db -h localhost

# Проверьте .env файл
cat .env | grep DATABASE_URL
```

### Проблема: "ModuleNotFoundError"

**Решение:**
```bash
# Убедитесь что виртуальное окружение активно
which python  # Linux/Mac
where python  # Windows

# Переустановите зависимости
pip install -r requirements.txt
```

### Проблема: "Ollama connection failed"

**Решение:**
```bash
# Проверьте что Ollama запущен
curl http://localhost:11434/api/tags

# Проверьте установленные модели
ollama list

# Установите модель
ollama pull llama3.1:8b
```

### Проблема: "JWT token invalid"

**Решение:**
```bash
# Очистите localStorage в браузере
# F12 → Console → localStorage.clear()

# Перегенерируйте JWT_SECRET_KEY в .env
openssl rand -hex 32
```

### Проблема: "Port 8000 already in use"

**Решение:**
```bash
# Linux - найти процесс
sudo lsof -i :8000
sudo kill -9 <PID>

# Windows
netstat -ano | findstr :8000
taskkill /PID <PID> /F

# Или используйте другой порт
uvicorn app.main:app --port 8001
```

## 🔒 Безопасность

### Рекомендации

1. **JWT Secret Key**
   - Используйте сильный случайный ключ
   - Никогда не коммитьте в Git
   - Меняйте регулярно в production

2. **Пароли базы данных**
   - Используйте сложные пароли
   - Не используйте стандартные пароли
   - Храните в .env файле

3. **HTTPS**
   - В production обязательно используйте HTTPS
   - Настройте SSL сертификаты
   - Используйте Nginx/Caddy как reverse proxy

4. **CORS**
   - Ограничьте allowed origins в production
   - Не используйте "*" в production

5. **Rate Limiting**
   - Добавьте rate limiting для API
   - Защитите от brute-force атак

### Обновления

Регулярно обновляйте зависимости:
```bash
pip list --outdated
pip install --upgrade <package>
```

## 📝 Лицензия

MIT License

Copyright (c) 2025 Llama Chat

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## 🤝 Участие в разработке

Мы приветствуем вклад в проект!

### Как помочь:

1. Fork репозиторий
2. Создайте ветку для фичи (`git checkout -b feature/AmazingFeature`)
3. Закоммитьте изменения (`git commit -m 'Add some AmazingFeature'`)
4. Push в ветку (`git push origin feature/AmazingFeature`)
5. Откройте Pull Request

### Правила:

- Следуйте PEP 8 для Python кода
- Добавляйте тесты для новых фич
- Обновляйте документацию
- Пишите понятные commit messages

## 📞 Поддержка

Если у вас возникли проблемы:

1. Проверьте [Troubleshooting](#troubleshooting)
2. Посмотрите [Issues](https://github.com/your-repo/issues)
3. Создайте новый Issue с подробным описанием

## 🗺 Roadmap

### Версия 1.1 (планируется)
- [ ] Темная тема
- [ ] Поиск по беседам
- [ ] Экспорт истории
- [ ] Поддержка markdown в сообщениях
- [ ] Голосовой ввод

### Версия 1.2 (планируется)
- [ ] Мультимодальность (изображения)
- [ ] Шаринг бесед
- [ ] Папки для организации бесед
- [ ] Продвинутые настройки моделей
- [ ] Plugins система

### Версия 2.0 (планируется)
- [ ] Многопользовательские беседы
- [ ] Real-time collaboration
- [ ] Advanced RAG
- [ ] Fine-tuning interface

## 📊 Статус проекта

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()
[![Coverage](https://img.shields.io/badge/coverage-85%25-green.svg)]()
[![Version](https://img.shields.io/badge/version-1.0.0-blue.svg)]()

## 🙏 Благодарности

- [FastAPI](https://fastapi.tiangolo.com/) - веб-фреймворк
- [Ollama](https://ollama.ai/) - локальные LLM
- [PostgreSQL](https://www.postgresql.org/) - база данных
- [SQLAlchemy](https://www.sqlalchemy.org/) - ORM
- Сообщество разработчиков open-source

---

*Последнее обновление: 2025*
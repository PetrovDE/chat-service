# LLaMA Service

AI-powered chat service with RAG support.

## 📋 Requirements

- Python 3.10+
- PostgreSQL 14+
- Ollama (for local LLM)

## 🚀 Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Setup environment

Copy `.env.example` to `.env` and configure:

```bash
DATABASE_URL=postgresql+asyncpg://user:password@localhost/llama_db
JWT_SECRET_KEY=your-secret-key-change-this
EMBEDDINGS_BASEURL=http://localhost:11434
```

### 3. Initialize database

```bash
# Create tables
python scripts/init_db.py

# Create admin user
python scripts/create_admin.py
```

### 4. Run server

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## 📚 API Documentation

After starting, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## 🏗️ Project Structure

```
app/
├── api/v1/         # API endpoints
├── core/           # Core configuration
├── crud/           # Database operations
├── db/             # Database models
├── schemas/        # Pydantic schemas
├── services/       # Business logic
└── rag/            # RAG components
```

## 🔑 Default Admin

- Username: `admin`
- Password: `admin123456`

⚠️ **Change password after first login!**


## 📝 License

MIT

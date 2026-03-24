# 20 Alembic Clean-Slate Workflow

Date: 2026-03-24

This project uses a clean-slate migration strategy for test/non-production environments.
Legacy Alembic history is intentionally discarded when architecture-level schema changes happen.

## Source of Truth

Migration chain:
- `alembic.ini` -> script location and logging
- `alembic/env.py` -> URL resolution + `target_metadata`
- `app/db/base.py` -> shared `Base.metadata` (with naming convention)
- `app/db/models/__init__.py` -> imports all current SQLAlchemy models
- `alembic/versions/` -> revision files (current chain starts from `*_initial_clean_schema.py`)

`alembic/env.py` loads metadata from `app.db.base.Base.metadata` and imports `app.db.models` to register all tables.

## Environment Variables

Minimum:
- `DATABASE_URL` (application runtime URL, often `postgresql+asyncpg://...`)
- `ALEMBIC_DATABASE_URL` (synchronous Alembic URL, usually `postgresql://...`)

Alembic uses `ALEMBIC_DATABASE_URL` first.
If only `DATABASE_URL` is provided and starts with `postgresql+asyncpg://`, it is normalized to `postgresql://`.

## Full Clean Reset

1. Reset DB schema (PostgreSQL):

```sql
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
```

Example for this repo Docker setup:

```bash
docker exec llama_chat_postgres psql -U llama_chat_user -d llama_chat_db -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"
```

2. Remove old migration files:

```bash
rm -f alembic/versions/*.py
```

PowerShell equivalent:

```powershell
Remove-Item alembic/versions/*.py
```

3. Generate fresh initial migration:

```bash
alembic revision --autogenerate -m "initial clean schema"
```

4. Inspect generated migration:

```bash
rg -n "create_table|create_index|ForeignKeyConstraint|UniqueConstraint|drop_table|drop_index" alembic/versions/*_initial_clean_schema.py
```

Expectation:
- includes only `create_*` in `upgrade()` for current schema objects;
- does not include legacy tables like `conversation_files`.

5. Apply migration:

```bash
alembic upgrade head
```

6. Verify state:

```bash
alembic current
alembic check
```

Expected:
- `alembic current` shows the generated revision as `(head)`.
- `alembic check` prints `No new upgrade operations detected.`

## Notes

- Do not use data migrations in this clean-slate flow.
- Do not preserve old revision IDs for compatibility in test/non-production environments.
- In production, use a dedicated forward-compatible migration plan instead of schema reset.

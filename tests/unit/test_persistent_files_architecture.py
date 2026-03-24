from __future__ import annotations

import asyncio
import io
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException
from starlette.datastructures import UploadFile

from app.api.v1.endpoints import files as files_endpoint
from app.core.config import settings
from app.crud.file import crud_file as crud_file_repo
from app.rag.retriever_helpers import build_where
from app.services.chat import rag_retrieval_helpers
from app.services.tabular.sql_execution import resolve_tabular_dataset


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _mk_file_obj(*, user_id, path: Path, status: str = "uploaded"):
    now = _utc_now_naive()
    return SimpleNamespace(
        id=uuid4(),
        user_id=user_id,
        original_filename=path.name,
        stored_filename=path.name,
        storage_key=f"raw/{user_id}/{path.name}",
        storage_path=str(path),
        mime_type="text/plain",
        extension=path.suffix.lstrip("."),
        size_bytes=path.stat().st_size if path.exists() else 0,
        checksum=None,
        visibility="private",
        status=status,
        source_kind="upload",
        created_at=now,
        updated_at=now,
        deleted_at=None,
        chunks_count=0,
        custom_metadata={},
    )


def test_upload_persists_raw_file_and_returns_quota(monkeypatch, tmp_path: Path):
    async def scenario():
        monkeypatch.setattr(settings, "RUNTIME_RAW_FILES_DIR", str(tmp_path / "runtime" / "raw"))
        monkeypatch.setattr(files_endpoint.settings, "RUNTIME_RAW_FILES_DIR", str(tmp_path / "runtime" / "raw"))

        user = SimpleNamespace(id=uuid4())
        created_holder = {"file": None}

        async def fake_usage(_db, *, user_id):  # noqa: ARG001
            return int(created_holder["file"].size_bytes if created_holder["file"] else 0)

        async def fake_create_file(_db, **kwargs):
            file_obj = _mk_file_obj(user_id=kwargs["user_id"], path=Path(kwargs["storage_path"]), status="uploaded")
            file_obj.original_filename = kwargs["original_filename"]
            file_obj.stored_filename = kwargs["stored_filename"]
            file_obj.storage_key = kwargs["storage_key"]
            file_obj.storage_path = kwargs["storage_path"]
            file_obj.mime_type = kwargs["mime_type"]
            file_obj.extension = kwargs["extension"]
            file_obj.size_bytes = kwargs["size_bytes"]
            file_obj.checksum = kwargs["checksum"]
            file_obj.source_kind = kwargs["source_kind"]
            file_obj.visibility = kwargs["visibility"]
            file_obj.chunks_count = 0
            created_holder["file"] = file_obj
            return file_obj

        async def fake_add_to_chat(*args, **kwargs):  # noqa: ARG001
            return None

        async def fake_get_active_processing(*args, **kwargs):  # noqa: ARG001
            return None

        async def fake_chat_ids(*args, **kwargs):  # noqa: ARG001
            return {created_holder["file"].id: []}

        async def fake_get_file_or_404(*args, **kwargs):  # noqa: ARG001
            return created_holder["file"]

        async def fake_schedule_process(*args, **kwargs):  # noqa: ARG001
            return uuid4()

        fake_crud = SimpleNamespace(
            get_user_storage_usage_bytes=fake_usage,
            create_file=fake_create_file,
            add_file_to_conversation=fake_add_to_chat,
            get_active_processing=fake_get_active_processing,
        )
        monkeypatch.setattr(files_endpoint, "crud_file", fake_crud)
        monkeypatch.setattr(files_endpoint, "_chat_ids_by_file", fake_chat_ids)
        monkeypatch.setattr(files_endpoint, "_get_user_file_or_404", fake_get_file_or_404)
        monkeypatch.setattr(files_endpoint, "process_file_async", fake_schedule_process)

        upload = UploadFile(filename="report.txt", file=io.BytesIO(b"hello persistent file"))
        result = await files_endpoint.upload_file(
            file=upload,
            source_kind="upload",
            chat_id=None,
            db=SimpleNamespace(),
            current_user=user,
            auto_process=False,
        )

        assert result.file.owner_user_id == user.id
        assert Path(result.file.storage_path).exists()
        assert result.quota.quota_used_bytes >= len(b"hello persistent file")

    asyncio.run(scenario())


def test_upload_enforces_user_quota(monkeypatch, tmp_path: Path):
    async def scenario():
        monkeypatch.setattr(settings, "RUNTIME_RAW_FILES_DIR", str(tmp_path / "runtime" / "raw"))
        monkeypatch.setattr(files_endpoint.settings, "RUNTIME_RAW_FILES_DIR", str(tmp_path / "runtime" / "raw"))
        monkeypatch.setattr(files_endpoint.settings, "USER_FILE_QUOTA_BYTES", 10)

        user = SimpleNamespace(id=uuid4())

        async def fake_usage(_db, *, user_id):  # noqa: ARG001
            return 8

        fake_crud = SimpleNamespace(
            get_user_storage_usage_bytes=fake_usage,
        )
        monkeypatch.setattr(files_endpoint, "crud_file", fake_crud)

        upload = UploadFile(filename="overflow.txt", file=io.BytesIO(b"0123456789"))
        with pytest.raises(HTTPException) as exc:
            await files_endpoint.upload_file(
                file=upload,
                source_kind="upload",
                chat_id=None,
                db=SimpleNamespace(),
                current_user=user,
                auto_process=False,
            )
        assert exc.value.status_code == 413
        assert "quota exceeded" in str(exc.value.detail).lower()

    asyncio.run(scenario())


def test_access_control_blocks_foreign_file(monkeypatch):
    async def scenario():
        async def fake_get_user_file(_db, *, file_id, user_id, include_deleted=False):  # noqa: ARG001
            return None

        monkeypatch.setattr(files_endpoint, "crud_file", SimpleNamespace(get_user_file=fake_get_user_file))
        with pytest.raises(HTTPException) as exc:
            await files_endpoint._get_user_file_or_404(SimpleNamespace(), user_id=uuid4(), file_id=uuid4())
        assert exc.value.status_code == 404

    asyncio.run(scenario())


def test_attach_and_detach_file_for_multiple_chats(monkeypatch):
    async def scenario():
        user = SimpleNamespace(id=uuid4())
        file_id = uuid4()
        chat_a = uuid4()
        chat_b = uuid4()
        attached = []

        async def fake_add(_db, *, file_id, conversation_id, attached_by_user_id):  # noqa: ARG001
            attached.append((file_id, conversation_id, attached_by_user_id))
            return SimpleNamespace(attached_at=_utc_now_naive())

        async def fake_remove(_db, *, file_id, conversation_id):  # noqa: ARG001
            return 1

        async def fake_get_chat(*args, **kwargs):  # noqa: ARG001
            return SimpleNamespace()

        async def fake_get_file(*args, **kwargs):  # noqa: ARG001
            return SimpleNamespace(id=file_id)

        monkeypatch.setattr(files_endpoint, "_get_user_chat_or_404", fake_get_chat)
        monkeypatch.setattr(files_endpoint, "_get_user_file_or_404", fake_get_file)
        monkeypatch.setattr(
            files_endpoint,
            "crud_file",
            SimpleNamespace(add_file_to_conversation=fake_add, remove_file_from_conversation=fake_remove),
        )

        first = await files_endpoint.attach_file_to_chat(
            file_id=file_id,
            request=files_endpoint.FileAttachRequest(chat_id=chat_a),
            db=SimpleNamespace(),
            current_user=user,
        )
        second = await files_endpoint.attach_file_to_chat(
            file_id=file_id,
            request=files_endpoint.FileAttachRequest(chat_id=chat_b),
            db=SimpleNamespace(),
            current_user=user,
        )
        detached = await files_endpoint.detach_file_from_chat(
            file_id=file_id,
            request=files_endpoint.FileDetachRequest(chat_id=chat_a),
            db=SimpleNamespace(),
            current_user=user,
        )

        assert first.status == "attached"
        assert second.status == "attached"
        assert len(attached) == 2
        assert detached.status == "detached"
        assert detached.removed == 1

    asyncio.run(scenario())


def test_attach_rejects_non_attachable_file_status(monkeypatch):
    async def scenario():
        user = SimpleNamespace(id=uuid4())
        file_id = uuid4()

        async def fake_get_chat(*args, **kwargs):  # noqa: ARG001
            return SimpleNamespace()

        async def fake_get_file(*args, **kwargs):  # noqa: ARG001
            return SimpleNamespace(id=file_id, status="failed")

        monkeypatch.setattr(files_endpoint, "_get_user_chat_or_404", fake_get_chat)
        monkeypatch.setattr(files_endpoint, "_get_user_file_or_404", fake_get_file)
        monkeypatch.setattr(
            files_endpoint,
            "crud_file",
            SimpleNamespace(add_file_to_conversation=lambda *args, **kwargs: None),
        )

        with pytest.raises(HTTPException) as exc:
            await files_endpoint.attach_file_to_chat(
                file_id=file_id,
                request=files_endpoint.FileAttachRequest(chat_id=uuid4()),
                db=SimpleNamespace(),
                current_user=user,
            )
        assert exc.value.status_code == 409
        assert "cannot be attached" in str(exc.value.detail).lower()

    asyncio.run(scenario())


def test_reprocess_uses_existing_raw_file(monkeypatch, tmp_path: Path):
    async def scenario():
        user = SimpleNamespace(id=uuid4())
        file_id = uuid4()
        raw_path = tmp_path / "raw.txt"
        raw_path.write_text("raw")
        file_obj = _mk_file_obj(user_id=user.id, path=raw_path, status="ready")
        file_obj.id = file_id

        processing_id = uuid4()

        async def fake_get_file(*args, **kwargs):  # noqa: ARG001
            return file_obj

        async def fake_process(*args, **kwargs):  # noqa: ARG001
            return processing_id

        monkeypatch.setattr(files_endpoint, "_get_user_file_or_404", fake_get_file)
        monkeypatch.setattr(files_endpoint, "process_file_async", fake_process)

        response = await files_endpoint.reprocess_file(
            file_id=file_id,
            request=files_endpoint.FileReprocessRequest(),
            db=SimpleNamespace(),
            current_user=user,
        )
        assert response.status == "scheduled"
        assert response.processing_id == processing_id

    asyncio.run(scenario())


def test_upload_preflight_validation_error_returns_422(monkeypatch, tmp_path: Path):
    async def scenario():
        monkeypatch.setattr(settings, "RUNTIME_RAW_FILES_DIR", str(tmp_path / "runtime" / "raw"))
        monkeypatch.setattr(files_endpoint.settings, "RUNTIME_RAW_FILES_DIR", str(tmp_path / "runtime" / "raw"))

        user = SimpleNamespace(id=uuid4())
        created_holder = {"file": None}

        async def fake_usage(_db, *, user_id):  # noqa: ARG001
            return int(created_holder["file"].size_bytes if created_holder["file"] else 0)

        async def fake_create_file(_db, **kwargs):
            file_obj = _mk_file_obj(user_id=kwargs["user_id"], path=Path(kwargs["storage_path"]), status="uploaded")
            file_obj.original_filename = kwargs["original_filename"]
            file_obj.stored_filename = kwargs["stored_filename"]
            file_obj.storage_key = kwargs["storage_key"]
            file_obj.storage_path = kwargs["storage_path"]
            file_obj.mime_type = kwargs["mime_type"]
            file_obj.extension = kwargs["extension"]
            file_obj.size_bytes = kwargs["size_bytes"]
            file_obj.checksum = kwargs["checksum"]
            file_obj.source_kind = kwargs["source_kind"]
            file_obj.visibility = kwargs["visibility"]
            file_obj.chunks_count = 0
            created_holder["file"] = file_obj
            return file_obj

        async def fake_chat_ids(*args, **kwargs):  # noqa: ARG001
            return {created_holder["file"].id: []}

        async def fake_get_file_or_404(*args, **kwargs):  # noqa: ARG001
            return created_holder["file"]

        async def fake_get_active_processing(*args, **kwargs):  # noqa: ARG001
            return None

        async def fake_process(*args, **kwargs):  # noqa: ARG001
            raise ValueError("Embedding auth failed: provider=aihub model=qwen3-emb")

        monkeypatch.setattr(
            files_endpoint,
            "crud_file",
            SimpleNamespace(
                get_user_storage_usage_bytes=fake_usage,
                create_file=fake_create_file,
                add_file_to_conversation=lambda *args, **kwargs: None,
                get_active_processing=fake_get_active_processing,
            ),
        )
        monkeypatch.setattr(files_endpoint, "_chat_ids_by_file", fake_chat_ids)
        monkeypatch.setattr(files_endpoint, "_get_user_file_or_404", fake_get_file_or_404)
        monkeypatch.setattr(files_endpoint, "process_file_async", fake_process)

        upload = UploadFile(filename="report.txt", file=io.BytesIO(b"hello"))
        with pytest.raises(HTTPException) as exc:
            await files_endpoint.upload_file(
                file=upload,
                source_kind="upload",
                chat_id=None,
                db=SimpleNamespace(),
                current_user=user,
                auto_process=True,
            )
        assert exc.value.status_code == 422
        assert "embedding auth failed" in str(exc.value.detail).lower()

    asyncio.run(scenario())


def test_reprocess_preflight_validation_error_returns_422(monkeypatch, tmp_path: Path):
    async def scenario():
        user = SimpleNamespace(id=uuid4())
        file_id = uuid4()
        raw_path = tmp_path / "raw.txt"
        raw_path.write_text("raw")
        file_obj = _mk_file_obj(user_id=user.id, path=raw_path, status="ready")
        file_obj.id = file_id

        async def fake_get_file(*args, **kwargs):  # noqa: ARG001
            return file_obj

        async def fake_process(*args, **kwargs):  # noqa: ARG001
            raise ValueError("Embedding model unavailable/config error: provider=local model=nomic")

        monkeypatch.setattr(files_endpoint, "_get_user_file_or_404", fake_get_file)
        monkeypatch.setattr(files_endpoint, "process_file_async", fake_process)

        with pytest.raises(HTTPException) as exc:
            await files_endpoint.reprocess_file(
                file_id=file_id,
                request=files_endpoint.FileReprocessRequest(),
                db=SimpleNamespace(),
                current_user=user,
            )
        assert exc.value.status_code == 422
        assert "embedding model unavailable" in str(exc.value.detail).lower()

    asyncio.run(scenario())


def test_update_processing_status_does_not_resurrect_deleted_file():
    async def scenario():
        file_obj = SimpleNamespace(
            id=uuid4(),
            status="deleted",
            deleted_at=_utc_now_naive(),
            updated_at=_utc_now_naive(),
        )
        calls = {"execute": 0, "commit": 0, "refresh": 0}

        class FakeDB:
            async def get(self, model, file_id):  # noqa: ARG002
                return file_obj

            async def execute(self, *args, **kwargs):  # noqa: ARG002
                calls["execute"] += 1
                raise AssertionError("execute must not be called for deleted file status updates")

            async def commit(self):
                calls["commit"] += 1

            async def refresh(self, obj):  # noqa: ARG002
                calls["refresh"] += 1

        result = await crud_file_repo.update_processing_status(
            FakeDB(),
            file_id=file_obj.id,
            status="completed",
            chunks_count=123,
            metadata_patch={"ingestion_progress": {"stage": "completed"}},
        )
        assert result is file_obj
        assert file_obj.status == "deleted"
        assert calls["execute"] == 0
        assert calls["commit"] == 0
        assert calls["refresh"] == 0

    asyncio.run(scenario())


def test_delete_file_lifecycle(monkeypatch, tmp_path: Path):
    async def scenario():
        user = SimpleNamespace(id=uuid4())
        file_id = uuid4()
        raw_path = tmp_path / "to_delete.txt"
        raw_path.write_text("data")
        file_obj = _mk_file_obj(user_id=user.id, path=raw_path, status="ready")
        file_obj.id = file_id

        profiles = [
            SimpleNamespace(id=uuid4(), artifact_metadata={}, is_active=True, status="ready", error_message=None, updated_at=None),
            SimpleNamespace(id=uuid4(), artifact_metadata={}, is_active=False, status="failed", error_message="x", updated_at=None),
        ]
        calls = {"remove_links": 0, "mark_deleting": 0, "mark_deleted": 0, "vector_delete": 0}

        async def fake_list_profiles(_db, *, file_id, user_id):  # noqa: ARG001
            return profiles

        async def fake_mark_deleting(_db, *, file_id):  # noqa: ARG001
            calls["mark_deleting"] += 1
            return file_obj

        async def fake_remove_links(_db, *, file_id):  # noqa: ARG001
            calls["remove_links"] += 1
            return 1

        async def fake_mark_deleted(_db, *, file_id):  # noqa: ARG001
            calls["mark_deleted"] += 1
            return file_obj

        async def fake_usage(_db, *, user_id):  # noqa: ARG001
            return 0

        class FakeVectorStore:
            def delete_by_metadata(self, metadata_filter):
                assert metadata_filter.get("file_id") == str(file_id)
                calls["vector_delete"] += 1
                return 0

        class FakeDB:
            async def commit(self):
                return None

        async def fake_get_file(*args, **kwargs):  # noqa: ARG001
            return file_obj

        monkeypatch.setattr(files_endpoint, "_get_user_file_or_404", fake_get_file)
        monkeypatch.setattr(files_endpoint, "cleanup_tabular_artifacts_for_file", lambda *args, **kwargs: None)
        monkeypatch.setattr(files_endpoint, "VectorStoreManager", FakeVectorStore)
        monkeypatch.setattr(
            files_endpoint,
            "crud_file",
            SimpleNamespace(
                mark_file_deleting=fake_mark_deleting,
                list_processing_profiles=fake_list_profiles,
                remove_file_from_all_conversations=fake_remove_links,
                mark_file_deleted=fake_mark_deleted,
                get_user_storage_usage_bytes=fake_usage,
            ),
        )

        result = await files_endpoint.delete_file(file_id=file_id, db=FakeDB(), current_user=user)
        assert result.status == "deleted"
        assert calls["mark_deleting"] == 1
        assert calls["remove_links"] == 1
        assert calls["mark_deleted"] == 1
        assert calls["vector_delete"] == 1
        assert not raw_path.exists()

    asyncio.run(scenario())


def test_retrieval_where_contract_supports_chat_and_processing():
    where = build_where(
        conversation_id="chat-1",
        user_id="user-1",
        file_ids=["file-1", "file-2"],
        processing_ids=["proc-1", "proc-2"],
        embedding_mode="local",
        embedding_model="nomic-embed-text:latest",
    )
    assert where is not None
    assert where["file_id"]["$in"] == ["file-1", "file-2"]
    assert where["processing_id"]["$in"] == ["proc-1", "proc-2"]
    assert where["embedding_mode"] == "local"


def test_grouped_retrieval_passes_processing_ids():
    async def scenario():
        captured = {}

        class FakeRetriever:
            async def query_rag(self, **kwargs):
                captured["kwargs"] = kwargs
                return {"docs": [], "debug": {}}

        await rag_retrieval_helpers.run_grouped_retrieval(
            rag_retriever_client=FakeRetriever(),
            query="q",
            user_id=uuid4(),
            conversation_id=uuid4(),
            groups={("local", "nomic"): ["f1"]},
            all_file_ids=["f1"],
            processing_ids_by_file={"f1": "p1"},
            top_k=5,
            rag_mode="hybrid",
            embedding_mode="local",
            embedding_model="nomic",
        )
        assert captured["kwargs"]["processing_ids"] == ["p1"]

    asyncio.run(scenario())


def test_grouped_retrieval_strict_contract_no_silent_signature_fallback():
    async def scenario():
        class LegacyRetriever:
            # Intentionally legacy signature without processing_ids/full_file_max_chunks.
            async def query_rag(  # noqa: PLR0913
                self,
                query,  # noqa: ARG002
                top_k=5,  # noqa: ARG002
                conversation_id=None,  # noqa: ARG002
                user_id=None,  # noqa: ARG002
                file_ids=None,  # noqa: ARG002
                embedding_mode="local",  # noqa: ARG002
                embedding_model=None,  # noqa: ARG002
                rag_mode=None,  # noqa: ARG002
                debug_return=False,  # noqa: ARG002
            ):
                return {"docs": [], "debug": {}}

        with pytest.raises(TypeError):
            await rag_retrieval_helpers.run_grouped_retrieval(
                rag_retriever_client=LegacyRetriever(),
                query="q",
                user_id=uuid4(),
                conversation_id=uuid4(),
                groups={("local", "nomic"): ["f1"]},
                all_file_ids=["f1"],
                processing_ids_by_file={"f1": "p1"},
                top_k=5,
                rag_mode="hybrid",
                embedding_mode="local",
                embedding_model="nomic",
            )

    asyncio.run(scenario())


def test_runtime_paths_are_service_relative(monkeypatch):
    monkeypatch.setattr(settings, "RUNTIME_ROOT", "runtime")
    monkeypatch.setattr(settings, "INGESTION_QUEUE_SQLITE_PATH", "runtime/queue/.ingestion_jobs.sqlite3")
    monkeypatch.setattr(settings, "TABULAR_RUNTIME_ROOT", "runtime/tabular_runtime/datasets")
    monkeypatch.setattr(settings, "TABULAR_RUNTIME_CATALOG_PATH", "runtime/tabular_runtime/catalog.duckdb")
    resolved = settings.get_runtime_root()
    queue_path = settings.get_ingestion_queue_path()
    tabular_root = settings.get_tabular_runtime_root()
    tabular_catalog = settings.get_tabular_runtime_catalog_path()
    service_root = settings.get_service_root()
    assert str(resolved).startswith(str(service_root))
    assert str(queue_path).startswith(str(service_root))
    assert str(tabular_root).startswith(str(service_root))
    assert str(tabular_catalog).startswith(str(service_root))


def test_legacy_sqlite_sidecar_metadata_is_not_resolved_for_tabular_runtime(tmp_path: Path):
    legacy_file = SimpleNamespace(
        custom_metadata={
            "tabular_sidecar": {
                "path": str(tmp_path / "legacy.sqlite"),
                "tables": [
                    {"table_name": "sheet_1", "sheet_name": "Sheet1", "row_count": 3, "columns": ["a", "b"]}
                ],
            }
        }
    )
    assert resolve_tabular_dataset(legacy_file) is None


def test_gitignore_covers_runtime_and_keeps_docs_tracked():
    payload = Path(".gitignore").read_text(encoding="utf-8")
    lines = [line.strip() for line in payload.splitlines()]
    assert "/runtime/" in payload
    assert "/docs/" not in lines
    assert "/docs/*" in payload
    assert "!/docs/13_persistent_user_files_architecture.md" in payload

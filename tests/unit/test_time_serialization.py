from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

from app.api.v1.endpoints.files import _to_file_info
from app.utils.time import ensure_utc_datetime, to_utc_iso


def test_ensure_utc_datetime_marks_naive_values_as_utc():
    naive = datetime(2026, 3, 12, 10, 30, 0)
    normalized = ensure_utc_datetime(naive)
    assert normalized is not None
    assert normalized.tzinfo is not None
    assert normalized.utcoffset() == timedelta(0)


def test_to_utc_iso_converts_offset_and_keeps_explicit_timezone():
    dt = datetime(2026, 3, 12, 15, 0, 0, tzinfo=timezone(timedelta(hours=5)))
    iso = to_utc_iso(dt)
    assert iso is not None
    assert iso.endswith("+00:00")
    assert iso.startswith("2026-03-12T10:00:00")


def test_file_info_mapping_serializes_uploaded_at_as_utc_aware():
    file_obj = SimpleNamespace(
        id=uuid4(),
        user_id=uuid4(),
        original_filename="f.txt",
        stored_filename="f",
        storage_key="raw/u/f",
        storage_path="/tmp/f",
        mime_type="text/plain",
        extension="txt",
        size_bytes=123,
        checksum=None,
        visibility="private",
        status="uploaded",
        source_kind="upload",
        chunks_count=0,
        created_at=datetime(2026, 3, 12, 12, 0, 0),
        updated_at=datetime(2026, 3, 12, 12, 0, 0),
        deleted_at=None,
    )
    info = _to_file_info(file_obj, chat_ids=[], active_processing=None)
    assert info.created_at.tzinfo is not None
    assert info.created_at.utcoffset() == timedelta(0)

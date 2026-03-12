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
        filename="f",
        original_filename="f.txt",
        file_type="txt",
        file_size=123,
        is_processed="uploaded",
        chunks_count=0,
        uploaded_at=datetime(2026, 3, 12, 12, 0, 0),
        processed_at=None,
    )
    info = _to_file_info(file_obj, [])
    assert info.uploaded_at.tzinfo is not None
    assert info.uploaded_at.utcoffset() == timedelta(0)

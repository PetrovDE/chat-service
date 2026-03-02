import json
from pathlib import Path


def test_golden_queries_spec_is_valid():
    path = Path("tests/golden/golden_queries.json")
    data = json.loads(path.read_text(encoding="utf-8"))

    assert isinstance(data, list) and len(data) >= 2

    for item in data:
        assert item.get("id")
        assert item.get("query")
        assert item.get("rag_mode") in {"auto", "hybrid", "full_file"}

        signals = item.get("expected_signals")
        assert isinstance(signals, dict)
        assert signals.get("response_not_empty") is True
        assert isinstance(signals.get("debug"), dict)

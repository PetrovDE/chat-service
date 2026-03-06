from app.services.chat.complex_analytics import (
    execute_complex_analytics_path,
    is_complex_analytics_query,
)


def test_public_entrypoints_are_importable():
    assert callable(execute_complex_analytics_path)
    assert callable(is_complex_analytics_query)

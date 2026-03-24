from app.services.chat.language import (
    answer_matches_expected_language,
    ensure_controlled_message_language,
)


def test_short_wrong_language_no_longer_passes_for_russian():
    assert answer_matches_expected_language("ok", "ru") is False


def test_controlled_message_falls_back_to_russian_when_input_is_english():
    text = ensure_controlled_message_language(
        text="Please retry later.",
        preferred_lang="ru",
        fallback_ru="Повторите позже.",
        fallback_en="Please retry later.",
    )
    assert text == "Повторите позже."

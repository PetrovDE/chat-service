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
        fallback_ru="\u041f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u0435 \u043f\u043e\u0437\u0436\u0435.",
        fallback_en="Please retry later.",
    )
    assert text == "\u041f\u043e\u0432\u0442\u043e\u0440\u0438\u0442\u0435 \u043f\u043e\u0437\u0436\u0435."


def test_mixed_russian_answer_with_disallowed_latin_word_is_rejected():
    mixed = "\u041f\u0440\u0438\u0432\u0435\u0442! \u041a\u0430\u043a \u044f \u043c\u043e\u0433\u0443 \u0432\u0430\u043c helfen?"
    assert answer_matches_expected_language(mixed, "ru") is False

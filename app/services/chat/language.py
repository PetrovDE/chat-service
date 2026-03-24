from __future__ import annotations

import re

CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")
LATIN_RE = re.compile(r"[A-Za-z]")


def detect_preferred_response_language(query: str) -> str:
    text = query or ""
    cyr = len(CYRILLIC_RE.findall(text))
    lat = len(LATIN_RE.findall(text))
    if cyr >= 2 and (lat == 0 or cyr * 2 >= lat):
        return "ru"
    if lat >= 2 and (cyr == 0 or lat >= cyr * 2):
        return "en"
    if cyr > 0:
        return "ru"
    if lat > 0:
        return "en"
    return "ru"


def normalize_preferred_response_language(preferred_lang: str) -> str:
    lang = str(preferred_lang or "").strip().lower()
    if lang.startswith("en"):
        return "en"
    return "ru"


def localized_text(*, preferred_lang: str, ru: str, en: str) -> str:
    return ru if normalize_preferred_response_language(preferred_lang) == "ru" else en


def build_language_policy_instruction(preferred_lang: str) -> str:
    if normalize_preferred_response_language(preferred_lang) == "ru":
        return (
            "Language policy:\n"
            "- The user question is in Russian.\n"
            "- Respond strictly in Russian.\n"
            "- Keep factual content unchanged.\n"
        )
    return (
        "Language policy:\n"
        "- The user question is in English.\n"
        "- Respond strictly in English.\n"
        "- Keep factual content unchanged.\n"
    )


def apply_language_policy_to_prompt(*, prompt: str, preferred_lang: str) -> str:
    instruction = build_language_policy_instruction(preferred_lang)
    return f"{instruction}\n{prompt}".strip()


def answer_matches_expected_language(answer: str, preferred_lang: str) -> bool:
    text = answer or ""
    cyr = len(CYRILLIC_RE.findall(text))
    lat = len(LATIN_RE.findall(text))
    total = cyr + lat
    if total == 0:
        return True

    lang = normalize_preferred_response_language(preferred_lang)
    if lang == "ru":
        if cyr == 0 and lat > 0:
            return False
        return cyr >= lat
    if lat == 0 and cyr > 0:
        return False
    return lat >= cyr


def ensure_controlled_message_language(
    *,
    text: str,
    preferred_lang: str,
    fallback_ru: str,
    fallback_en: str,
) -> str:
    candidate = str(text or "").strip()
    if candidate and answer_matches_expected_language(candidate, preferred_lang):
        return candidate
    return localized_text(preferred_lang=preferred_lang, ru=fallback_ru, en=fallback_en)

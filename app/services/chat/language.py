from __future__ import annotations

import re

CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")
LATIN_RE = re.compile(r"[A-Za-z]")


def detect_preferred_response_language(query: str) -> str:
    text = query or ""
    cyr = len(CYRILLIC_RE.findall(text))
    lat = len(LATIN_RE.findall(text))
    if cyr >= 2 and cyr >= lat:
        return "ru"
    if lat >= 2 and lat > cyr:
        return "en"
    return "ru"


def build_language_policy_instruction(preferred_lang: str) -> str:
    if preferred_lang == "ru":
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
    if total < 20:
        return True
    if preferred_lang == "ru":
        return cyr >= lat
    return lat >= cyr

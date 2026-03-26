from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from app.services.chat.tabular_query_parser import parse_tabular_query
from app.services.chat.tabular_temporal_planner import (
    detect_requested_time_grain,
    extract_datetime_source_hint,
    has_temporal_grouping_signal,
)


_FOLLOWUP_PREFIXES = (
    "use ",
    "take ",
    "from ",
    "group by ",
    "yes",
    "\u0434\u0430",
    "\u0430\u0433\u0430",
    "\u0438\u0437 ",
    "\u043f\u043e ",
    "\u043c\u0435\u0441\u044f\u0446",
    "\u043d\u0435\u0434\u0435\u043b",
    "\u0434\u0435\u043d",
    "\u043a\u0432\u0430\u0440\u0442\u0430\u043b",
    "\u0433\u043e\u0434",
    "\u043d\u0430\u0440\u0438\u0441\u0443\u0439",
    "month",
    "week",
    "day",
    "quarter",
    "year",
)

_FOLLOWUP_SUBSTRINGS = (
    "from date",
    "from dates",
    "from the date",
    "date column",
    "datetime column",
    "use created",
    "use date",
    "use month",
    "group by month",
    "group by week",
    "group by day",
    "group by quarter",
    "group by year",
    "\u0438\u0437 \u0434\u0430\u0442",
    "\u0438\u0437 \u0434\u0430\u0442\u044b",
    "\u0432\u0437\u044f\u0442\u044c \u0438\u0437 \u0434\u0430\u0442",
    "\u0432\u044b\u0434\u0435\u043b\u0438\u0432 \u043c\u0435\u0441\u044f\u0446",
    "\u043d\u0443\u0436\u043d\u043e \u0433\u0440\u0430\u0444\u0438\u043a",
)


@dataclass(frozen=True)
class TabularFollowupContext:
    effective_query: str
    followup_context_used: bool
    prior_tabular_intent_reused: bool
    prior_user_query: Optional[str] = None

    def as_debug(self) -> Dict[str, object]:
        return {
            "followup_context_used": bool(self.followup_context_used),
            "prior_tabular_intent_reused": bool(self.prior_tabular_intent_reused),
            "prior_user_query": self.prior_user_query,
        }


def _last_user_message(history: List[Dict[str, str]]) -> Optional[str]:
    for item in reversed(list(history or [])):
        if str(item.get("role") or "").strip().lower() != "user":
            continue
        content = str(item.get("content") or "").strip()
        if content:
            return content
    return None


def _all_user_messages(history: List[Dict[str, str]]) -> List[str]:
    out: List[str] = []
    for item in list(history or []):
        if str(item.get("role") or "").strip().lower() != "user":
            continue
        content = str(item.get("content") or "").strip()
        if content:
            out.append(content)
    return out


def _resolve_anchor_user_query(history: List[Dict[str, str]]) -> tuple[Optional[str], Optional[str]]:
    user_messages = _all_user_messages(history)
    if not user_messages:
        return None, None
    previous_user_query = str(user_messages[-1])
    anchor_user_query = previous_user_query
    if _looks_like_short_followup_refinement(previous_user_query):
        for candidate in reversed(user_messages[:-1]):
            candidate_text = str(candidate or "").strip()
            if not candidate_text:
                continue
            if not _looks_like_tabular_intent(candidate_text):
                continue
            if _looks_like_short_followup_refinement(candidate_text):
                continue
            anchor_user_query = candidate_text
            break
    return previous_user_query, anchor_user_query


def _looks_like_tabular_intent(query: str) -> bool:
    parsed = parse_tabular_query(query)
    if parsed.route != "unknown":
        return True
    if parsed.operation is not None:
        return True
    if bool(parsed.requested_fields):
        return True
    if detect_requested_time_grain(query):
        return True
    if has_temporal_grouping_signal(query):
        return True
    return False


def _looks_like_short_followup_refinement(query: str) -> bool:
    text = str(query or "").strip().lower()
    if not text:
        return False
    if len(text) > 180:
        return False
    token_count = len([token for token in text.replace("\n", " ").split(" ") if token.strip()])
    if token_count > 20:
        return False
    if any(text.startswith(prefix) for prefix in _FOLLOWUP_PREFIXES):
        return True
    if any(token in text for token in _FOLLOWUP_SUBSTRINGS):
        return True
    if extract_datetime_source_hint(text):
        return True
    return False


def apply_tabular_followup_context(
    *,
    query: str,
    conversation_history: Optional[List[Dict[str, str]]],
) -> TabularFollowupContext:
    current_query = str(query or "").strip()
    if not current_query:
        return TabularFollowupContext(
            effective_query=current_query,
            followup_context_used=False,
            prior_tabular_intent_reused=False,
            prior_user_query=None,
        )

    history = list(conversation_history or [])
    previous_user_query, anchor_user_query = _resolve_anchor_user_query(history)
    if not previous_user_query:
        return TabularFollowupContext(
            effective_query=current_query,
            followup_context_used=False,
            prior_tabular_intent_reused=False,
            prior_user_query=None,
        )

    anchor_query = str(anchor_user_query or previous_user_query)
    if not _looks_like_tabular_intent(anchor_query):
        return TabularFollowupContext(
            effective_query=current_query,
            followup_context_used=False,
            prior_tabular_intent_reused=False,
            prior_user_query=previous_user_query,
        )

    if not _looks_like_short_followup_refinement(current_query):
        return TabularFollowupContext(
            effective_query=current_query,
            followup_context_used=False,
            prior_tabular_intent_reused=False,
            prior_user_query=previous_user_query,
        )

    if anchor_query != previous_user_query:
        effective_query = (
            f"{anchor_query}\n"
            f"Follow-up refinement: {previous_user_query}\n"
            f"Follow-up refinement: {current_query}"
        )
    else:
        effective_query = f"{previous_user_query}\nFollow-up refinement: {current_query}"
    return TabularFollowupContext(
        effective_query=effective_query,
        followup_context_used=True,
        prior_tabular_intent_reused=True,
        prior_user_query=anchor_query,
    )

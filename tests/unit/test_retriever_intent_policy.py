from app.rag.retriever_helpers import resolve_intent


def test_resolve_intent_does_not_auto_switch_to_full_file_by_keywords():
    intent = resolve_intent(
        query="please analyze full file and summarize everything",
        query_intent=None,
        rag_mode="auto",
        file_ids=["f1"],
        detect_intent_fn=lambda query: "fact_lookup",  # noqa: ARG005
    )
    assert intent == "fact_lookup"


def test_resolve_intent_full_file_requires_explicit_mode():
    intent = resolve_intent(
        query="analyze full file",
        query_intent=None,
        rag_mode="full_file",
        file_ids=["f1"],
        detect_intent_fn=lambda query: "fact_lookup",  # noqa: ARG005
    )
    assert intent == "analyze_full_file"

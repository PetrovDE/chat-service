from app.services.chat.complex_analytics import planner


def test_planner_intent_detector_flags_complex_query():
    assert planner.is_complex_analytics_query("run python pandas heatmap over comment_text") is True
    assert planner.is_complex_analytics_query("how many rows in table") is False


def test_compute_plan_contract_merges_query_and_plan_flags():
    plan = {
        "analysis_goal": "dependency analysis",
        "required_outputs": ["summary", "dependency chart"],
        "required_contract": {"expects_visualization": False, "expects_dependency": True, "expects_nlp": False},
    }
    contract = planner.compute_plan_contract(plan=plan, query="build heatmap and NLP insights")
    assert contract["expects_visualization"] is True
    assert contract["expects_dependency"] is True
    assert contract["expects_nlp"] is True


def test_extract_json_from_text_supports_fenced_block():
    payload = planner.extract_json_from_text("```json\n{\"analysis_goal\":\"x\"}\n```")
    assert isinstance(payload, dict)
    assert payload["analysis_goal"] == "x"

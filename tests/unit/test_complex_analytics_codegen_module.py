import asyncio

import pandas as pd

from app.services.chat.complex_analytics import codegen


def test_codegen_contract_validation_requires_result_assignment():
    err = codegen.validate_generated_code_contract("x = 1", plan_contract={})
    assert err == "missing_result_assignment"


def test_codegen_contract_validation_respects_visualization_contract():
    code = """
df = datasets["sheet_1"].copy()
result = {"status": "ok", "metrics": {}, "notes": [], "artifacts": []}
"""
    err = codegen.validate_generated_code_contract(
        code,
        plan_contract={"expects_visualization": True},
    )
    assert err == "missing_visualization_contract"


def test_codegen_prompt_includes_contract_and_dataset():
    prompt = codegen.build_codegen_prompt(
        analysis_plan="do analysis",
        primary_table_name="sheet_1",
        dataframe_profile={"rows_total": 10, "columns_total": 2},
        plan_contract={"expects_visualization": True},
    )
    assert "sheet_1" in prompt
    assert "expects_visualization" in prompt


def test_codegen_visualization_patch_adds_save_plot():
    raw = """
df = datasets["sheet_1"].copy()
result = {"status": "ok", "table_name": "sheet_1", "metrics": {}, "notes": [], "artifacts": []}
"""
    patched = codegen._inject_visualization_fallback(raw)
    assert "save_plot(" in patched
    err = codegen.validate_generated_code_contract(
        patched,
        plan_contract={"expects_visualization": True},
    )
    assert err is None


def test_codegen_auto_visual_patch_meta_flag(monkeypatch):
    async def fake_generate_response(**kwargs):  # noqa: ANN003
        policy_class = str(kwargs.get("policy_class") or "")
        if policy_class == "complex_analytics_plan":
            return {
                "response": """
{
  "analysis_goal": "full analysis",
  "required_artifacts": ["plots", "metrics"],
  "required_outputs": ["summary", "metrics", "artifacts"],
  "data_contract": {"required_inputs": ["datasets"], "required_outputs": ["result"]},
  "required_contract": {"expects_visualization": true, "expects_dependency": false, "expects_nlp": false},
  "python_generation_prompt": "Generate robust analysis code for the dataset.",
  "should_generate_code": true
}
""",
                "provider_effective": "ollama",
                "model_route": "ollama",
            }
        return {
            "response": """
df = datasets["sheet_1"].copy()
result = {"status": "ok", "table_name": "sheet_1", "metrics": {}, "notes": [], "artifacts": []}
""",
            "provider_effective": "ollama",
            "model_route": "ollama",
        }

    monkeypatch.setattr(codegen.llm_manager, "generate_response", fake_generate_response)
    monkeypatch.setattr(codegen.settings, "COMPLEX_ANALYTICS_CODEGEN_ENABLED", True)

    generated_code, meta = asyncio.run(
        codegen.generate_complex_analysis_code(
            query="Analyze dataset and build charts",
            primary_table_name="sheet_1",
            primary_frame=pd.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]}),
            model_source="local",
            provider_mode="explicit",
            model_name="llama3.2",
        )
    )

    assert "save_plot(" in generated_code
    assert meta.get("code_source") == "llm"
    assert meta.get("codegen_auto_visual_patch_applied") is True
    assert meta.get("complex_analytics_codegen", {}).get("auto_visual_patch_applied") is True

from app.core.config import settings


def test_default_supported_filetypes_include_spreadsheets():
    assert settings.is_file_supported("report.xlsx")
    assert settings.is_file_supported("report.xls")
    assert settings.is_file_supported("report.csv")
    assert settings.is_file_supported("report.tsv")
